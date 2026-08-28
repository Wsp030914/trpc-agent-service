package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	platformruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

const (
	envRole            = "TRPC_AGENT_SERVICE_ROLE"
	envPostgresDSN     = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	envRedisURL        = "TRPC_AGENT_SERVICE_REDIS_URL"
	envRedisStream     = "TRPC_AGENT_SERVICE_REDIS_STREAM"
	envRedisGroup      = "TRPC_AGENT_SERVICE_REDIS_GROUP"
	envDispatcherID    = "TRPC_AGENT_SERVICE_DISPATCHER_ID"
	envHealthAddr      = "TRPC_AGENT_SERVICE_HEALTH_ADDR"
	envWorkerID        = "TRPC_AGENT_SERVICE_WORKER_ID"
	envAdminToken      = "TRPC_AGENT_SERVICE_ADMIN_TOKEN"
	envShutdownTimeout = "TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT"

	defaultHealthAddr      = ":8080"
	defaultRedisStream     = "trpc-agent-service:dispatch"
	defaultRedisGroup      = "workers"
	healthCheckTimeout     = 2 * time.Second
	defaultShutdownTimeout = 30 * time.Second
)

var errWorkerShutdownTimeout = errors.New("worker did not stop before shutdown deadline")

type serviceRole string

const (
	roleGateway serviceRole = "gateway"
	roleWorker  serviceRole = "worker"
	roleAll     serviceRole = "all"
)

type serviceConfig struct {
	Role            serviceRole
	PostgresDSN     string
	RedisURL        string
	RedisStream     string
	RedisGroup      string
	DispatcherID    string
	HealthAddr      string
	WorkerID        string
	AdminToken      string
	ShutdownTimeout time.Duration
}

func main() {
	if len(os.Args) > 1 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		fmt.Fprintf(os.Stderr, "usage: %s\n", os.Args[0])
		fmt.Fprintln(os.Stderr, "required: TRPC_AGENT_SERVICE_ROLE, TRPC_AGENT_SERVICE_POSTGRES_DSN, TRPC_AGENT_SERVICE_REDIS_URL")
		fmt.Fprintln(os.Stderr, "worker role also requires TRPC_AGENT_SERVICE_WORKER_ID")
		return
	}

	config, err := configFromEnvironment(os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runService(ctx, config); err != nil {
		log.Fatal(err)
	}
}

func configFromEnvironment(getenv func(string) string) (serviceConfig, error) {
	if getenv == nil {
		return serviceConfig{}, errors.New("environment reader is required")
	}
	config := serviceConfig{
		Role:            serviceRole(getenv(envRole)),
		PostgresDSN:     getenv(envPostgresDSN),
		RedisURL:        getenv(envRedisURL),
		RedisStream:     getenv(envRedisStream),
		RedisGroup:      getenv(envRedisGroup),
		DispatcherID:    getenv(envDispatcherID),
		HealthAddr:      getenv(envHealthAddr),
		WorkerID:        getenv(envWorkerID),
		AdminToken:      getenv(envAdminToken),
		ShutdownTimeout: defaultShutdownTimeout,
	}
	if config.Role != roleGateway && config.Role != roleWorker && config.Role != roleAll {
		return serviceConfig{}, fmt.Errorf("%s must be gateway, worker, or all", envRole)
	}
	if config.PostgresDSN == "" {
		return serviceConfig{}, fmt.Errorf("%s is required", envPostgresDSN)
	}
	if config.RedisURL == "" {
		return serviceConfig{}, fmt.Errorf("%s is required", envRedisURL)
	}
	if config.RedisStream == "" {
		config.RedisStream = defaultRedisStream
	}
	if config.RedisGroup == "" {
		config.RedisGroup = defaultRedisGroup
	}
	if config.HealthAddr == "" {
		config.HealthAddr = defaultHealthAddr
	}
	if config.Role.runsWorker() && config.WorkerID == "" {
		return serviceConfig{}, fmt.Errorf("%s is required for worker role", envWorkerID)
	}
	if config.Role.runsGateway() && config.AdminToken == "" {
		return serviceConfig{}, fmt.Errorf("%s is required for gateway role", envAdminToken)
	}
	if config.Role.runsGateway() && config.DispatcherID == "" {
		return serviceConfig{}, fmt.Errorf("%s is required for gateway role", envDispatcherID)
	}
	if value := getenv(envShutdownTimeout); value != "" {
		duration, err := time.ParseDuration(value)
		if err != nil || duration <= 0 {
			return serviceConfig{}, fmt.Errorf("%s must be a positive duration", envShutdownTimeout)
		}
		config.ShutdownTimeout = duration
	}
	return config, nil
}

func (r serviceRole) runsWorker() bool {
	return r == roleWorker || r == roleAll
}

func (r serviceRole) runsGateway() bool {
	return r == roleGateway || r == roleAll
}

func runService(ctx context.Context, config serviceConfig) (serviceErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	pool, err := pgxpool.New(ctx, config.PostgresDSN)
	if err != nil {
		return fmt.Errorf("connect postgres: %w", err)
	}
	defer func() {
		if !errors.Is(serviceErr, errWorkerShutdownTimeout) {
			pool.Close()
		}
	}()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	store, err := postgres.New(pool)
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	redisClient, err := platformredis.NewClient(ctx, config.RedisURL)
	if err != nil {
		return fmt.Errorf("connect redis: %w", err)
	}
	defer func() { _ = redisClient.Close() }()
	stream, err := platformredis.NewStream(redisClient, config.RedisStream, config.RedisGroup, 30*time.Second)
	if err != nil {
		return err
	}
	if err := stream.Init(ctx); err != nil {
		return err
	}
	var ingressHandler, adminHandler http.Handler
	if config.Role.runsGateway() {
		ingressHandler, err = newGatewayHandler(store)
		if err != nil {
			return err
		}
		adminHandler, err = newAdminHandler(store, config.AdminToken)
		if err != nil {
			return err
		}
	}

	state := &healthState{check: readinessCheck(pool, redisClient)}
	health, err := startHealthServer(config.HealthAddr, state, ingressHandler, adminHandler)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		_ = health.shutdown(shutdownCtx)
	}()

	var runtime *workerRuntime
	var dispatchRelay *relay.Relay
	if config.Role.runsGateway() {
		dispatchRelay, err = relay.New(store, stream, config.DispatcherID)
		if err != nil {
			return err
		}
	}
	if config.Role.runsWorker() {
		runtime, err = newWorkerRuntime(store, config.PostgresDSN, redisClient, stream, config.WorkerID, os.Getenv)
		if err != nil {
			return err
		}
		defer func() {
			if !errors.Is(serviceErr, errWorkerShutdownTimeout) {
				_ = runtime.close()
			}
		}()
	}

	componentCtx, cancelComponents := context.WithCancel(ctx)
	defer cancelComponents()
	var relayDone <-chan error
	if dispatchRelay != nil {
		done := make(chan error, 1)
		go func() { done <- dispatchRelay.Run(componentCtx) }()
		relayDone = done
	}
	state.ready.Store(true)
	log.Printf("trpc-agent-service %s role=%s health=%s", trpcservice.Version, config.Role, config.HealthAddr)
	if runtime == nil {
		return waitForGatewayShutdown(componentCtx, health, state, config.ShutdownTimeout, relayDone)
	}
	return runWorkerUntilShutdown(componentCtx, runtime, health, state, config.ShutdownTimeout, relayDone)
}

func newGatewayHandler(store *postgres.Store) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	events, err := postgres.NewExecutionEventJournal(store)
	if err != nil {
		return nil, err
	}
	queued, err := gateway.NewQueuedRunner(
		gateway.New(gateway.WithAdmitter(store)),
		events,
	)
	if err != nil {
		return nil, err
	}
	return ingress.NewOpenAIHandler(auth.HTTPAPIKeyResolver{
		Credentials: store,
		Directory:   store,
	}, queued)
}

func newAdminHandler(store *postgres.Store, token string) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	return admin.NewHTTPHandler(admin.API{Bindings: store, Repository: store}, token)
}

func waitForGatewayShutdown(
	ctx context.Context,
	health *healthServer,
	state *healthState,
	shutdownTimeout time.Duration,
	relayDone <-chan error,
) error {
	select {
	case err := <-relayDone:
		state.ready.Store(false)
		return err
	case <-ctx.Done():
		state.ready.Store(false)
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return health.shutdown(shutdownCtx)
	case <-health.done:
		return health.wait()
	}
}

func runWorkerUntilShutdown(
	ctx context.Context,
	runtime *workerRuntime,
	health *healthServer,
	state *healthState,
	shutdownTimeout time.Duration,
	relayDone <-chan error,
) error {
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	done := make(chan error, 1)
	go func() {
		done <- runtime.consumer.Run(runCtx)
	}()

	select {
	case err := <-relayDone:
		state.ready.Store(false)
		runtime.consumer.StopClaiming()
		cancelRun()
		return err
	case err := <-done:
		state.ready.Store(false)
		return err
	case <-health.done:
		state.ready.Store(false)
		runtime.consumer.StopClaiming()
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
		return workerShutdownResult(health.wait(), workerErr, stopped)
	case <-ctx.Done():
		state.ready.Store(false)
		runtime.consumer.StopClaiming()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	healthErr := health.shutdown(shutdownCtx)
	if healthErr != nil {
		cancelRun()
	}
	workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
	return workerShutdownResult(healthErr, workerErr, stopped)
}

func awaitWorkerExit(ctx context.Context, done <-chan error, cancel context.CancelFunc) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-ctx.Done():
		cancel()
		select {
		case err := <-done:
			return err, true
		default:
			return ctx.Err(), false
		}
	}
}

func workerShutdownResult(healthErr, workerErr error, stopped bool) error {
	if !stopped {
		return errors.Join(healthErr, fmt.Errorf("%w: %v", errWorkerShutdownTimeout, workerErr))
	}
	return errors.Join(healthErr, workerErr)
}

type workerRuntime struct {
	consumer *worker.Consumer
	runners  *platformruntime.RuntimeRunnerResolver
	sessions *sessionpostgres.SessionResolver
}

func newWorkerRuntime(
	store *postgres.Store,
	defaultSessionDSN string,
	redisClient *platformredis.Client,
	stream *platformredis.Stream,
	owner string,
	getenv func(string) string,
) (*workerRuntime, error) {
	secrets := environmentSecretProvider{getenv: getenv}
	apiKeys, err := secret.NewSecretModelAPIKeyResolver(secrets)
	if err != nil {
		return nil, err
	}
	models, err := platformruntime.NewOpenAIModelResolver(
		apiKeys,
		platformruntime.WithModelEndpointPolicy(defaultEndpointPolicy{}),
	)
	if err != nil {
		return nil, err
	}
	sessions, err := sessionpostgres.NewSessionResolver(sessionDSNResolver{
		defaultDSN: defaultSessionDSN,
		secrets:    secrets,
	})
	if err != nil {
		return nil, err
	}
	runners, err := platformruntime.NewRuntimeRunnerResolver(models, sessions)
	if err != nil {
		_ = sessions.Close()
		return nil, err
	}
	locker, err := platformredis.NewSessionLocker(redisClient, 30*time.Second)
	if err != nil {
		_ = runners.Close()
		_ = sessions.Close()
		return nil, err
	}
	events, err := postgres.NewExecutionEventJournal(store)
	if err != nil {
		_ = runners.Close()
		_ = sessions.Close()
		return nil, err
	}
	audit, err := postgres.NewAuditStore(store)
	if err != nil {
		_ = runners.Close()
		_ = sessions.Close()
		return nil, err
	}
	executor := worker.New(
		store,
		storage.StaticResolver{},
		worker.WithRunner(runners),
		worker.WithSessionLocker(locker),
		worker.WithEventSink(events),
		worker.WithAuditSink(audit),
	)
	consumer, err := worker.NewConsumer(executor, stream, store, owner)
	if err != nil {
		_ = runners.Close()
		_ = sessions.Close()
		return nil, err
	}
	return &workerRuntime{consumer: consumer, runners: runners, sessions: sessions}, nil
}

func (r *workerRuntime) close() error {
	if r == nil {
		return nil
	}
	return errors.Join(r.runners.Close(), r.sessions.Close())
}

type environmentSecretProvider struct {
	getenv func(string) string
}

func (p environmentSecretProvider) ResolveSecret(
	ctx context.Context,
	scope tenant.Scope,
	ref tenant.SecretRef,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := ref.Validate(); err != nil {
		return "", err
	}
	if p.getenv == nil {
		return "", errors.New("environment reader is required")
	}
	value := p.getenv(scopedSecretEnvironmentKey(scope, ref))
	if value == "" {
		return "", errors.New("scoped secret is not configured")
	}
	return value, nil
}

func scopedSecretEnvironmentKey(scope tenant.Scope, ref tenant.SecretRef) string {
	parts := []string{
		"TRPC_AGENT_SERVICE_SECRET",
		hex.EncodeToString([]byte(scope.TenantID)),
		hex.EncodeToString([]byte(scope.AppID)),
		hex.EncodeToString([]byte(ref.Name)),
		hex.EncodeToString([]byte(ref.Version)),
	}
	return strings.Join(parts, "_")
}

type sessionDSNResolver struct {
	defaultDSN string
	secrets    environmentSecretProvider
}

// defaultEndpointPolicy is the production model endpoint policy: it allows
// operator-configured https endpoints and blocks addresses that enable
// server-side request forgery (loopback, link-local, multicast, unspecified).
type defaultEndpointPolicy struct{}

func (defaultEndpointPolicy) ResolveModelBaseURL(
	_ context.Context,
	_ worker.Execution,
	configuredURL string,
) (string, error) {
	parsed, err := url.Parse(configuredURL)
	if err != nil {
		return "", fmt.Errorf("parse model base url: %w", err)
	}
	host := parsed.Hostname()
	if parsed.Scheme != "https" || host == "" {
		return "", errors.New("model base url must use https")
	}
	if ip := net.ParseIP(host); ip != nil {
		if blockedModelEndpointIP(ip) {
			return "", errors.New("model base url resolves to a blocked address")
		}
		return configuredURL, nil
	}
	addresses, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("resolve model base url host: %w", err)
	}
	for _, address := range addresses {
		if blockedModelEndpointIP(address) {
			return "", errors.New("model base url resolves to a blocked address")
		}
	}
	return configuredURL, nil
}

func blockedModelEndpointIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

func (r sessionDSNResolver) ResolveSessionDSN(
	ctx context.Context,
	handle storage.Handle,
) (string, error) {
	if handle.Ref.DSNRef == "" {
		if r.defaultDSN == "" {
			return "", errors.New("session backend dsn_ref is required")
		}
		return r.defaultDSN, nil
	}
	return r.secrets.ResolveSecret(ctx, handle.Scope, tenant.SecretRef{Name: handle.Ref.DSNRef})
}

type healthState struct {
	ready atomic.Bool
	check func(context.Context) error
}

type healthDependency interface {
	Ping(context.Context) error
}

func readinessCheck(dependencies ...healthDependency) func(context.Context) error {
	return func(ctx context.Context) error {
		for _, dependency := range dependencies {
			if dependency == nil {
				return errors.New("health dependency is required")
			}
			if err := dependency.Ping(ctx); err != nil {
				return err
			}
		}
		return nil
	}
}

type healthServer struct {
	server *http.Server
	done   chan struct{}

	mu       sync.Mutex
	serveErr error
}

func startHealthServer(
	address string,
	state *healthState,
	ingressHandler http.Handler,
	adminHandler http.Handler,
) (*healthServer, error) {
	if state == nil {
		return nil, errors.New("health state is required")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("listen health server: %w", err)
	}
	health := &healthServer{
		server: &http.Server{
			Handler:           serviceHandler(state, ingressHandler, adminHandler),
			ReadHeaderTimeout: 5 * time.Second,
		},
		done: make(chan struct{}),
	}
	go func() {
		err := health.server.Serve(listener)
		health.mu.Lock()
		health.serveErr = err
		health.mu.Unlock()
		close(health.done)
	}()
	return health, nil
}

func serviceHandler(state *healthState, ingressHandler, adminHandler http.Handler) http.Handler {
	health := healthHandler(state)
	mux := http.NewServeMux()
	mux.Handle("/livez", health)
	mux.Handle("/readyz", health)
	if ingressHandler != nil {
		mux.Handle("/v1/", ingressHandler)
	}
	if adminHandler != nil {
		mux.Handle("/admin/v1/", adminHandler)
	}
	return mux
}

func (s *healthServer) shutdown(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	if err := s.server.Shutdown(ctx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return s.wait()
}

func (s *healthServer) wait() error {
	if s == nil {
		return nil
	}
	<-s.done
	s.mu.Lock()
	err := s.serveErr
	s.mu.Unlock()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func healthHandler(state *healthState) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/livez", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !state.ready.Load() {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		if state.check != nil {
			checkCtx, cancel := context.WithTimeout(r.Context(), healthCheckTimeout)
			defer cancel()
			if state.check(checkCtx) != nil {
				http.Error(w, "not ready", http.StatusServiceUnavailable)
				return
			}
		}
		w.WriteHeader(http.StatusOK)
	})
	return mux
}
