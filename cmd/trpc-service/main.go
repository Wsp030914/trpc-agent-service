package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
)

const (
	envRole              = "TRPC_AGENT_SERVICE_ROLE"
	envPostgresDSN       = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	envRedisURL          = "TRPC_AGENT_SERVICE_REDIS_URL"
	envRedisStream       = "TRPC_AGENT_SERVICE_REDIS_STREAM"
	envRedisGroup        = "TRPC_AGENT_SERVICE_REDIS_GROUP"
	envDispatcherID      = "TRPC_AGENT_SERVICE_DISPATCHER_ID"
	envHealthAddr        = "TRPC_AGENT_SERVICE_HEALTH_ADDR"
	envWorkerID          = "TRPC_AGENT_SERVICE_WORKER_ID"
	envAdminToken        = "TRPC_AGENT_SERVICE_ADMIN_TOKEN"
	envTencentDBGateways = "TRPC_AGENT_SERVICE_TENCENTDB_GATEWAYS"
	envShutdownTimeout   = "TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT"
	envCOSEndpoints      = "TRPC_AGENT_SERVICE_COS_ENDPOINTS"
	envQdrantEndpoints   = "TRPC_AGENT_SERVICE_QDRANT_ENDPOINTS"

	defaultHealthAddr       = ":8080"
	defaultRedisStream      = "trpc-agent-service:dispatch"
	defaultRedisGroup       = "workers"
	healthCheckTimeout      = 2 * time.Second
	dispatchLeaseDuration   = 30 * time.Second
	sessionLeaseDuration    = 30 * time.Second
	defaultShutdownTimeout  = 30 * time.Second
	dataMigrationLease      = 30 * time.Second
	dataMigrationPoll       = time.Second
	artifactCleanupLease    = 30 * time.Second
	artifactCleanupTimeout  = 15 * time.Second
	artifactCleanupRetry    = time.Minute
	knowledgeIndexLease     = 30 * time.Second
	knowledgeIndexRetry     = time.Minute
	knowledgeIndexAttempts  = 5
	knowledgeChunkSize      = 1000
	knowledgeChunkOverlap   = 100
	healthReadHeaderTimeout = 5 * time.Second
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
		log.Print(platformlog.SafeError(err))
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runService(ctx, config); err != nil {
		log.Print(platformlog.SafeError(err))
		os.Exit(1)
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
	telemetry, err := startTelemetry(ctx)
	if err != nil {
		return err
	}
	defer func() { serviceErr = errors.Join(serviceErr, telemetry.close()) }()

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
	defer func() { serviceErr = errors.Join(serviceErr, redisClient.Close()) }()
	stream, err := platformredis.NewStream(redisClient, config.RedisStream, config.RedisGroup, dispatchLeaseDuration)
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
		serviceErr = errors.Join(serviceErr, health.shutdown(shutdownCtx))
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
		runtime, err = newWorkerRuntime(workerRuntimeDependencies{
			store:             store,
			defaultSessionDSN: config.PostgresDSN,
			defaultRedisURL:   config.RedisURL,
			redisClient:       redisClient,
			stream:            stream,
			owner:             config.WorkerID,
			getenv:            os.Getenv,
		})
		if err != nil {
			return err
		}
		defer func() {
			if !errors.Is(serviceErr, errWorkerShutdownTimeout) {
				serviceErr = errors.Join(serviceErr, runtime.close())
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
	return admin.NewHTTPHandler(admin.API{
		Bindings:            store,
		Repository:          store,
		ToolPolicyValidator: noToolsResolver{},
	}, token)
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
	migrationDone := make(chan error, 1)
	go func() {
		migrationDone <- runtime.runDataMigrations(runCtx)
	}()

	select {
	case err := <-relayDone:
		state.ready.Store(false)
		runtime.consumer.StopClaiming()
		cancelRun()
		return err
	case err := <-done:
		state.ready.Store(false)
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		migrationErr, stopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
		return dataMigrationShutdownResult(err, migrationErr, stopped)
	case err := <-migrationDone:
		if err == nil || errors.Is(err, context.Canceled) {
			return nil
		}
		state.ready.Store(false)
		runtime.consumer.StopClaiming()
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
		return workerShutdownResult(err, workerErr, stopped)
	case <-health.done:
		state.ready.Store(false)
		runtime.consumer.StopClaiming()
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
		migrationErr, migrationStopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
		return dataMigrationShutdownResult(workerShutdownResult(health.wait(), workerErr, stopped), migrationErr, migrationStopped)
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
	migrationErr, migrationStopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
	return dataMigrationShutdownResult(workerShutdownResult(healthErr, workerErr, stopped), migrationErr, migrationStopped)
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

func awaitDataMigrationExit(ctx context.Context, done <-chan error) (error, bool) {
	select {
	case err := <-done:
		return err, true
	case <-ctx.Done():
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
		return errors.Join(healthErr, fmt.Errorf("%w: %s", errWorkerShutdownTimeout, platformlog.SafeError(workerErr)))
	}
	return errors.Join(healthErr, workerErr)
}

func dataMigrationShutdownResult(baseErr, migrationErr error, stopped bool) error {
	if !stopped {
		return errors.Join(baseErr, fmt.Errorf("%w: data migration worker: %s", errWorkerShutdownTimeout, platformlog.SafeError(migrationErr)))
	}
	if errors.Is(migrationErr, context.Canceled) {
		return baseErr
	}
	return errors.Join(baseErr, migrationErr)
}
