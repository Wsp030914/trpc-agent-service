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
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	channelsattachments "github.com/liuzengh/trpc-agent-service/trpcservice/channels/attachments"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

const (
	envRole                = "TRPC_AGENT_SERVICE_ROLE"
	envPostgresDSN         = "TRPC_AGENT_SERVICE_POSTGRES_DSN"
	envRedisURL            = "TRPC_AGENT_SERVICE_REDIS_URL"
	envRedisStream         = "TRPC_AGENT_SERVICE_REDIS_STREAM"
	envRedisGroup          = "TRPC_AGENT_SERVICE_REDIS_GROUP"
	envDispatcherID        = "TRPC_AGENT_SERVICE_DISPATCHER_ID"
	envHTTPAddr            = "TRPC_AGENT_SERVICE_HTTP_ADDR"
	envWorkerID            = "TRPC_AGENT_SERVICE_WORKER_ID"
	envAdminToken          = "TRPC_AGENT_SERVICE_ADMIN_TOKEN"
	envTencentDBGateways   = "TRPC_AGENT_SERVICE_TENCENTDB_GATEWAYS"
	envShutdownTimeout     = "TRPC_AGENT_SERVICE_SHUTDOWN_TIMEOUT"
	envCOSEndpoints        = "TRPC_AGENT_SERVICE_COS_ENDPOINTS"
	envQdrantEndpoints     = "TRPC_AGENT_SERVICE_QDRANT_ENDPOINTS"
	envOTELProtocol        = "TRPC_AGENT_SERVICE_OTEL_PROTOCOL"
	envOTELTracesEndpoint  = "TRPC_AGENT_SERVICE_OTEL_TRACES_ENDPOINT"
	envOTELMetricsEndpoint = "TRPC_AGENT_SERVICE_OTEL_METRICS_ENDPOINT"
	envModelPricing        = "TRPC_AGENT_SERVICE_MODEL_PRICING"

	defaultHTTPAddr        = ":8080"
	defaultRedisStream     = "trpc-agent-service:dispatch"
	defaultRedisGroup      = "workers"
	dispatchLeaseDuration  = 30 * time.Second
	sessionLeaseDuration   = 30 * time.Second
	defaultShutdownTimeout = 30 * time.Second
	dataMigrationLease     = 30 * time.Second
	dataMigrationPoll      = time.Second
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
	HTTPAddr        string
	WorkerID        string
	AdminToken      string
	ShutdownTimeout time.Duration
	Telemetry       platformtelemetry.Config
	Pricing         platformmetrics.PricingCatalog
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
		HTTPAddr:        getenv(envHTTPAddr),
		WorkerID:        getenv(envWorkerID),
		AdminToken:      getenv(envAdminToken),
		ShutdownTimeout: defaultShutdownTimeout,
		Telemetry: platformtelemetry.Config{
			Protocol:       getenv(envOTELProtocol),
			TraceEndpoint:  getenv(envOTELTracesEndpoint),
			MetricEndpoint: getenv(envOTELMetricsEndpoint),
		},
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
	if config.HTTPAddr == "" {
		config.HTTPAddr = defaultHTTPAddr
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
	pricing, err := platformmetrics.ParsePricingJSON(getenv(envModelPricing))
	if err != nil {
		return serviceConfig{}, err
	}
	config.Pricing = pricing
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
	config.Telemetry.ServiceName = "trpc-agent-service"
	config.Telemetry.ServiceVersion = trpcservice.Version
	telemetryRuntime, telemetryErr := platformtelemetry.Start(ctx, config.Telemetry)
	if telemetryErr != nil {
		log.Printf("telemetry initialization failed: %s", platformlog.SafeError(telemetryErr))
		telemetryRuntime = platformtelemetry.NewNoop(ctx, config.Telemetry.ServiceName)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		serviceErr = errors.Join(serviceErr, telemetryRuntime.Close(shutdownCtx))
	}()
	metricsRecorder, metricsErr := platformmetrics.New(telemetryRuntime.MeterProvider, config.Pricing)
	if metricsErr != nil {
		log.Printf("metrics initialization failed: %s", platformlog.SafeError(metricsErr))
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
	secrets := environmentSecretProvider{getenv: os.Getenv}
	hasher, err := platformsecret.NewExternalIDHasher(secrets, "v1")
	if err != nil {
		return err
	}
	protector, err := platformsecret.NewAEADTargetProtector(secrets, "v1")
	if err != nil {
		return err
	}
	store, err := postgres.New(
		pool,
		postgres.WithChannelIdentityMapping(hasher, protector, []string{"v1"}),
		postgres.WithMetrics(metricsRecorder),
	)
	if err != nil {
		return err
	}
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrate postgres: %w", err)
	}
	cosEndpoints := environmentCOSEndpointResolver{getenv: os.Getenv}
	artifacts, err := artifactcos.NewResolver(secrets, cosEndpoints)
	if err != nil {
		return err
	}
	defer func() { serviceErr = errors.Join(serviceErr, artifacts.Close()) }()
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
		ingressHandler, err = newGatewayHandler(store, artifacts)
		if err != nil {
			return err
		}
		adminHandler, err = newAdminHandler(store, config.AdminToken)
		if err != nil {
			return err
		}
	}

	server, err := startServiceServer(config.HTTPAddr, ingressHandler, adminHandler)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), config.ShutdownTimeout)
		defer cancel()
		serviceErr = errors.Join(serviceErr, server.shutdown(shutdownCtx))
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
			redisClient:       redisClient,
			stream:            stream,
			owner:             config.WorkerID,
			getenv:            os.Getenv,
			artifacts:         artifacts,
			defaultSessionDSN: config.PostgresDSN,
			defaultRedisURL:   config.RedisURL,
			metrics:           metricsRecorder,
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
	log.Printf("trpc-agent-service %s role=%s http=%s", trpcservice.Version, config.Role, config.HTTPAddr)
	if runtime == nil {
		return waitForGatewayShutdown(componentCtx, server, config.ShutdownTimeout, relayDone)
	}
	return runWorkerUntilShutdown(componentCtx, runtime, server, config.ShutdownTimeout, relayDone)
}

func newGatewayHandler(store *postgres.Store, artifacts *artifactcos.Resolver) (http.Handler, error) {
	if store == nil || artifacts == nil {
		return nil, errors.New("gateway dependencies are required")
	}
	events, err := postgres.NewExecutionEventJournal(store)
	if err != nil {
		return nil, err
	}
	admitter := gateway.New(store)
	admitter.Metrics = store.Metrics()
	queued, err := gateway.NewQueuedRunner(
		admitter,
		events,
	)
	if err != nil {
		return nil, err
	}
	openAIHandler, err := ingress.NewOpenAIHandler(auth.HTTPAPIKeyResolver{
		Credentials: store,
		Directory:   store,
	}, queued)
	if err != nil {
		return nil, err
	}
	secrets := environmentSecretProvider{getenv: os.Getenv}
	attachmentIngestor, err := channelsattachments.NewIngestor(
		store,
		secrets,
		artifacts,
	)
	if err != nil {
		return nil, err
	}
	wecomAdapter, err := wecom.NewAdapter(
		store,
		admitter,
		secrets,
		wecom.WithAttachmentIngestor(attachmentIngestor),
		wecom.WithMetrics(store.Metrics()),
	)
	if err != nil {
		return nil, err
	}
	feishuAdapter, err := feishu.NewAdapter(
		store,
		admitter,
		secrets,
		feishu.WithAttachmentIngestor(attachmentIngestor),
		feishu.WithRecallAdmitter(store),
		feishu.WithMetrics(store.Metrics()),
	)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/im/wecom/", wecomAdapter)
	mux.Handle("/im/feishu/", feishuAdapter)
	mux.Handle("/", openAIHandler)
	return mux, nil
}

func newAdminHandler(store *postgres.Store, token string) (http.Handler, error) {
	if store == nil {
		return nil, errors.New("postgres store is required")
	}
	return admin.NewHTTPHandler(admin.API{
		Bindings:   store,
		Repository: store,
	}, token)
}

func waitForGatewayShutdown(
	ctx context.Context,
	server *serviceServer,
	shutdownTimeout time.Duration,
	relayDone <-chan error,
) error {
	select {
	case err := <-relayDone:
		return err
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		return server.shutdown(shutdownCtx)
	case <-server.done:
		return server.wait()
	}
}

func runWorkerUntilShutdown(
	ctx context.Context,
	runtime *workerRuntime,
	server *serviceServer,
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
	var replyDone <-chan error
	if runtime.replySender != nil {
		done := make(chan error, 1)
		go func() { done <- runtime.replySender.Run(runCtx) }()
		replyDone = done
	}

	select {
	case err := <-relayDone:
		runtime.consumer.StopClaiming()
		cancelRun()
		return err
	case err := <-done:
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		migrationErr, stopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
		replyErr, replyStopped := awaitReplyExit(shutdownCtx, replyDone, cancelRun)
		return dataMigrationShutdownResult(errors.Join(err, replyShutdownError(replyErr, replyStopped)), migrationErr, stopped)
	case err := <-replyDone:
		runtime.consumer.StopClaiming()
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
		migrationErr, migrationStopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
		return dataMigrationShutdownResult(
			workerShutdownResult(err, workerErr, stopped),
			migrationErr,
			migrationStopped,
		)
	case err := <-migrationDone:
		runtime.consumer.StopClaiming()
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
		replyErr, replyStopped := awaitReplyExit(shutdownCtx, replyDone, cancelRun)
		return dataMigrationShutdownResult(
			workerShutdownResult(errors.Join(nonCancellationError(err), replyShutdownError(replyErr, replyStopped)), workerErr, stopped),
			err,
			true,
		)
	case <-server.done:
		runtime.consumer.StopClaiming()
		cancelRun()
		shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancelShutdown()
		workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
		migrationErr, migrationStopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
		replyErr, replyStopped := awaitReplyExit(shutdownCtx, replyDone, cancelRun)
		return dataMigrationShutdownResult(workerShutdownResult(errors.Join(server.wait(), replyShutdownError(replyErr, replyStopped)), workerErr, stopped), migrationErr, migrationStopped)
	case <-ctx.Done():
		runtime.consumer.StopClaiming()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancelShutdown()
	serverErr := server.shutdown(shutdownCtx)
	if serverErr != nil {
		cancelRun()
	}
	workerErr, stopped := awaitWorkerExit(shutdownCtx, done, cancelRun)
	migrationErr, migrationStopped := awaitDataMigrationExit(shutdownCtx, migrationDone)
	replyErr, replyStopped := awaitReplyExit(shutdownCtx, replyDone, cancelRun)
	return dataMigrationShutdownResult(workerShutdownResult(errors.Join(serverErr, replyShutdownError(replyErr, replyStopped)), workerErr, stopped), migrationErr, migrationStopped)
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

func awaitReplyExit(ctx context.Context, done <-chan error, cancel context.CancelFunc) (error, bool) {
	if done == nil {
		return nil, true
	}
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

func replyShutdownError(err error, stopped bool) error {
	if !stopped || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func nonCancellationError(err error) error {
	if err == nil || errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func workerShutdownResult(serverErr, workerErr error, stopped bool) error {
	if !stopped {
		return errors.Join(serverErr, fmt.Errorf("%w: %s", errWorkerShutdownTimeout, platformlog.SafeError(workerErr)))
	}
	return errors.Join(serverErr, workerErr)
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
