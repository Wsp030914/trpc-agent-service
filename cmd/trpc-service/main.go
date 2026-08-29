package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/ingress"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	knowledgecos "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/cos"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	memorytencentdb "github.com/liuzengh/trpc-agent-service/trpcservice/memory/tencentdb"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationredispostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/redispostgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/relay"
	platformruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	"github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	sessionredis "github.com/liuzengh/trpc-agent-service/trpcservice/session/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/knowledge/chunking"
	frameworkdocument "trpc.group/trpc-go/trpc-agent-go/knowledge/document"
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

	defaultHealthAddr      = ":8080"
	defaultRedisStream     = "trpc-agent-service:dispatch"
	defaultRedisGroup      = "workers"
	healthCheckTimeout     = 2 * time.Second
	defaultShutdownTimeout = 30 * time.Second
	dataMigrationLease     = 30 * time.Second
	dataMigrationPoll      = time.Second
	artifactCleanupLease   = 30 * time.Second
	artifactCleanupRetry   = time.Minute
	knowledgeIndexLease    = 30 * time.Second
	knowledgeIndexRetry    = time.Minute
	knowledgeIndexAttempts = 5
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
		runtime, err = newWorkerRuntime(store, config.PostgresDSN, config.RedisURL, redisClient, stream, config.WorkerID, os.Getenv)
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
		return errors.Join(healthErr, fmt.Errorf("%w: %v", errWorkerShutdownTimeout, workerErr))
	}
	return errors.Join(healthErr, workerErr)
}

func dataMigrationShutdownResult(baseErr, migrationErr error, stopped bool) error {
	if !stopped {
		return errors.Join(baseErr, fmt.Errorf("%w: data migration worker: %v", errWorkerShutdownTimeout, migrationErr))
	}
	if errors.Is(migrationErr, context.Canceled) {
		return baseErr
	}
	return errors.Join(baseErr, migrationErr)
}

type workerRuntime struct {
	consumer         *worker.Consumer
	runners          *platformruntime.RuntimeRunnerResolver
	sessions         *platformsession.Router
	postgresSessions *sessionpostgres.SessionResolver
	redisSessions    *sessionredis.SessionResolver
	memories         *memorytencentdb.Resolver
	artifacts        *artifactcos.Resolver
	knowledgeSources *knowledgecos.Resolver
	knowledge        *knowledgeqdrant.Resolver
	artifactServices artifactCleanupExecutor
	store            *postgres.Store
	owner            string
}

type artifactCleanupExecutor interface {
	DeleteCleanup(context.Context, worker.Execution, platformartifact.CleanupRecord) error
}

func newWorkerRuntime(
	store *postgres.Store,
	defaultSessionDSN string,
	defaultRedisURL string,
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
	redisSessions, err := sessionredis.NewSessionResolver(sessionURLResolver{
		defaultURL: defaultRedisURL,
		secrets:    secrets,
	})
	if err != nil {
		_ = sessions.Close()
		return nil, err
	}
	sessionRouter, err := platformsession.NewRouter(map[string]platformsession.Resolver{
		"postgres": sessions,
		"redis":    redisSessions,
	})
	if err != nil {
		_ = redisSessions.Close()
		_ = sessions.Close()
		return nil, err
	}
	memories, err := memorytencentdb.NewResolver(
		secrets,
		environmentTencentDBGatewayResolver{getenv: getenv},
	)
	if err != nil {
		_ = sessionRouter.Close()
		return nil, err
	}
	artifacts, err := artifactcos.NewResolver(secrets, environmentCOSEndpointResolver{getenv: getenv})
	if err != nil {
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	artifactServices, err := platformartifact.NewExecutionResolver(artifacts, store)
	if err != nil {
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	knowledgeSources, err := knowledgecos.NewResolver(
		secrets,
		environmentCOSEndpointResolver{getenv: getenv},
	)
	if err != nil {
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	knowledge, err := knowledgeqdrant.NewResolver(
		secrets,
		environmentQdrantEndpointResolver{getenv: getenv},
		store,
		defaultEndpointPolicy{},
	)
	if err != nil {
		_ = knowledgeSources.Close()
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	runners, err := platformruntime.NewRuntimeRunnerResolver(
		models,
		sessionRouter,
		platformruntime.WithSessionIngestorResolver(memories),
		platformruntime.WithArtifactResolver(artifactServices),
		platformruntime.WithKnowledgeResolver(knowledge),
	)
	if err != nil {
		_ = knowledge.Close()
		_ = knowledgeSources.Close()
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	locker, err := platformredis.NewSessionLocker(redisClient, 30*time.Second)
	if err != nil {
		_ = runners.Close()
		_ = knowledge.Close()
		_ = knowledgeSources.Close()
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	events, err := postgres.NewExecutionEventJournal(store)
	if err != nil {
		_ = runners.Close()
		_ = knowledge.Close()
		_ = knowledgeSources.Close()
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	audit, err := postgres.NewAuditStore(store)
	if err != nil {
		_ = runners.Close()
		_ = knowledge.Close()
		_ = knowledgeSources.Close()
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
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
		_ = knowledge.Close()
		_ = knowledgeSources.Close()
		_ = artifacts.Close()
		_ = memories.Close()
		_ = sessionRouter.Close()
		return nil, err
	}
	return &workerRuntime{
		consumer:         consumer,
		runners:          runners,
		sessions:         sessionRouter,
		postgresSessions: sessions,
		redisSessions:    redisSessions,
		memories:         memories,
		artifacts:        artifacts,
		knowledgeSources: knowledgeSources,
		knowledge:        knowledge,
		artifactServices: artifactServices,
		store:            store,
		owner:            owner,
	}, nil
}

func (r *workerRuntime) close() error {
	if r == nil {
		return nil
	}
	return errors.Join(r.runners.Close(), r.sessions.Close(), r.memories.Close(), r.artifacts.Close(), r.knowledgeSources.Close(), r.knowledge.Close())
}

func (r *workerRuntime) runDataMigrations(ctx context.Context) error {
	if r == nil || r.store == nil || r.sessions == nil || r.postgresSessions == nil || r.redisSessions == nil || r.artifactServices == nil || r.knowledgeSources == nil || r.knowledge == nil || r.owner == "" {
		return errors.New("data migration worker runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(dataMigrationPoll)
	defer ticker.Stop()
	for {
		if err := r.runDataMigrationPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("data migration worker pass failed: %v", err)
		}
		if err := r.runArtifactCleanupPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("artifact cleanup worker pass failed: %v", err)
		}
		if err := r.runKnowledgeIndexPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("knowledge index worker pass failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *workerRuntime) runDataMigrationPass(ctx context.Context) error {
	records, err := r.store.ListOwnedDataMigrations(ctx, r.owner)
	if err != nil {
		return fmt.Errorf("list owned data migrations: %w", err)
	}
	for _, record := range records {
		if err := r.runDataMigration(ctx, record); err != nil {
			return err
		}
	}
	record, found, err := r.store.ClaimNextDataMigration(ctx, r.owner, dataMigrationLease)
	if err != nil {
		return fmt.Errorf("claim expired data migration: %w", err)
	}
	if !found {
		return nil
	}
	return r.runDataMigration(ctx, record)
}

func (r *workerRuntime) runDataMigration(ctx context.Context, record migration.Record) error {
	if err := record.Validate(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	leaseDone := make(chan error, 1)
	go r.renewDataMigrationLease(runCtx, cancel, record, leaseDone)

	copier, err := migrationredispostgres.NewCopier(
		runCtx,
		r.store,
		storage.StaticResolver{},
		r.sessions,
		r.postgresSessions,
		r.redisSessions,
		record,
	)
	if err == nil {
		defer copier.Close()
		err = (migration.Executor{
			Catalog:    r.store,
			Repository: r.store,
			Copier:     copier,
		}).Run(runCtx, record)
	}
	cancel()
	leaseErr := <-leaseDone
	if errors.Is(err, migration.ErrLeaseLost) || errors.Is(leaseErr, migration.ErrLeaseLost) {
		return nil
	}
	if leaseErr != nil {
		return fmt.Errorf("renew data migration lease: %w", leaseErr)
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return ctx.Err()
	}
	if errors.Is(err, migration.ErrDrainIncomplete) || err == nil {
		return nil
	}
	if copier == nil {
		return r.failDataMigration(ctx, record, fmt.Errorf("create Redis to PostgreSQL copier: %w", err))
	}
	// Executor persists copy and verification failures as FAILED. Keep this
	// worker available for unrelated Sessions and migrations.
	log.Printf("data migration %s failed: %v", record.ID, err)
	return nil
}

func (r *workerRuntime) renewDataMigrationLease(
	ctx context.Context,
	cancel context.CancelFunc,
	record migration.Record,
	done chan<- error,
) {
	defer close(done)
	ticker := time.NewTicker(dataMigrationLease / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			updated, err := r.store.RenewDataMigration(ctx, record, dataMigrationLease)
			if err != nil {
				if ctx.Err() == nil {
					done <- err
					cancel()
				}
				return
			}
			record = updated
		}
	}
}

func (r *workerRuntime) failDataMigration(ctx context.Context, record migration.Record, cause error) error {
	if err := r.store.UpdateDataMigrationReport(
		context.WithoutCancel(ctx), record, record.Progress, record.Validation, cause.Error(),
	); err != nil {
		if errors.Is(err, migration.ErrLeaseLost) {
			return nil
		}
		return fmt.Errorf("record failed data migration: %w", err)
	}
	if err := r.store.AdvanceDataMigration(context.WithoutCancel(ctx), record, migration.StatusFailed); err != nil {
		if errors.Is(err, migration.ErrLeaseLost) {
			return nil
		}
		return fmt.Errorf("fail data migration: %w", err)
	}
	log.Printf("data migration %s failed: %v", record.ID, cause)
	return nil
}

func (r *workerRuntime) runArtifactCleanupPass(ctx context.Context) error {
	record, found, err := r.store.ClaimNextArtifactCleanup(ctx, r.owner, artifactCleanupLease)
	if err != nil {
		return fmt.Errorf("claim artifact cleanup: %w", err)
	}
	if !found {
		return nil
	}
	config, err := r.store.ResolveAppConfig(ctx, record.TenantID, record.AppID, record.ConfigVersion)
	if err == nil {
		var exec worker.Execution
		exec, err = artifactCleanupExecution(ctx, record, config)
		if err == nil {
			err = r.artifactServices.DeleteCleanup(ctx, exec, record)
		}
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if retryErr := r.store.RetryArtifactCleanup(context.WithoutCancel(ctx), record, artifactCleanupRetry, err); retryErr != nil {
			if errors.Is(retryErr, platformartifact.ErrCleanupLeaseLost) {
				return nil
			}
			return fmt.Errorf("retry artifact cleanup: %w", retryErr)
		}
		return nil
	}
	if err := r.store.CompleteArtifactCleanup(ctx, record); err != nil {
		if errors.Is(err, platformartifact.ErrCleanupLeaseLost) {
			return nil
		}
		return fmt.Errorf("complete artifact cleanup: %w", err)
	}
	return nil
}

func (r *workerRuntime) runKnowledgeIndexPass(ctx context.Context) error {
	job, found, err := r.store.ClaimNextKnowledgeIndex(ctx, r.owner, knowledgeIndexLease)
	if err != nil {
		return fmt.Errorf("claim knowledge index: %w", err)
	}
	if !found {
		return nil
	}
	return r.runKnowledgeIndex(ctx, job)
}

func (r *workerRuntime) runKnowledgeIndex(ctx context.Context, job platformknowledge.IndexJob) error {
	if err := job.Validate(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	leaseDone := make(chan error, 1)
	go r.renewKnowledgeIndexLease(runCtx, cancel, job, leaseDone)

	err := r.indexKnowledgeSource(runCtx, job)
	cancel()
	leaseErr := <-leaseDone
	if errors.Is(err, platformknowledge.ErrIndexLeaseLost) || errors.Is(leaseErr, platformknowledge.ErrIndexLeaseLost) {
		return nil
	}
	if leaseErr != nil {
		return fmt.Errorf("renew knowledge index lease: %w", leaseErr)
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) {
		return ctx.Err()
	}
	if err != nil {
		if job.Attempt >= knowledgeIndexAttempts {
			if failErr := r.store.FailKnowledgeIndex(context.WithoutCancel(ctx), job, err); failErr != nil {
				if errors.Is(failErr, platformknowledge.ErrIndexLeaseLost) {
					return nil
				}
				return fmt.Errorf("fail knowledge index: %w", failErr)
			}
			return nil
		}
		if retryErr := r.store.RetryKnowledgeIndex(context.WithoutCancel(ctx), job, knowledgeIndexRetry, err); retryErr != nil {
			if errors.Is(retryErr, platformknowledge.ErrIndexLeaseLost) {
				return nil
			}
			return fmt.Errorf("retry knowledge index: %w", retryErr)
		}
		return nil
	}
	if err := r.store.CompleteKnowledgeIndex(ctx, job); err != nil {
		if errors.Is(err, platformknowledge.ErrIndexLeaseLost) {
			return nil
		}
		return fmt.Errorf("complete knowledge index: %w", err)
	}
	return nil
}

func (r *workerRuntime) renewKnowledgeIndexLease(
	ctx context.Context,
	cancel context.CancelFunc,
	job platformknowledge.IndexJob,
	done chan<- error,
) {
	defer close(done)
	ticker := time.NewTicker(knowledgeIndexLease / 2)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.store.RenewKnowledgeIndex(ctx, job, knowledgeIndexLease); err != nil {
				if ctx.Err() == nil {
					done <- err
					cancel()
				}
				return
			}
		}
	}
}

func (r *workerRuntime) indexKnowledgeSource(ctx context.Context, job platformknowledge.IndexJob) error {
	config, err := r.store.ResolveAppConfig(ctx, job.Document.Scope.TenantID, job.Document.Scope.AppID, job.ConfigVersion)
	if err != nil {
		return fmt.Errorf("resolve knowledge index config: %w", err)
	}
	exec, err := knowledgeIndexExecution(ctx, job, config)
	if err != nil {
		return err
	}
	content, err := r.knowledgeSources.GetSource(ctx, exec, job.Document)
	if err != nil {
		return err
	}
	vectorDocuments, chunks, err := knowledgeSourceChunks(job, content)
	if err != nil {
		return err
	}
	for _, chunk := range chunks {
		if err := r.store.CreateKnowledgeChunk(ctx, chunk); err != nil {
			return err
		}
	}
	if err := r.knowledge.Index(ctx, exec, vectorDocuments); err != nil {
		return err
	}
	for _, chunk := range chunks {
		if err := r.store.MarkKnowledgeChunkAvailable(ctx, chunk); err != nil {
			return err
		}
	}
	return nil
}

func knowledgeIndexExecution(
	ctx context.Context,
	job platformknowledge.IndexJob,
	config tenant.AppConfig,
) (worker.Execution, error) {
	if err := job.Validate(); err != nil {
		return worker.Execution{}, err
	}
	if config.TenantID != job.Document.Scope.TenantID || config.AppID != job.Document.Scope.AppID || config.Version != job.ConfigVersion {
		return worker.Execution{}, errors.New("knowledge index config does not match job")
	}
	runtime := tenant.RuntimeContext{
		TenantID:           job.Document.Scope.TenantID,
		AppID:              job.Document.Scope.AppID,
		ConfigVersion:      job.ConfigVersion,
		SessionPrincipalID: "knowledge-index",
		SessionID:          job.ID,
		UserID:             "knowledge-index",
		TraceID:            job.ID,
	}
	handles, err := (storage.StaticResolver{}).Resolve(ctx, runtime, config.BackendConfig)
	if err != nil {
		return worker.Execution{}, err
	}
	return worker.Execution{Tenant: runtime, Config: config, Storage: handles}, nil
}

func knowledgeSourceChunks(
	job platformknowledge.IndexJob,
	content []byte,
) ([]*frameworkdocument.Document, []platformknowledge.Chunk, error) {
	if err := job.Validate(); err != nil {
		return nil, nil, err
	}
	if !utf8.Valid(content) {
		return nil, nil, errors.New("knowledge source must be valid utf-8 text")
	}
	chunker := chunking.NewFixedSizeChunking(
		chunking.WithChunkSize(1000),
		chunking.WithOverlap(100),
	)
	parts, err := chunker.Chunk(&frameworkdocument.Document{ID: job.Document.ID, Content: string(content)})
	if err != nil {
		return nil, nil, fmt.Errorf("chunk knowledge source: %w", err)
	}
	vectorDocuments := make([]*frameworkdocument.Document, 0, len(parts))
	chunks := make([]platformknowledge.Chunk, 0, len(parts))
	for index, part := range parts {
		chunkID := strconv.Itoa(index + 1)
		key, err := job.Document.Scope.Key(
			"knowledge-chunk",
			job.Document.KnowledgeBaseID,
			job.Document.ID,
			strconv.Itoa(job.Document.Version),
			job.Document.IndexGeneration,
			chunkID,
		)
		if err != nil {
			return nil, nil, err
		}
		vectorDocuments = append(vectorDocuments, &frameworkdocument.Document{
			ID:      key,
			Content: part.Content,
			Metadata: map[string]any{
				platformknowledge.MetadataTenantID:        job.Document.Scope.TenantID,
				platformknowledge.MetadataAppID:           job.Document.Scope.AppID,
				platformknowledge.MetadataKnowledgeBaseID: job.Document.KnowledgeBaseID,
				platformknowledge.MetadataDocumentID:      job.Document.ID,
				platformknowledge.MetadataDocumentVersion: strconv.Itoa(job.Document.Version),
				platformknowledge.MetadataChunkID:         chunkID,
				platformknowledge.MetadataIndexGeneration: job.Document.IndexGeneration,
			},
		})
		chunks = append(chunks, platformknowledge.Chunk{
			Document: job.Document,
			ChunkID:  chunkID,
			Status:   platformknowledge.ChunkStatusPending,
		})
	}
	if len(vectorDocuments) == 0 {
		return nil, nil, errors.New("knowledge source produced no chunks")
	}
	return vectorDocuments, chunks, nil
}

func artifactCleanupExecution(
	ctx context.Context,
	record platformartifact.CleanupRecord,
	config tenant.AppConfig,
) (worker.Execution, error) {
	if err := record.Validate(); err != nil {
		return worker.Execution{}, err
	}
	if config.TenantID != record.TenantID || config.AppID != record.AppID || config.Version != record.ConfigVersion {
		return worker.Execution{}, errors.New("artifact cleanup config does not match record")
	}
	runtime := tenant.RuntimeContext{
		TenantID:           record.TenantID,
		AppID:              record.AppID,
		ConfigVersion:      record.ConfigVersion,
		SessionPrincipalID: record.SessionPrincipalID,
		SessionID:          record.SessionID,
		UserID:             record.SessionPrincipalID,
		TraceID:            record.ID,
	}
	handles, err := (storage.StaticResolver{}).Resolve(ctx, runtime, config.BackendConfig)
	if err != nil {
		return worker.Execution{}, err
	}
	return worker.Execution{Tenant: runtime, Config: config, Storage: handles}, nil
}

type environmentSecretProvider struct {
	getenv func(string) string
}

type environmentCOSEndpointResolver struct {
	getenv func(string) string
}

type environmentQdrantEndpointResolver struct {
	getenv func(string) string
}

type environmentTencentDBGatewayResolver struct {
	getenv func(string) string
}

func (r environmentTencentDBGatewayResolver) ResolveTencentDBGateway(
	ctx context.Context,
	backendName string,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if backendName == "" {
		return "", errors.New("tencentdb memory backend name is required")
	}
	if r.getenv == nil {
		return "", errors.New("environment reader is required")
	}
	var gateways map[string]string
	if err := json.Unmarshal([]byte(r.getenv(envTencentDBGateways)), &gateways); err != nil {
		return "", fmt.Errorf("decode %s: %w", envTencentDBGateways, err)
	}
	gatewayURL := strings.TrimSpace(gateways[backendName])
	if gatewayURL == "" {
		return "", fmt.Errorf("tencentdb memory gateway is not configured for backend %q", backendName)
	}
	return gatewayURL, nil
}

func (r environmentQdrantEndpointResolver) ResolveQdrantEndpoint(
	ctx context.Context,
	backendName string,
) (knowledgeqdrant.Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return knowledgeqdrant.Endpoint{}, err
	}
	if backendName == "" {
		return knowledgeqdrant.Endpoint{}, errors.New("qdrant backend name is required")
	}
	if r.getenv == nil {
		return knowledgeqdrant.Endpoint{}, errors.New("environment reader is required")
	}
	var endpoints map[string]knowledgeqdrant.Endpoint
	if err := json.Unmarshal([]byte(r.getenv(envQdrantEndpoints)), &endpoints); err != nil {
		return knowledgeqdrant.Endpoint{}, fmt.Errorf("decode %s: %w", envQdrantEndpoints, err)
	}
	endpoint, ok := endpoints[backendName]
	if !ok {
		return knowledgeqdrant.Endpoint{}, fmt.Errorf("qdrant endpoint is not configured for backend %q", backendName)
	}
	return endpoint, nil
}

func (r environmentCOSEndpointResolver) ResolveCOSEndpoint(
	ctx context.Context,
	backendName string,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if backendName == "" {
		return "", errors.New("cos backend name is required")
	}
	if r.getenv == nil {
		return "", errors.New("environment reader is required")
	}
	var endpoints map[string]string
	if err := json.Unmarshal([]byte(r.getenv(envCOSEndpoints)), &endpoints); err != nil {
		return "", fmt.Errorf("decode %s: %w", envCOSEndpoints, err)
	}
	endpoint := endpoints[backendName]
	if endpoint == "" {
		return "", fmt.Errorf("cos endpoint is not configured for backend %q", backendName)
	}
	return endpoint, nil
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

type sessionURLResolver struct {
	defaultURL string
	secrets    environmentSecretProvider
}

func (r sessionURLResolver) ResolveSessionURL(ctx context.Context, handle storage.Handle) (string, error) {
	secretRef := handle.Ref.SecretRef
	if secretRef == (tenant.SecretRef{}) && handle.Ref.DSNRef != "" {
		secretRef = tenant.SecretRef{Name: handle.Ref.DSNRef}
	}
	if secretRef == (tenant.SecretRef{}) {
		if r.defaultURL == "" {
			return "", errors.New("session backend secret_ref is required")
		}
		return r.defaultURL, nil
	}
	return r.secrets.ResolveSecret(ctx, handle.Scope, secretRef)
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
	secretRef := handle.Ref.SecretRef
	if secretRef == (tenant.SecretRef{}) && handle.Ref.DSNRef != "" {
		secretRef = tenant.SecretRef{Name: handle.Ref.DSNRef}
	}
	if secretRef == (tenant.SecretRef{}) {
		if r.defaultDSN == "" {
			return "", errors.New("session backend secret_ref is required")
		}
		return r.defaultDSN, nil
	}
	return r.secrets.ResolveSecret(ctx, handle.Scope, secretRef)
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
