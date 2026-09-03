package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	channeloutbound "github.com/liuzengh/trpc-agent-service/trpcservice/channels/outbound"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	memorytencentdb "github.com/liuzengh/trpc-agent-service/trpcservice/memory/tencentdb"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationredispostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/redispostgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	platformruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	sessionredis "github.com/liuzengh/trpc-agent-service/trpcservice/session/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type workerRuntime struct {
	consumer         *worker.Consumer
	sessions         *platformsession.Router
	postgresSessions *sessionpostgres.SessionResolver
	memories         *memorytencentdb.Resolver
	knowledge        *knowledgeqdrant.Resolver
	replySender      *worker.ReplySender
	store            *postgres.Store
	owner            string
}

type workerRuntimeDependencies struct {
	store             *postgres.Store
	redisClient       *platformredis.Client
	stream            *platformredis.Stream
	owner             string
	getenv            func(string) string
	artifacts         *artifactcos.Resolver
	defaultSessionDSN string
	defaultRedisURL   string
	metrics           *platformmetrics.Recorder
}

const (
	defaultReplyRateLimit       = 5
	defaultReplyRateLimitWindow = time.Second
)

func newWorkerRuntime(deps workerRuntimeDependencies) (*workerRuntime, error) {
	if deps.store == nil || deps.redisClient == nil || deps.stream == nil || deps.owner == "" || deps.artifacts == nil {
		return nil, errors.New("worker runtime dependencies are required")
	}
	secrets := environmentSecretProvider{getenv: deps.getenv}
	models, err := platformruntime.NewOpenAIModelResolver(
		secrets,
		platformruntime.DefaultEndpointPolicy{},
	)
	if err != nil {
		return nil, err
	}
	sessions, err := sessionpostgres.NewSessionResolver(secrets, deps.defaultSessionDSN)
	if err != nil {
		return nil, err
	}
	redisSessions, err := sessionredis.NewSessionResolver(secrets, deps.defaultRedisURL)
	if err != nil {
		return nil, joinCloseError(err, sessions.Close)
	}
	sessionRouter, err := platformsession.NewRouter(sessions, redisSessions)
	if err != nil {
		return nil, joinCloseError(err, redisSessions.Close, sessions.Close)
	}
	memories, err := memorytencentdb.NewResolver(
		secrets,
		environmentTencentDBGatewayResolver{getenv: deps.getenv},
	)
	if err != nil {
		return nil, joinCloseError(err, sessionRouter.Close)
	}
	artifactServices, err := platformartifact.NewExecutionResolver(deps.artifacts.ResolveArtifact, deps.store)
	if err != nil {
		return nil, joinCloseError(err, memories.Close, sessionRouter.Close)
	}
	knowledge, err := knowledgeqdrant.NewResolver(
		secrets,
		environmentQdrantEndpointResolver{getenv: deps.getenv},
		deps.store,
		platformruntime.DefaultEndpointPolicy{},
	)
	if err != nil {
		return nil, joinCloseError(err, memories.Close, sessionRouter.Close)
	}
	runtimeBuilder, err := platformruntime.NewRuntime(
		models,
		sessionRouter,
		memories,
		artifactServices,
		knowledge,
		platformruntime.NewToolCatalog(),
	)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	metricsRecorder := deps.metrics
	if metricsRecorder == nil {
		metricsRecorder = deps.store.Metrics()
	}
	runtimeBuilder.SetObservability(deps.store, metricsRecorder)
	locker, err := platformredis.NewSessionLocker(deps.redisClient, sessionLeaseDuration)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	replyLimiter, err := platformredis.NewReplyRateLimiter(
		deps.redisClient,
		defaultReplyRateLimit,
		defaultReplyRateLimitWindow,
	)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	replyProviders, err := channeloutbound.NewResolver(deps.store, secrets, replyLimiter)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	events, err := postgres.NewExecutionEventJournal(deps.store, postgres.WithReplyEventBuilder(worker.BuildReplyEvent))
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	replySender, err := worker.NewReplySender(
		deps.store,
		deps.store.ResolveReplyTarget,
		replyProviders.ResolveReplyProvider,
		worker.ReplySenderOptions{Owner: deps.owner, Metrics: metricsRecorder},
	)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	executor := worker.New(
		deps.store,
		runtimeBuilder.BuildRunner,
		locker,
		events,
		deps.store.IsExecutionCanceled,
	)
	executor.Audit = deps.store
	executor.Metrics = metricsRecorder
	consumer, err := worker.NewConsumer(executor, deps.stream, deps.store, deps.owner)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	return &workerRuntime{
		consumer:         consumer,
		sessions:         sessionRouter,
		postgresSessions: sessions,
		memories:         memories,
		knowledge:        knowledge,
		replySender:      replySender,
		store:            deps.store,
		owner:            deps.owner,
	}, nil
}

func joinCloseError(base error, closers ...func() error) error {
	result := base
	for _, close := range closers {
		if close != nil {
			result = errors.Join(result, close())
		}
	}
	return result
}

func (r *workerRuntime) close() error {
	if r == nil {
		return nil
	}
	return errors.Join(r.sessions.Close(), r.memories.Close(), r.knowledge.Close())
}

func (r *workerRuntime) runDataMigrations(ctx context.Context) error {
	if r == nil || r.store == nil || r.sessions == nil || r.postgresSessions == nil || r.owner == "" {
		return errors.New("data migration worker runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ticker := time.NewTicker(dataMigrationPoll)
	defer ticker.Stop()
	for {
		if err := r.runDataMigrationPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("data migration worker pass failed: %s", platformlog.SafeError(err))
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
		r.sessions,
		r.postgresSessions,
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
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if errors.Is(err, migration.ErrDrainIncomplete) || err == nil {
		return nil
	}
	if copier == nil {
		return r.failDataMigration(ctx, record, fmt.Errorf("create Redis to PostgreSQL copier: %w", err))
	}
	log.Printf("data migration %s failed: %s", record.ID, platformlog.SafeError(err))
	return nil
}

func (r *workerRuntime) renewDataMigrationLease(
	ctx context.Context,
	cancel context.CancelFunc,
	record migration.Record,
	done chan<- error,
) {
	renewLease(ctx, cancel, dataMigrationLease, done, func(renewCtx context.Context) error {
		updated, err := r.store.RenewDataMigration(renewCtx, record, dataMigrationLease)
		if err == nil {
			record = updated
		}
		return err
	})
}

func (r *workerRuntime) failDataMigration(ctx context.Context, record migration.Record, cause error) error {
	record.FailureReason = platformlog.SafeError(cause)
	if err := r.store.AdvanceDataMigration(context.WithoutCancel(ctx), record, migration.StatusFailed); err != nil {
		if errors.Is(err, migration.ErrLeaseLost) {
			return nil
		}
		return fmt.Errorf("fail data migration: %w", err)
	}
	log.Printf("data migration %s failed: %s", record.ID, platformlog.SafeError(cause))
	return nil
}

func renewLease(
	ctx context.Context,
	cancel context.CancelFunc,
	leaseDuration time.Duration,
	done chan<- error,
	renew func(context.Context) error,
) {
	defer close(done)
	if ctx == nil {
		ctx = context.Background()
	}
	if cancel == nil {
		cancel = func() {}
	}
	if renew == nil || leaseDuration <= 0 {
		err := errors.New("lease renewal is not configured")
		done <- err
		cancel()
		return
	}
	interval := leaseDuration / 2
	if interval <= 0 {
		interval = time.Nanosecond
	}
	renewTimeout := leaseDuration / 3
	if renewTimeout <= 0 {
		renewTimeout = time.Nanosecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancelRenew := context.WithTimeout(ctx, renewTimeout)
			err := renew(renewCtx)
			cancelRenew()
			if err != nil {
				if ctx.Err() == nil {
					done <- err
					cancel()
				}
				return
			}
		}
	}
}
