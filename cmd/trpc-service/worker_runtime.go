package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
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
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
	platformruntime "github.com/liuzengh/trpc-agent-service/trpcservice/runtime"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	sessionredis "github.com/liuzengh/trpc-agent-service/trpcservice/session/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type workerRuntime struct {
	consumer                    *worker.Consumer
	sessions                    *platformsession.Router
	postgresSessions            *sessionpostgres.SessionResolver
	memories                    *memorytencentdb.Resolver
	knowledge                   *knowledgeqdrant.Resolver
	replyProviders              *channeloutbound.Resolver
	replySender                 *worker.ReplySender
	artifactCleanup             *platformartifact.CleanupWorker
	store                       *postgres.Store
	secrets                     platformsecret.SecretProvider
	qdrantEndpoints             knowledgeqdrant.EndpointResolver
	owner                       string
	concurrency                 int
	pauseAfterMigrationCopy     time.Duration
	pauseAfterMigrationCopyItem time.Duration
}

type workerRuntimeDependencies struct {
	store                       *postgres.Store
	redisClient                 *platformredis.Client
	stream                      *platformredis.Stream
	owner                       string
	getenv                      func(string) string
	artifacts                   *artifactcos.Resolver
	defaultSessionDSN           string
	defaultRedisURL             string
	inMemorySessions            platformsession.Resolver
	metrics                     *platformmetrics.Recorder
	modelTimeout                time.Duration
	artifactRetention           time.Duration
	concurrency                 int
	pauseAfterClaim             time.Duration
	pauseAfterMigrationCopy     time.Duration
	pauseAfterMigrationCopyItem time.Duration
}

const (
	defaultReplyRateLimit       = 5
	defaultReplyRateLimitWindow = time.Second
	dataMigrationRetryInitial   = 250 * time.Millisecond
	dataMigrationRetryMax       = 30 * time.Second
	artifactCleanupPoll         = 30 * time.Second
	metricsRefreshPoll          = 15 * time.Second
)

func newWorkerRuntime(deps workerRuntimeDependencies) (*workerRuntime, error) {
	if deps.store == nil || deps.redisClient == nil || deps.stream == nil || deps.owner == "" || deps.artifacts == nil {
		return nil, errors.New("worker runtime dependencies are required")
	}
	secrets := environmentSecretProvider{getenv: deps.getenv}
	qdrantEndpoints := environmentQdrantEndpointResolver{getenv: deps.getenv}
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
	var sessionRouter *platformsession.Router
	if deps.inMemorySessions != nil {
		sessionRouter, err = platformsession.NewRouter(sessions, redisSessions, deps.inMemorySessions)
	} else {
		sessionRouter, err = platformsession.NewRouter(sessions, redisSessions)
	}
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
	artifactCleanup, err := platformartifact.NewCleanupWorker(
		deps.store,
		func(ctx context.Context, candidate platformartifact.CleanupCandidate) error {
			config, err := deps.store.ResolveAppConfig(
				ctx,
				candidate.Record.TenantID,
				candidate.Record.AppID,
				candidate.Record.ConfigVersion,
			)
			if err != nil {
				return fmt.Errorf("resolve artifact cleanup config: %w", err)
			}
			ref := config.BackendConfig.Artifact
			if ref.IsZero() {
				return errors.New("artifact cleanup config has no artifact backend")
			}
			return deps.artifacts.DeleteExactObject(
				ctx,
				tenant.Scope{TenantID: candidate.Record.TenantID, AppID: candidate.Record.AppID},
				candidate.Record.ConfigVersion,
				ref,
				candidate.Record.ObjectKey,
			)
		},
		platformartifact.CleanupOptions{
			Owner:        deps.owner,
			RetentionAge: deps.artifactRetention,
		},
	)
	if err != nil {
		return nil, joinCloseError(err, memories.Close, sessionRouter.Close)
	}
	knowledge, err := knowledgeqdrant.NewResolver(
		secrets,
		qdrantEndpoints,
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
		return nil, joinCloseError(err, replyProviders.Close, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	replySender, err := worker.NewReplySender(
		deps.store,
		deps.store.ResolveReplyTarget,
		replyProviders.ResolveReplyProvider,
		worker.ReplySenderOptions{Owner: deps.owner, Metrics: metricsRecorder},
	)
	if err != nil {
		return nil, joinCloseError(err, replyProviders.Close, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	executor := worker.New(
		deps.store,
		runtimeBuilder.BuildRunner,
		locker,
		events,
		deps.store.IsExecutionCanceled,
	)
	executor.ModelTimeout = deps.modelTimeout
	executor.Audit = deps.store
	executor.Metrics = metricsRecorder
	executor.Approvals = deps.store
	consumerOptions := worker.ConsumerOptions{Concurrency: deps.concurrency}
	if deps.pauseAfterClaim > 0 {
		consumerOptions.AfterClaim = func(ctx context.Context, _ queue.Claim) {
			timer := time.NewTimer(deps.pauseAfterClaim)
			defer timer.Stop()
			select {
			case <-ctx.Done():
			case <-timer.C:
			}
		}
	}
	consumer, err := worker.NewConsumerWithOptions(executor, deps.stream, deps.store, deps.owner, consumerOptions)
	if err != nil {
		return nil, joinCloseError(err, replyProviders.Close, knowledge.Close, memories.Close, sessionRouter.Close)
	}
	return &workerRuntime{
		consumer:                    consumer,
		sessions:                    sessionRouter,
		postgresSessions:            sessions,
		memories:                    memories,
		knowledge:                   knowledge,
		replyProviders:              replyProviders,
		replySender:                 replySender,
		artifactCleanup:             artifactCleanup,
		store:                       deps.store,
		secrets:                     secrets,
		qdrantEndpoints:             qdrantEndpoints,
		owner:                       deps.owner,
		concurrency:                 deps.concurrency,
		pauseAfterMigrationCopy:     deps.pauseAfterMigrationCopy,
		pauseAfterMigrationCopyItem: deps.pauseAfterMigrationCopyItem,
	}, nil
}

// runHeartbeat exposes worker liveness to the Admin control plane. It carries
// only process metadata; execution payloads and lease fencing tokens stay in
// their owning paths.
func (r *workerRuntime) runHeartbeat(ctx context.Context) {
	if r == nil || r.store == nil || r.owner == "" || r.concurrency <= 0 {
		return
	}
	startedAt := time.Now().UTC()
	write := func(writeCtx context.Context, status, lastError string) {
		if err := r.store.UpsertWorkerHeartbeat(writeCtx, r.owner, status, r.concurrency, startedAt, lastError); err != nil {
			log.Printf("worker heartbeat failed: %s", platformlog.SafeError(err))
		}
	}
	write(ctx, "READY", "")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	defer write(context.WithoutCancel(ctx), "NOT_READY", "")
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			write(ctx, "READY", "")
		}
	}
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
	return errors.Join(r.replyProviders.Close(), r.sessions.Close(), r.memories.Close(), r.knowledge.Close())
}

func (r *workerRuntime) runDataMigrations(ctx context.Context) error {
	if r == nil || r.store == nil || r.sessions == nil || r.postgresSessions == nil || r.owner == "" {
		return errors.New("data migration worker runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lastArtifactCleanup := time.Time{}
	lastMetricsRefresh := time.Time{}
	for attempt := 0; ; {
		if r.store.Metrics() != nil && (lastMetricsRefresh.IsZero() || time.Since(lastMetricsRefresh) >= metricsRefreshPoll) {
			lastMetricsRefresh = time.Now()
			if _, err := r.store.OperationsSummary(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("operational metrics refresh failed: %s", platformlog.SafeError(err))
			}
		}
		if r.artifactCleanup != nil && (lastArtifactCleanup.IsZero() || time.Since(lastArtifactCleanup) >= artifactCleanupPoll) {
			lastArtifactCleanup = time.Now()
			if _, err := r.artifactCleanup.RunPass(ctx); err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				log.Printf("artifact cleanup worker pass failed: %s", platformlog.SafeError(err))
			}
		}
		delay := dataMigrationPoll
		if err := r.runDataMigrationPass(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("data migration worker pass failed: %s", platformlog.SafeError(err))
			delay = dataMigrationRetryDelay(attempt)
			attempt++
		} else {
			attempt = 0
		}
		if err := waitForDataMigration(ctx, delay); err != nil {
			return err
		}
	}
}

func dataMigrationRetryDelay(attempt int) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	delay := dataMigrationRetryInitial
	for attempt > 0 && delay < dataMigrationRetryMax {
		delay *= 2
		attempt--
	}
	if delay > dataMigrationRetryMax {
		delay = dataMigrationRetryMax
	}
	half := delay / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}

func waitForDataMigration(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		delay = dataMigrationPoll
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
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

	var err error
	var run func(context.Context) error
	var closeCopier func()
	var copierCreated bool
	switch record.EffectiveDomain() {
	case migration.DomainSession:
		copier, copierErr := migrationredispostgres.NewCopier(
			runCtx,
			r.store,
			r.sessions,
			r.postgresSessions,
			record,
		)
		err = copierErr
		if copierErr == nil {
			copierCreated = true
			closeCopier = copier.Close
			run = func(executeCtx context.Context) error {
				return (migration.Executor{
					Catalog:       r.store,
					Repository:    r.store,
					Copier:        copier,
					AfterCopy:     pauseAfterMigrationCopy(r.pauseAfterMigrationCopy),
					AfterCopyItem: pauseAfterMigrationCopyItem(r.pauseAfterMigrationCopyItem),
				}).Run(executeCtx, record)
			}
		}
	case migration.DomainKnowledge:
		copier, copierErr := knowledgeqdrant.NewMigrationCopier(
			runCtx,
			r.store,
			r.secrets,
			r.qdrantEndpoints,
			record,
		)
		err = copierErr
		if copierErr == nil {
			copierCreated = true
			closeCopier = func() {
				if closeErr := copier.Close(); closeErr != nil {
					log.Printf("knowledge migration %s close failed: %s", record.ID, platformlog.SafeError(closeErr))
				}
			}
			run = func(executeCtx context.Context) error {
				return (migration.Executor{
					KnowledgeCatalog: r.store,
					Repository:       r.store,
					KnowledgeCopier:  copier,
					AfterCopy:        pauseAfterMigrationCopy(r.pauseAfterMigrationCopy),
					AfterCopyItem:    pauseAfterMigrationCopyItem(r.pauseAfterMigrationCopyItem),
				}).Run(executeCtx, record)
			}
		}
	default:
		err = errors.New("unsupported data migration domain")
	}
	if closeCopier != nil {
		defer closeCopier()
	}
	if run != nil {
		err = run(runCtx)
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
	if !copierCreated {
		return r.failDataMigration(ctx, record, fmt.Errorf("create %s migration copier: %w", record.EffectiveDomain(), err))
	}
	log.Printf("data migration %s failed: %s", record.ID, platformlog.SafeError(err))
	return nil
}

func pauseAfterMigrationCopy(duration time.Duration) func(context.Context, migration.Record) error {
	if duration <= 0 {
		return nil
	}
	return func(ctx context.Context, _ migration.Record) error {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
}

func pauseAfterMigrationCopyItem(duration time.Duration) func(context.Context, migration.Record) error {
	if duration <= 0 {
		return nil
	}
	return func(ctx context.Context, _ migration.Record) error {
		timer := time.NewTimer(duration)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return nil
		}
	}
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
					cancel()
					done <- err
				}
				return
			}
		}
	}
}
