package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"
	"unicode/utf8"

	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/wecom"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	knowledgecos "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/cos"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	memorytencentdb "github.com/liuzengh/trpc-agent-service/trpcservice/memory/tencentdb"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	migrationredispostgres "github.com/liuzengh/trpc-agent-service/trpcservice/migration/redispostgres"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformredis "github.com/liuzengh/trpc-agent-service/trpcservice/redis"
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
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

type workerRuntime struct {
	consumer         *worker.Consumer
	runners          *platformruntime.RuntimeRunnerResolver
	sessions         *platformsession.Router
	postgresSessions *sessionpostgres.SessionResolver
	memories         *memorytencentdb.Resolver
	artifacts        *artifactcos.Resolver
	knowledgeSources *knowledgecos.Resolver
	knowledge        *knowledgeqdrant.Resolver
	artifactServices artifactCleanupExecutor
	replySender      *worker.ReplySender
	store            *postgres.Store
	owner            string
}

type artifactCleanupExecutor interface {
	DeleteCleanup(context.Context, worker.Execution, platformartifact.CleanupRecord) error
}

type workerRuntimeDependencies struct {
	store             *postgres.Store
	defaultSessionDSN string
	defaultRedisURL   string
	redisClient       *platformredis.Client
	stream            *platformredis.Stream
	owner             string
	getenv            func(string) string
}

func newWorkerRuntime(deps workerRuntimeDependencies) (*workerRuntime, error) {
	if deps.store == nil || deps.redisClient == nil || deps.stream == nil || deps.owner == "" {
		return nil, errors.New("worker runtime dependencies are required")
	}
	secrets := environmentSecretProvider{getenv: deps.getenv}
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
		defaultDSN: deps.defaultSessionDSN,
		secrets:    secrets,
	})
	if err != nil {
		return nil, err
	}
	redisSessions, err := sessionredis.NewSessionResolver(sessionURLResolver{
		defaultURL: deps.defaultRedisURL,
		secrets:    secrets,
	})
	if err != nil {
		return nil, joinCloseError(err, sessions.Close)
	}
	sessionRouter, err := platformsession.NewRouter(map[string]platformsession.Resolver{
		"postgres": sessions,
		"redis":    redisSessions,
	})
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
	artifacts, err := artifactcos.NewResolver(secrets, environmentCOSEndpointResolver{getenv: deps.getenv})
	if err != nil {
		return nil, joinCloseError(err, memories.Close, sessionRouter.Close)
	}
	artifactServices, err := platformartifact.NewExecutionResolver(artifacts, deps.store)
	if err != nil {
		return nil, joinCloseError(err, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	knowledgeSources, err := knowledgecos.NewResolver(
		secrets,
		environmentCOSEndpointResolver{getenv: deps.getenv},
	)
	if err != nil {
		return nil, joinCloseError(err, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	knowledge, err := knowledgeqdrant.NewResolver(
		secrets,
		environmentQdrantEndpointResolver{getenv: deps.getenv},
		deps.store,
		defaultEndpointPolicy{},
	)
	if err != nil {
		return nil, joinCloseError(err, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	runners, err := platformruntime.NewRuntimeRunnerResolver(
		models,
		sessionRouter,
		platformruntime.WithSessionIngestorResolver(memories),
		platformruntime.WithArtifactResolver(artifactServices),
		platformruntime.WithKnowledgeResolver(knowledge),
		platformruntime.WithRuntimeToolResolver(noToolsResolver{}),
	)
	if err != nil {
		return nil, joinCloseError(err, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	locker, err := platformredis.NewSessionLocker(deps.redisClient, sessionLeaseDuration)
	if err != nil {
		return nil, joinCloseError(err, runners.Close, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	replyBuilder, err := worker.NewReplyEventBuilder(runtimeReplyCapabilityResolver{})
	if err != nil {
		return nil, joinCloseError(err, runners.Close, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	events, err := postgres.NewExecutionEventJournal(deps.store, postgres.WithReplyEventBuilder(replyBuilder))
	if err != nil {
		return nil, joinCloseError(err, runners.Close, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	audit, err := postgres.NewAuditStore(deps.store)
	if err != nil {
		return nil, joinCloseError(err, runners.Close, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	replySender, err := worker.NewReplySender(
		deps.store,
		deps.store,
		runtimeReplyProviderResolver{store: deps.store, secrets: secrets},
		worker.ReplySenderOptions{Owner: deps.owner},
	)
	if err != nil {
		return nil, joinCloseError(err, runners.Close, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	executor := worker.New(
		deps.store,
		storage.StaticResolver{},
		worker.WithRunner(runners),
		worker.WithSessionLocker(locker),
		worker.WithEventSink(events),
		worker.WithAuditSink(audit),
	)
	consumer, err := worker.NewConsumer(executor, deps.stream, deps.store, deps.owner)
	if err != nil {
		return nil, joinCloseError(err, runners.Close, knowledge.Close, knowledgeSources.Close, artifacts.Close, memories.Close, sessionRouter.Close)
	}
	return &workerRuntime{
		consumer:         consumer,
		runners:          runners,
		sessions:         sessionRouter,
		postgresSessions: sessions,
		memories:         memories,
		artifacts:        artifacts,
		knowledgeSources: knowledgeSources,
		knowledge:        knowledge,
		artifactServices: artifactServices,
		replySender:      replySender,
		store:            deps.store,
		owner:            deps.owner,
	}, nil
}

type runtimeReplyCapabilityResolver struct{}

func (runtimeReplyCapabilityResolver) ResolveReplyCapability(
	_ context.Context,
	exec worker.Execution,
) (channels.ProviderCapability, error) {
	switch channels.Channel(exec.Tenant.Channel) {
	case channels.ChannelWeCom:
		return (&wecom.OutboundClient{}).Capability(), nil
	case channels.ChannelFeishu:
		return (&feishu.OutboundClient{}).Capability(), nil
	default:
		return channels.ProviderCapability{}, fmt.Errorf("unsupported reply channel %q", exec.Tenant.Channel)
	}
}

type runtimeReplyProviderResolver struct {
	store   *postgres.Store
	secrets secret.SecretProvider
}

func (r runtimeReplyProviderResolver) ResolveReplyProvider(
	ctx context.Context,
	delivery worker.ReplyDelivery,
) (worker.ReplyProvider, error) {
	if r.store == nil || r.secrets == nil {
		return worker.ReplyProvider{}, errors.New("reply provider resolver is not initialized")
	}
	binding, err := r.store.ResolveBinding(
		ctx,
		delivery.Reply.TenantID,
		delivery.Reply.AppID,
		delivery.Reply.BindingID,
	)
	if err != nil {
		return worker.ReplyProvider{}, err
	}
	if binding.Status != channels.BindingActive {
		return worker.ReplyProvider{}, worker.ErrReplyBindingInactive
	}
	if binding.Channel != delivery.Reply.Channel || binding.BindingRevision != delivery.Reply.BindingRevision {
		return worker.ReplyProvider{}, worker.ErrReplyBindingChanged
	}
	switch binding.Channel {
	case channels.ChannelWeCom:
		client := wecom.NewOutboundClient(nil)
		return worker.ReplyProvider{Capability: client.Capability(), Client: client}, nil
	case channels.ChannelFeishu:
		client, err := feishu.NewOutboundClient(ctx, r.secrets, binding.Snapshot())
		if err != nil {
			return worker.ReplyProvider{}, err
		}
		return worker.ReplyProvider{Capability: client.Capability(), Client: client}, nil
	default:
		return worker.ReplyProvider{}, fmt.Errorf("unsupported reply channel %q", binding.Channel)
	}
}

type noToolsResolver struct{}

func (noToolsResolver) ResolveTools(ctx context.Context, exec worker.Execution) ([]frameworktool.Tool, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	if len(exec.Config.Tools.VisibleTools) > 0 || len(exec.Config.Tools.ExecutableTools) > 0 {
		return nil, errors.New("configured tools are not supported by this runtime")
	}
	return nil, nil
}

func (noToolsResolver) ValidateToolPolicy(_ context.Context, policy tenant.ToolPolicy) error {
	if len(policy.VisibleTools) > 0 || len(policy.ExecutableTools) > 0 {
		return errors.New("configured tools are not supported by this runtime")
	}
	return nil
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
	return errors.Join(r.runners.Close(), r.sessions.Close(), r.memories.Close(), r.artifacts.Close(), r.knowledgeSources.Close(), r.knowledge.Close())
}

func (r *workerRuntime) runDataMigrations(ctx context.Context) error {
	if r == nil || r.store == nil || r.sessions == nil || r.postgresSessions == nil || r.artifactServices == nil || r.knowledgeSources == nil || r.knowledge == nil || r.owner == "" {
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
		if err := r.runArtifactCleanupPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("artifact cleanup worker pass failed: %s", platformlog.SafeError(err))
		}
		if err := r.runKnowledgeIndexPass(ctx); err != nil && ctx.Err() == nil {
			log.Printf("knowledge index worker pass failed: %s", platformlog.SafeError(err))
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
	// Executor persists copy and verification failures as FAILED. Keep this
	// worker available for unrelated Sessions and migrations.
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
	if err := r.store.UpdateDataMigrationReport(
		context.WithoutCancel(ctx), record, record.Progress, record.Validation, platformlog.SafeError(cause),
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
	log.Printf("data migration %s failed: %s", record.ID, platformlog.SafeError(cause))
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
	return r.runArtifactCleanup(ctx, record)
}

func (r *workerRuntime) runArtifactCleanup(ctx context.Context, record platformartifact.CleanupRecord) error {
	if err := record.Validate(); err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	leaseDone := make(chan error, 1)
	go r.renewArtifactCleanupLease(runCtx, cancel, record, leaseDone)

	config, err := r.store.ResolveAppConfig(runCtx, record.TenantID, record.AppID, record.ConfigVersion)
	if err == nil {
		var exec worker.Execution
		exec, err = artifactCleanupExecution(runCtx, record, config)
		if err == nil {
			err = deleteArtifactCleanup(runCtx, r.artifactServices, exec, record)
		}
	}
	cancel()
	leaseErr := <-leaseDone
	if errors.Is(err, platformartifact.ErrCleanupLeaseLost) || errors.Is(leaseErr, platformartifact.ErrCleanupLeaseLost) {
		return nil
	}
	if leaseErr != nil {
		leaseErr = fmt.Errorf("renew artifact cleanup lease: %w", leaseErr)
		if err != nil {
			return errors.Join(err, leaseErr)
		}
		return leaseErr
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

func deleteArtifactCleanup(
	ctx context.Context,
	executor artifactCleanupExecutor,
	exec worker.Execution,
	record platformartifact.CleanupRecord,
) error {
	deleteCtx, cancel := context.WithTimeout(ctx, artifactCleanupTimeout)
	defer cancel()
	return executor.DeleteCleanup(deleteCtx, exec, record)
}

func (r *workerRuntime) renewArtifactCleanupLease(
	ctx context.Context,
	cancel context.CancelFunc,
	record platformartifact.CleanupRecord,
	done chan<- error,
) {
	renewLease(ctx, cancel, artifactCleanupLease, done, func(renewCtx context.Context) error {
		return r.store.RenewArtifactCleanup(renewCtx, record, artifactCleanupLease)
	})
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
	if ctx.Err() != nil {
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
	renewLease(ctx, cancel, knowledgeIndexLease, done, func(renewCtx context.Context) error {
		return r.store.RenewKnowledgeIndex(renewCtx, job, knowledgeIndexLease)
	})
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
		chunking.WithChunkSize(knowledgeChunkSize),
		chunking.WithOverlap(knowledgeChunkOverlap),
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
