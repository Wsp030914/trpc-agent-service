// Package redispostgres implements the supported Redis-to-PostgreSQL Session
// migration adapter.
package redispostgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	sessionpostgres "github.com/liuzengh/trpc-agent-service/trpcservice/session/postgres"
	sessionredis "github.com/liuzengh/trpc-agent-service/trpcservice/session/redis"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

const migrationPrincipal = "data-migration"

// Copier resolves the fixed Redis source and PostgreSQL target for one
// migration record and copies complete Session data between them.
type Copier struct {
	core     migration.RedisPostgresCopier
	importer *sessionpostgres.SummaryImporter
}

// NewCopier resolves the source and target Session services selected by record.
// The caller must Close the returned copier after the migration run ends.
func NewCopier(
	ctx context.Context,
	configs config.Resolver,
	stores storage.Resolver,
	sessions platformsession.Resolver,
	postgresSessions *sessionpostgres.SessionResolver,
	redisSessions *sessionredis.SessionResolver,
	record migration.Record,
) (*Copier, error) {
	if configs == nil || stores == nil || sessions == nil || postgresSessions == nil || redisSessions == nil {
		return nil, errors.New("data migration copier dependencies are required")
	}
	if err := record.Validate(); err != nil {
		return nil, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sourceExec, err := resolveExecution(ctx, configs, stores, record, record.SourceConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve data migration source: %w", err)
	}
	targetExec, err := resolveExecution(ctx, configs, stores, record, record.TargetConfigVersion)
	if err != nil {
		return nil, fmt.Errorf("resolve data migration target: %w", err)
	}
	if !isRedisSession(sourceExec.Config.BackendConfig.Session) {
		return nil, errors.New("data migration source session backend must use redis")
	}
	if !isPostgresSession(targetExec.Config.BackendConfig.Session) {
		return nil, errors.New("data migration target session backend must use postgres")
	}
	if err := redisSessions.CheckSessionBackend(ctx, sourceExec); err != nil {
		return nil, fmt.Errorf("check redis source session backend: %w", err)
	}
	source, err := sessions.ResolveSession(ctx, sourceExec)
	if err != nil {
		return nil, fmt.Errorf("resolve redis source session: %w", err)
	}
	target, err := sessions.ResolveSession(ctx, targetExec)
	if err != nil {
		return nil, fmt.Errorf("resolve postgres target session: %w", err)
	}
	importer, err := postgresSessions.NewSummaryImporter(ctx, targetExec)
	if err != nil {
		return nil, err
	}
	return &Copier{
		core: migration.RedisPostgresCopier{
			Source:    source,
			Target:    target,
			Summaries: importer,
		},
		importer: importer,
	}, nil
}

// CopySession moves one Session selected by the platform-owned catalog.
func (c *Copier) CopySession(ctx context.Context, key session.Key) error {
	if c == nil {
		return errors.New("redis postgres migration copier is required")
	}
	return c.core.CopySession(ctx, key)
}

// VerifySession confirms one copied Session retains its authoritative data.
func (c *Copier) VerifySession(ctx context.Context, key session.Key) error {
	if c == nil {
		return errors.New("redis postgres migration copier is required")
	}
	return c.core.VerifySession(ctx, key)
}

// Close releases the dedicated PostgreSQL summary import connection pool.
func (c *Copier) Close() {
	if c != nil && c.importer != nil {
		c.importer.Close()
		c.importer = nil
	}
}

func resolveExecution(
	ctx context.Context,
	configs config.Resolver,
	stores storage.Resolver,
	record migration.Record,
	version string,
) (worker.Execution, error) {
	runtime := tenant.RuntimeContext{
		TenantID:           record.TenantID,
		AppID:              record.AppID,
		ConfigVersion:      version,
		SessionPrincipalID: migrationPrincipal,
		SessionID:          record.ID,
		UserID:             migrationPrincipal,
		TraceID:            record.ID,
	}
	cfg, err := configs.ResolveAppConfig(ctx, record.TenantID, record.AppID, version)
	if err != nil {
		return worker.Execution{}, err
	}
	if err := cfg.Validate(); err != nil {
		return worker.Execution{}, fmt.Errorf("app config: %w", err)
	}
	if cfg.TenantID != record.TenantID || cfg.AppID != record.AppID || cfg.Version != version {
		return worker.Execution{}, errors.New("resolved app config does not match data migration")
	}
	handles, err := stores.Resolve(ctx, runtime, cfg.BackendConfig)
	if err != nil {
		return worker.Execution{}, err
	}
	return worker.Execution{Tenant: runtime, Config: cfg, Storage: handles}, nil
}

func isRedisSession(ref tenant.BackendRef) bool {
	return ref.Kind == tenant.BackendRedis && (ref.Provider == "" || ref.Provider == "redis")
}

func isPostgresSession(ref tenant.BackendRef) bool {
	return ref.Kind == tenant.BackendSQL && (ref.Provider == "" || ref.Provider == "postgres")
}
