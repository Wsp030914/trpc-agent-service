// Package postgres wires tenant-scoped PostgreSQL Session services.
package postgres

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionpostgres "trpc.group/trpc-go/trpc-agent-go/session/postgres"
)

const defaultSessionSchema = "agent"

var postgresIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// SessionDSNResolver resolves the PostgreSQL DSN for a configured session backend.
type SessionDSNResolver interface {
	ResolveSessionDSN(context.Context, storage.Handle) (string, error)
}

// SessionResolver caches framework PostgreSQL session services selected by an
// immutable application configuration. Session concurrency is owned by the
// Redis lease at the worker boundary, not by this provider.
type SessionResolver struct {
	dsns     SessionDSNResolver
	mu       sync.Mutex
	closed   bool
	services map[string]session.Service
}

// NewSessionResolver creates a PostgreSQL session resolver.
func NewSessionResolver(dsns SessionDSNResolver) (*SessionResolver, error) {
	if dsns == nil {
		return nil, errors.New("session dsn resolver is required")
	}
	return &SessionResolver{dsns: dsns, services: make(map[string]session.Service)}, nil
}

// ResolveSession returns the configured framework PostgreSQL session service.
func (r *SessionResolver) ResolveSession(ctx context.Context, exec worker.Execution) (session.Service, error) {
	if r == nil || r.dsns == nil {
		return nil, errors.New("postgres session resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return nil, err
	}
	handle := exec.Storage.Session
	if err := handle.Validate(exec.Tenant.Scope(), storage.CapabilitySession, exec.Config.BackendConfig.Session); err != nil {
		return nil, fmt.Errorf("session storage handle: %w", err)
	}
	if handle.Ref.Kind != tenant.BackendSQL || (handle.Ref.Provider != "" && handle.Ref.Provider != "postgres") {
		return nil, fmt.Errorf("session backend %q must use postgres provider", handle.Ref.Name)
	}
	schema, err := sessionSchema(handle.Ref)
	if err != nil {
		return nil, err
	}
	key, err := r.sessionCacheKey(exec)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("postgres session resolver is closed")
	}
	if value := r.services[key]; value != nil {
		return value, nil
	}
	dsn, err := r.dsns.ResolveSessionDSN(ctx, handle)
	if err != nil {
		return nil, fmt.Errorf("resolve session dsn: %w", err)
	}
	if dsn == "" {
		return nil, errors.New("session dsn is required")
	}
	if err := ensureSchema(ctx, dsn, schema); err != nil {
		return nil, err
	}
	service, err := sessionpostgres.NewService(sessionpostgres.WithPostgresClientDSN(dsn), sessionpostgres.WithSchema(schema))
	if err != nil {
		return nil, fmt.Errorf("create postgres session service: %w", err)
	}
	r.services[key] = service
	return service, nil
}

func ensureSchema(ctx context.Context, dsn, schema string) error {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return fmt.Errorf("connect postgres session backend: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+pgx.Identifier{schema}.Sanitize()); err != nil {
		return fmt.Errorf("create postgres session schema: %w", err)
	}
	return nil
}

// Close closes all cached session services.
func (r *SessionResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	services := r.services
	r.services = nil
	r.mu.Unlock()
	var errs []error
	for _, service := range services {
		if err := service.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
func sessionSchema(ref tenant.BackendRef) (string, error) {
	schema := ref.Options["schema"]
	if schema == "" {
		schema = defaultSessionSchema
	}
	if !postgresIdentifier.MatchString(schema) {
		return "", errors.New("session backend schema is invalid")
	}
	return schema, nil
}
func (r *SessionResolver) sessionCacheKey(exec worker.Execution) (string, error) {
	schema, err := sessionSchema(exec.Storage.Session.Ref)
	if err != nil {
		return "", err
	}
	parts := []string{exec.Tenant.ConfigVersion, exec.Config.BackendConfig.Name, exec.Storage.Session.Ref.Name, schema}
	if ref := exec.Storage.Session.Ref.SecretRef; ref != (tenant.SecretRef{}) {
		parts = append(parts, ref.Name, ref.Version)
	} else if ref := exec.Storage.Session.Ref.DSNRef; ref != "" {
		parts = append(parts, ref)
	}
	return exec.Tenant.Scope().Key("session-service", parts...)
}
