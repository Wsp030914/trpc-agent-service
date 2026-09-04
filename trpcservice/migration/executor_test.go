package migration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestExecutorCopiesAndVerifiesSessions(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusDraining,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	key := session.Key{AppName: "tenant:tenant-1:app:app-1:runner", UserID: "user-1", SessionID: "session-1"}
	repository := &testMigrationRepository{}
	copier := &testSessionCopier{}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{keys: []session.Key{key}},
		Repository: repository,
		Copier:     copier,
	}

	if err := executor.Run(context.Background(), record); err != nil {
		t.Fatalf("run migration: %v", err)
	}
	if copier.copied != 1 || copier.verified != 1 {
		t.Fatalf("copied=%d verified=%d, want 1 each", copier.copied, copier.verified)
	}
	if len(repository.transitions) != 3 ||
		repository.transitions[0] != migration.StatusCopying ||
		repository.transitions[1] != migration.StatusVerifying ||
		repository.transitions[2] != migration.StatusSucceeded {
		t.Fatalf("transitions=%v, want COPYING VERIFYING SUCCEEDED", repository.transitions)
	}
}

func TestExecutorFailsWhenDrainDeadlineExpires(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "migration-1",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusDraining,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	repository := &testMigrationRepository{advanceErr: migration.ErrDrainDeadlineExceeded}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{},
		Repository: repository,
		Copier:     &testSessionCopier{},
	}

	err := executor.Run(context.Background(), record)
	if !errors.Is(err, migration.ErrDrainDeadlineExceeded) {
		t.Fatalf("run migration error = %v, want drain deadline exceeded", err)
	}
	if len(repository.transitions) != 2 ||
		repository.transitions[0] != migration.StatusCopying ||
		repository.transitions[1] != migration.StatusFailed {
		t.Fatalf("transitions=%v, want COPYING FAILED", repository.transitions)
	}
	if repository.failureReason == "" {
		t.Fatalf("failure reason is empty")
	}
}

func TestExecutorLeavesDurablePhaseOpenForRetryableBackendFailure(t *testing.T) {
	t.Parallel()
	record := migration.Record{
		ID:                  "migration-retry",
		TenantID:            "tenant-1",
		AppID:               "app-1",
		SourceConfigVersion: "v1",
		TargetConfigVersion: "v2",
		Status:              migration.StatusCopying,
		LeaseOwner:          "worker-1",
		LeaseUntil:          time.Now().Add(time.Minute),
		RunToken:            "run-1",
	}
	repository := &testMigrationRepository{}
	executor := migration.Executor{
		Catalog:    testSessionCatalog{keys: []session.Key{{AppName: "tenant:tenant-1:app:app-1:runner", UserID: "user-1", SessionID: "session-1"}}},
		Repository: repository,
		Copier:     &retryingSessionCopier{},
	}

	err := executor.Run(context.Background(), record)
	if !errors.Is(err, errMigrationBackendUnavailable) {
		t.Fatalf("run migration error = %v, want backend unavailable", err)
	}
	if len(repository.transitions) != 0 {
		t.Fatalf("durable transitions = %v, want no terminal transition", repository.transitions)
	}
}

type testSessionCatalog struct {
	keys []session.Key
	err  error
}

func (c testSessionCatalog) ListDataMigrationSessionKeys(
	_ context.Context,
	_ migration.Record,
) ([]session.Key, error) {
	return c.keys, c.err
}

type testMigrationRepository struct {
	transitions   []migration.Status
	advanceErr    error
	failureReason string
}

func (r *testMigrationRepository) AdvanceDataMigration(
	_ context.Context,
	record migration.Record,
	next migration.Status,
) error {
	r.transitions = append(r.transitions, next)
	if next == migration.StatusCopying && r.advanceErr != nil {
		return r.advanceErr
	}
	if next == migration.StatusFailed {
		r.failureReason = record.FailureReason
	}
	return nil
}

type testSessionCopier struct {
	copied   int
	verified int
}

var errMigrationBackendUnavailable = errors.New("migration backend unavailable")

type retryingSessionCopier struct{}

func (*retryingSessionCopier) CopySession(_ context.Context, _ session.Key) error {
	return migration.NewRetryableError(errMigrationBackendUnavailable)
}

func (*retryingSessionCopier) VerifySession(_ context.Context, _ session.Key) error { return nil }

func (c *testSessionCopier) CopySession(_ context.Context, _ session.Key) error {
	c.copied++
	return nil
}

func (c *testSessionCopier) VerifySession(_ context.Context, _ session.Key) error {
	c.verified++
	return nil
}
