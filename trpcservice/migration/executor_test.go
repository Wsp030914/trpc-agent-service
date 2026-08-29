package migration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

func TestExecutorPersistsSessionCopyAndVerificationProgress(t *testing.T) {
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
	if len(repository.reports) != 3 {
		t.Fatalf("reports=%d, want 3", len(repository.reports))
	}
	last := repository.reports[len(repository.reports)-1]
	if last.progress != (migration.Progress{SessionCount: 1, SessionsCopied: 1, SessionsChecked: 1}) {
		t.Fatalf("last progress=%+v", last.progress)
	}
	if last.validation != (migration.Validation{SessionsVerified: 1}) {
		t.Fatalf("last validation=%+v", last.validation)
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
	if len(repository.reports) != 1 || repository.reports[0].failure == "" {
		t.Fatalf("failure reports=%#v, want one failure", repository.reports)
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
	transitions []migration.Status
	reports     []testMigrationReport
	advanceErr  error
}

type testMigrationReport struct {
	progress   migration.Progress
	validation migration.Validation
	failure    string
}

func (r *testMigrationRepository) AdvanceDataMigration(
	_ context.Context,
	_ migration.Record,
	next migration.Status,
) error {
	r.transitions = append(r.transitions, next)
	if next == migration.StatusCopying && r.advanceErr != nil {
		return r.advanceErr
	}
	return nil
}

func (r *testMigrationRepository) UpdateDataMigrationReport(
	_ context.Context,
	_ migration.Record,
	progress migration.Progress,
	validation migration.Validation,
	failure string,
) error {
	r.reports = append(r.reports, testMigrationReport{
		progress: progress, validation: validation, failure: failure,
	})
	return nil
}

type testSessionCopier struct {
	copied   int
	verified int
}

func (c *testSessionCopier) CopySession(_ context.Context, _ session.Key) error {
	c.copied++
	return nil
}

func (c *testSessionCopier) VerifySession(_ context.Context, _ session.Key) error {
	c.verified++
	return nil
}
