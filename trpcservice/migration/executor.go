package migration

import (
	"context"
	"errors"
	"fmt"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"trpc.group/trpc-go/trpc-agent-go/session"
)

// SessionCatalog lists every logical Session in the platform scope being migrated.
// The catalog must be platform-owned rather than discovered by scanning backend keys.
type SessionCatalog interface {
	ListDataMigrationSessionKeys(context.Context, Record) ([]session.Key, error)
}

// Repository owns durable migration state transitions.
type Repository interface {
	AdvanceDataMigration(context.Context, Record, Status) error
}

// SessionCopier moves and verifies one logical Session between fixed source and
// target providers. It must preserve Event, State, and Summary data.
type SessionCopier interface {
	CopySession(context.Context, session.Key) error
	VerifySession(context.Context, session.Key) error
}

// Executor performs the copy and verification phases of one claimed migration.
// Draining is entered by the control plane before Run is called.
type Executor struct {
	Catalog    SessionCatalog
	Repository Repository
	Copier     SessionCopier
}

// Run advances a DRAINING migration through COPYING and VERIFYING to SUCCEEDED.
// A data failure is recorded as FAILED. Context cancellation and lease failures
// are returned to allow another owner to safely resume the claimed record.
func (e Executor) Run(ctx context.Context, record Record) error {
	if err := e.validate(); err != nil {
		return err
	}
	if err := record.Validate(); err != nil {
		return err
	}
	if record.Status != StatusDraining && record.Status != StatusCopying && record.Status != StatusVerifying {
		return errors.New("data migration is not runnable")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	keys, err := e.Catalog.ListDataMigrationSessionKeys(ctx, record)
	if err != nil {
		return e.fail(ctx, record, fmt.Errorf("list migration sessions: %w", err))
	}

	if record.Status == StatusDraining {
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusCopying); err != nil {
			if errors.Is(err, ErrDrainDeadlineExceeded) {
				return e.fail(ctx, record, err)
			}
			return err
		}
		record.Status = StatusCopying
	}
	if record.Status == StatusCopying {
		for index := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.Copier.CopySession(ctx, keys[index]); err != nil {
				return e.fail(ctx, record, fmt.Errorf("copy session %d: %w", index, err))
			}
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusVerifying); err != nil {
			return err
		}
		record.Status = StatusVerifying
	}
	if record.Status == StatusVerifying {
		for index := range keys {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := e.Copier.VerifySession(ctx, keys[index]); err != nil {
				return e.fail(ctx, record, fmt.Errorf("verify session %d: %w", index, err))
			}
		}
		if err := e.Repository.AdvanceDataMigration(ctx, record, StatusSucceeded); err != nil {
			return err
		}
	}
	return nil
}

func (e Executor) fail(ctx context.Context, record Record, cause error) error {
	if ctx.Err() != nil {
		return cause
	}
	record.FailureReason = platformlog.SafeError(cause)
	if err := e.Repository.AdvanceDataMigration(ctx, record, StatusFailed); err != nil {
		return errors.Join(cause, err)
	}
	return cause
}

func (e Executor) validate() error {
	if e.Catalog == nil || e.Repository == nil || e.Copier == nil {
		return errors.New("data migration executor dependencies are required")
	}
	return nil
}
