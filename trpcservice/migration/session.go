package migration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

// ErrSummaryImportRequired reports that a Session with persisted summaries
// needs a backend-specific summary importer.
var ErrSummaryImportRequired = errors.New("session summary import is required")

// SummaryImporter writes existing summaries to a target backend without
// regenerating their text or boundaries.
type SummaryImporter interface {
	ReplaceSessionSummaries(context.Context, session.Key, map[string]*session.Summary) error
}

// RedisPostgresCopier copies the Redis-to-PostgreSQL Session migration path.
// Source and Target must be the official Redis and PostgreSQL providers for
// the fixed migration configuration versions.
type RedisPostgresCopier struct {
	Source    session.Service
	Target    session.Service
	Summaries SummaryImporter
}

// CopySession moves Event, State, Track, and Summary data for key.
func (c RedisPostgresCopier) CopySession(ctx context.Context, key session.Key) error {
	_, err := copySession(ctx, c.Source, c.Target, key, c.Summaries)
	return err
}

// VerifySession checks the complete migrated Session for key.
func (c RedisPostgresCopier) VerifySession(ctx context.Context, key session.Key) error {
	return verifySession(ctx, c.Source, c.Target, key, true)
}

// CopySession copies Event, State, and Track data selected by key into target.
// It rejects source summaries because session.Service has no portable API for
// importing an existing summary. The caller must stop source writers first.
func CopySession(ctx context.Context, source, target session.Service, key session.Key) (bool, error) {
	return copySession(ctx, source, target, key, nil)
}

func copySession(
	ctx context.Context,
	source, target session.Service,
	key session.Key,
	summaries SummaryImporter,
) (bool, error) {
	if source == nil || target == nil {
		return false, errors.New("source and target session services are required")
	}
	if err := key.CheckSessionKey(); err != nil {
		return false, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}

	sourceSession, err := source.GetSession(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get source session: %w", err)
	}
	if sourceSession == nil {
		return false, nil
	}
	if sourceSession.ID != key.SessionID || sourceSession.AppName != key.AppName || sourceSession.UserID != key.UserID {
		return false, errors.New("source session does not match key")
	}
	sourceSession = sourceSession.Clone()
	if len(sourceSession.Summaries) > 0 && summaries == nil {
		return false, ErrSummaryImportRequired
	}

	targetSession, err := target.GetSession(ctx, key)
	if err != nil {
		return false, fmt.Errorf("get target session: %w", err)
	}
	if targetSession != nil {
		if err := target.DeleteSession(ctx, key); err != nil {
			return false, fmt.Errorf("replace target session: %w", err)
		}
	}
	targetSession, err = target.CreateSession(ctx, key, cloneState(sourceSession.State))
	if err != nil {
		return false, fmt.Errorf("create target session: %w", err)
	}
	if targetSession == nil {
		return false, errors.New("created target session is required")
	}
	for index := range sourceSession.Events {
		event := sourceSession.Events[index]
		if err := target.AppendEvent(ctx, targetSession, &event); err != nil {
			return false, fmt.Errorf("append target event %d: %w", index, err)
		}
	}
	if err := copyTracks(ctx, target, targetSession, sourceSession); err != nil {
		return false, err
	}
	if err := target.UpdateSessionState(ctx, key, cloneState(sourceSession.State)); err != nil {
		return false, fmt.Errorf("update target session state: %w", err)
	}
	if len(sourceSession.Summaries) > 0 {
		if err := summaries.ReplaceSessionSummaries(ctx, key, cloneSummaries(sourceSession.Summaries)); err != nil {
			return false, fmt.Errorf("import target summaries: %w", err)
		}
	}
	return true, nil
}

// VerifySession checks that source and target contain equivalent Session
// content for key. A source session that does not exist is equivalent only to
// an absent target session.
func VerifySession(ctx context.Context, source, target session.Service, key session.Key) error {
	return verifySession(ctx, source, target, key, false)
}

func verifySession(
	ctx context.Context,
	source, target session.Service,
	key session.Key,
	verifySummaries bool,
) error {
	if source == nil || target == nil {
		return errors.New("source and target session services are required")
	}
	if err := key.CheckSessionKey(); err != nil {
		return err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	sourceSession, err := source.GetSession(ctx, key)
	if err != nil {
		return fmt.Errorf("get source session: %w", err)
	}
	targetSession, err := target.GetSession(ctx, key)
	if err != nil {
		return fmt.Errorf("get target session: %w", err)
	}
	if sourceSession == nil {
		if targetSession != nil {
			return errors.New("target session exists without source session")
		}
		return nil
	}
	if targetSession == nil {
		return errors.New("target session is missing")
	}
	if sourceSession.ID != targetSession.ID ||
		sourceSession.AppName != targetSession.AppName ||
		sourceSession.UserID != targetSession.UserID {
		return errors.New("target session identity does not match source")
	}
	if !equalState(sourceSession.State, targetSession.State) {
		return errors.New("target session state does not match source")
	}
	if !reflect.DeepEqual(sourceSession.Events, targetSession.Events) {
		return errors.New("target session events do not match source")
	}
	if !reflect.DeepEqual(sourceSession.Tracks, targetSession.Tracks) {
		return errors.New("target session tracks do not match source")
	}
	if len(sourceSession.Summaries) > 0 && !verifySummaries {
		return ErrSummaryImportRequired
	}
	if verifySummaries && !equalSummaries(sourceSession.Summaries, targetSession.Summaries) {
		return errors.New("target session summaries do not match source")
	}
	return nil
}

func copyTracks(ctx context.Context, target session.Service, targetSession, sourceSession *session.Session) error {
	if len(sourceSession.Tracks) == 0 {
		return nil
	}
	trackTarget, ok := target.(session.TrackService)
	if !ok {
		return errors.New("target session service does not support track import")
	}
	for _, history := range sourceSession.Tracks {
		if history == nil {
			continue
		}
		for index := range history.Events {
			trackEvent := history.Events[index]
			if err := trackTarget.AppendTrackEvent(ctx, targetSession, &trackEvent); err != nil {
				return fmt.Errorf("append target track event %q/%d: %w", history.Track, index, err)
			}
		}
	}
	return nil
}

func cloneSummaries(source map[string]*session.Summary) map[string]*session.Summary {
	if source == nil {
		return nil
	}
	target := make(map[string]*session.Summary, len(source))
	for key, value := range source {
		target[key] = value.Clone()
	}
	return target
}

func equalSummaries(left, right map[string]*session.Summary) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftSummary := range left {
		rightSummary, ok := right[key]
		if !ok || !equalSummary(leftSummary, rightSummary) {
			return false
		}
	}
	return true
}

func equalSummary(left, right *session.Summary) bool {
	if left == nil || right == nil {
		return left == right
	}
	if left.Summary != right.Summary || !reflect.DeepEqual(left.Topics, right.Topics) ||
		!left.UpdatedAt.Equal(right.UpdatedAt) {
		return false
	}
	if left.Boundary == nil || right.Boundary == nil {
		return left.Boundary == right.Boundary
	}
	return left.Boundary.Version == right.Boundary.Version &&
		left.Boundary.FilterKey == right.Boundary.FilterKey &&
		left.Boundary.LastEventID == right.Boundary.LastEventID &&
		left.Boundary.CutoffAt.Equal(right.Boundary.CutoffAt)
}

func cloneState(source session.StateMap) session.StateMap {
	if source == nil {
		return nil
	}
	target := make(session.StateMap, len(source))
	for key, value := range source {
		target[key] = bytes.Clone(value)
	}
	return target
}

func equalState(left, right session.StateMap) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftValue := range left {
		if !bytes.Equal(leftValue, right[key]) {
			return false
		}
	}
	return true
}
