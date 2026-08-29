package migration_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/migration"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
)

func TestCopyAndVerifySession(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = source.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-a"}
	sourceSession, err := source.CreateSession(ctx, key, session.StateMap{"topic": []byte("billing")})
	if err != nil {
		t.Fatalf("create source session: %v", err)
	}
	if err := source.AppendEvent(ctx, sourceSession, event.NewResponseEvent("invocation-a", "user", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewUserMessage("summarize billing"),
		}},
	})); err != nil {
		t.Fatalf("append source user event: %v", err)
	}
	if err := source.AppendEvent(ctx, sourceSession, event.NewResponseEvent("invocation-b", "assistant", &model.Response{
		Object: model.ObjectTypeChatCompletion,
		Done:   true,
		Choices: []model.Choice{{
			Message: model.NewAssistantMessage("billing reply"),
		}},
	})); err != nil {
		t.Fatalf("append source event: %v", err)
	}

	copied, err := migration.CopySession(ctx, source, target, key)
	if err != nil {
		t.Fatalf("copy session: %v", err)
	}
	if !copied {
		t.Fatal("copy session reported missing source")
	}
	if err := migration.VerifySession(ctx, source, target, key); err != nil {
		t.Fatalf("verify copied session: %v", err)
	}
}

func TestCopySessionMissingSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	source := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = source.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-a"}

	copied, err := migration.CopySession(ctx, source, target, key)
	if err != nil {
		t.Fatalf("copy missing source: %v", err)
	}
	if copied {
		t.Fatal("copy missing source reported copied")
	}
	if err := migration.VerifySession(ctx, source, target, key); err != nil {
		t.Fatalf("verify missing session: %v", err)
	}
}

func TestCopySessionRequiresSummaryImporter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	sourceBase := inmemory.NewSessionService()
	target := inmemory.NewSessionService()
	t.Cleanup(func() { _ = sourceBase.Close() })
	t.Cleanup(func() { _ = target.Close() })
	key := session.Key{AppName: "tenant:tenant-a:app:support:runner", UserID: "user-a", SessionID: "session-a"}
	sourceSession := session.NewSession(key.AppName, key.UserID, key.SessionID)
	sourceSession.Summaries[""] = &session.Summary{
		Summary:   "billing history",
		UpdatedAt: time.Now().UTC(),
	}
	source := summarySessionService{Service: sourceBase, value: sourceSession}

	if _, err := migration.CopySession(ctx, source, target, key); !errors.Is(err, migration.ErrSummaryImportRequired) {
		t.Fatalf("copy session error = %v, want summary importer", err)
	}

	importer := &summaryImportSpy{}
	copier := migration.RedisPostgresCopier{
		Source:    source,
		Target:    target,
		Summaries: importer,
	}
	if err := copier.CopySession(ctx, key); err != nil {
		t.Fatalf("copy session with importer: %v", err)
	}
	if importer.key != key {
		t.Fatalf("import key = %#v, want %#v", importer.key, key)
	}
	if got := importer.summaries[""]; got == nil || got.Summary != "billing history" {
		t.Fatalf("imported summary = %#v", got)
	}
}

type summaryImportSpy struct {
	key       session.Key
	summaries map[string]*session.Summary
}

type summarySessionService struct {
	session.Service
	value *session.Session
}

func (s summarySessionService) GetSession(
	_ context.Context,
	_ session.Key,
	_ ...session.Option,
) (*session.Session, error) {
	return s.value.Clone(), nil
}

func (s *summaryImportSpy) ReplaceSessionSummaries(
	_ context.Context,
	key session.Key,
	summaries map[string]*session.Summary,
) error {
	s.key = key
	s.summaries = summaries
	return nil
}
