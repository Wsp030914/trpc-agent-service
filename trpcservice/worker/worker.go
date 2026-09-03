// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const defaultEventSinkTimeout = 5 * time.Second

// ErrExecutionCanceled means a verified recall stopped the current run.
var ErrExecutionCanceled = errors.New("execution canceled by recall")

// CancellationCheck reads the authoritative canceled state for one execution.
// Recall itself owns the durable state transition; the worker only reads it.
type CancellationCheck func(context.Context, string, string, string) (bool, error)

// Execution is the prepared context for running one tenant-scoped job.
type Execution struct {
	RequestID    string
	TenantSource gateway.TenantSource
	Tenant       tenant.RuntimeContext
	Config       tenant.AppConfig
	Message      gateway.Message
	PartitionKey string
}

// SessionLock owns an acquired session partition lock. Context is canceled
// when the authoritative backend can no longer guarantee that the lock is
// held. Release must be called after the runner event channel is drained.
type SessionLock interface {
	Context() context.Context
	Release() error
}

// SessionLocker serializes runner execution for one session partition.
// Production workers must use an authoritative shared backend implementation.
type SessionLocker interface {
	Lock(ctx context.Context, partitionKey string) (SessionLock, error)
}

// EventSink receives runner events after the worker has started execution.
// Implementations must stop work and return when ctx is done.
type EventSink interface {
	HandleRunnerEvent(ctx context.Context, exec Execution, evt *event.Event) error
}

// RunResult summarizes one worker-owned runner execution.
type RunResult struct {
	Execution       Execution
	EventCount      int
	RunnerCompleted bool
}

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Config           config.Resolver
	Runner           func(context.Context, Execution) (runner.Runner, error)
	SessionLocker    SessionLocker
	Events           EventSink
	EventSinkTimeout time.Duration
	Cancellation     CancellationCheck
}

// New creates a Worker with all execution dependencies explicitly attached.
func New(
	configResolver config.Resolver,
	runnerBuilder func(context.Context, Execution) (runner.Runner, error),
	sessionLocker SessionLocker,
	events EventSink,
	cancellation CancellationCheck,
) *Worker {
	return &Worker{
		Config:        configResolver,
		Runner:        runnerBuilder,
		SessionLocker: sessionLocker,
		Events:        events,
		Cancellation:  cancellation,
	}
}

// Prepare validates a job and resolves the tenant backend_config.
func (w Worker) Prepare(ctx context.Context, job execution.Job) (Execution, error) {
	if err := job.Validate(); err != nil {
		return Execution{}, err
	}
	partitionKey, err := job.PartitionKey()
	if err != nil {
		return Execution{}, err
	}
	if w.Config == nil {
		return Execution{}, errors.New("config resolver is required")
	}
	tenantContext := job.Tenant()
	cfg, err := w.Config.ResolveAppConfig(
		ctx,
		tenantContext.TenantID,
		tenantContext.AppID,
		tenantContext.ConfigVersion,
	)
	if err != nil {
		return Execution{}, err
	}
	if cfg.TenantID != tenantContext.TenantID ||
		cfg.AppID != tenantContext.AppID ||
		cfg.Version != tenantContext.ConfigVersion {
		return Execution{}, errors.New("resolved app config does not match job scope")
	}
	requestID := job.RequestID()
	if w.Cancellation != nil {
		canceled, err := w.Cancellation(ctx, tenantContext.TenantID, tenantContext.AppID, requestID)
		if err != nil {
			return Execution{}, fmt.Errorf("check execution cancellation: %w", err)
		}
		if canceled {
			return Execution{}, ErrExecutionCanceled
		}
	}
	return Execution{
		RequestID:    requestID,
		TenantSource: job.TenantSource(),
		Tenant:       tenantContext,
		Config:       cfg,
		Message:      job.Message(),
		PartitionKey: partitionKey,
	}, nil
}

// Run prepares a job, calls runner.Runner, and drains the returned event channel.
// After draining, it returns the first sink error or terminal runner error.
// It uses SessionPrincipalID as the runner user ID so group and thread messages
// share a session, while UserID remains available in runtime state as the sender.
func (w Worker) Run(ctx context.Context, job execution.Job) (result RunResult, err error) {
	if ctx == nil {
		ctx = context.Background()
	}
	exec, err := w.Prepare(ctx, job)
	if err != nil {
		return RunResult{}, err
	}
	result = RunResult{Execution: exec}
	if w.Runner == nil {
		return result, errors.New("runner builder is required")
	}
	message, err := w.runnerMessage(exec)
	if err != nil {
		return result, err
	}
	appName, err := exec.Tenant.Scope().Key("runner")
	if err != nil {
		return result, err
	}
	if w.SessionLocker == nil {
		return result, errors.New("session locker is required")
	}
	lock, err := w.SessionLocker.Lock(ctx, exec.PartitionKey)
	if err != nil {
		return result, fmt.Errorf("lock session partition: %w", err)
	}
	if lock == nil {
		return result, errors.New("session lock is required")
	}
	defer func() {
		if releaseErr := lock.Release(); releaseErr != nil {
			err = errors.Join(err, fmt.Errorf("release session partition: %w", releaseErr))
		}
	}()
	runCtx := lock.Context()
	if runCtx == nil {
		return result, errors.New("session lock context is required")
	}
	r, err := w.Runner(runCtx, exec)
	if err != nil {
		return result, err
	}
	if r == nil {
		return result, errors.New("runner is required")
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("close runner: %w", closeErr))
		}
	}()
	events, err := r.Run(
		runCtx,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		message,
		agent.WithRequestID(exec.RequestID),
		agent.WithAppName(appName),
		agent.MergeRuntimeState(runnerRuntimeState(exec)),
		agent.WithToolPermissionPolicy(w.toolPermissionPolicy(exec)),
	)
	if err != nil {
		return result, err
	}
	if events == nil {
		return result, errors.New("runner event channel is nil")
	}
	stopManagedCancel := cancelManagedRunnerOnContextDone(runCtx, r, exec.RequestID)
	defer stopManagedCancel()
	var sinkErr error
	var runnerErr error
	sinkTimedOut := false
	for evt := range events {
		if evt == nil {
			continue
		}
		result.EventCount++
		if evt.IsRunnerCompletion() {
			result.RunnerCompleted = true
		}
		if runnerErr == nil && evt.IsTerminalError() {
			runnerErr = fmt.Errorf("runner event: %w", evt.Error)
		}
		if w.Events == nil || sinkTimedOut || runCtx.Err() != nil {
			continue
		}
		if err := w.handleRunnerEvent(runCtx, exec, evt); err != nil {
			if sinkErr == nil {
				sinkErr = err
			}
			if errors.Is(err, context.DeadlineExceeded) {
				sinkTimedOut = true
			}
		}
	}
	if err := runCtx.Err(); err != nil {
		return result, err
	}
	if sinkErr != nil {
		return result, sinkErr
	}
	if runnerErr != nil {
		return result, runnerErr
	}
	return result, nil
}

func cancelManagedRunnerOnContextDone(
	ctx context.Context,
	r runner.Runner,
	requestID string,
) func() {
	managed, ok := r.(runner.ManagedRunner)
	if !ok || requestID == "" {
		return func() {}
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			managed.Cancel(requestID)
		case <-done:
		}
	}()
	return func() { close(done) }
}

func (w Worker) toolPermissionPolicy(exec Execution) frameworktool.PermissionPolicy {
	return frameworktool.PermissionPolicyFunc(func(
		_ context.Context,
		request *frameworktool.PermissionRequest,
	) (frameworktool.PermissionDecision, error) {
		name := permissionToolName(request)
		if err := platformtool.AuthorizeExecution(exec.Config.Tools, name); err != nil {
			return frameworktool.DenyPermission("tool is not authorized"), nil
		}
		return frameworktool.NormalizePermissionDecision(frameworktool.AllowPermission())
	})
}

func permissionToolName(request *frameworktool.PermissionRequest) string {
	if request == nil {
		return ""
	}
	if request.ToolName != "" {
		return request.ToolName
	}
	if request.Declaration != nil {
		return request.Declaration.Name
	}
	return ""
}

func (w Worker) handleRunnerEvent(ctx context.Context, exec Execution, evt *event.Event) error {
	sinkCtx, cancel := context.WithTimeout(ctx, w.eventSinkTimeout())
	defer cancel()

	err := w.Events.HandleRunnerEvent(sinkCtx, exec, evt)
	if sinkCtx.Err() != nil {
		return fmt.Errorf("event sink timeout: %w", sinkCtx.Err())
	}
	return err
}

func (w Worker) eventSinkTimeout() time.Duration {
	if w.EventSinkTimeout > 0 {
		return w.EventSinkTimeout
	}
	return defaultEventSinkTimeout
}

func (w Worker) runnerMessage(exec Execution) (model.Message, error) {
	message := exec.Message
	result := model.NewUserMessage(message.Text)
	if len(message.ArtifactRefs) == 0 {
		return result, nil
	}
	if exec.Config.BackendConfig.Artifact.IsZero() {
		return model.Message{}, errors.New("artifact backend is required for inbound artifacts")
	}
	for index, ref := range message.ArtifactRefs {
		_, version, err := gateway.ParseArtifactRef(ref)
		if err != nil {
			return model.Message{}, fmt.Errorf("artifact ref %d: %w", index, err)
		}
		contentRef := &model.ContentRef{ArtifactRef: ref, ArtifactVersion: version}
		result.ContentParts = append(result.ContentParts, model.ContentPart{
			Type:       model.ContentTypeFile,
			ContentRef: contentRef,
		})
	}
	return result, nil
}

func runnerRuntimeState(exec Execution) map[string]any {
	state := map[string]any{
		"tenant_id":            exec.Tenant.TenantID,
		"app_id":               exec.Tenant.AppID,
		"config_version":       exec.Tenant.ConfigVersion,
		"request_id":           exec.RequestID,
		"session_id":           exec.Tenant.SessionID,
		"session_principal_id": exec.Tenant.SessionPrincipalID,
		"user_id":              exec.Tenant.UserID,
	}
	if exec.Tenant.TraceID != "" {
		state["trace_id"] = exec.Tenant.TraceID
	}
	if exec.Tenant.Channel != "" {
		state["channel"] = exec.Tenant.Channel
	}
	if exec.Tenant.BindingID != "" {
		state["binding_id"] = exec.Tenant.BindingID
	}
	return state
}
