// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	platformapproval "github.com/liuzengh/trpc-agent-service/trpcservice/approval"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"go.opentelemetry.io/otel/attribute"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	defaultEventSinkTimeout = 5 * time.Second
	defaultModelTimeout     = time.Minute
)

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
	// TerminalStatus is set only on the terminal event passed to a durable
	// EventSink. The sink must persist it in the same transaction as that event.
	TerminalStatus queue.CompletionStatus
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
	RunnerStarted   bool
	RunnerCompleted bool
	// ApprovalPending means the runner stopped at a durable human-review
	// boundary. The consumer must park the execution until the decision is
	// persisted; it must not mark this run successful.
	ApprovalPending bool
	ApprovalID      string
	InputTokens     int
	OutputTokens    int
	TotalTokens     int
	Cost            *float64
	// CleanupError is diagnostic only. Runner/session cleanup happens after the
	// business result has been durably projected and must not turn success into
	// a retryable execution failure.
	CleanupError error
}

type approvalContinuation struct {
	mu      sync.Mutex
	pending bool
	id      string
}

func (s *approvalContinuation) mark(id string) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.pending = true
	if id != "" {
		s.id = id
	}
	s.mu.Unlock()
}

func (s *approvalContinuation) snapshot() (bool, string) {
	if s == nil {
		return false, ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pending, s.id
}

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Config           config.Resolver
	Runner           func(context.Context, Execution) (runner.Runner, error)
	SessionLocker    SessionLocker
	Events           EventSink
	EventSinkTimeout time.Duration
	// ModelTimeout is an operator-owned upper bound for one Runner model
	// invocation path. The context is still drained and Runner.Close is called
	// after expiry.
	ModelTimeout time.Duration
	Cancellation CancellationCheck
	Audit        platformaudit.Sink
	Metrics      *platformmetrics.Recorder
	// Approvals is optional for local/test runtimes. Production workers attach
	// the durable repository; without it review-required tools remain ASK.
	Approvals platformapproval.Repository
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
		return Execution{}, NewPermanentExecutionError(err)
	}
	partitionKey, err := job.PartitionKey()
	if err != nil {
		return Execution{}, NewPermanentExecutionError(err)
	}
	if w.Config == nil {
		return Execution{}, NewPermanentExecutionError(errors.New("config resolver is required"))
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
		return Execution{}, NewPermanentExecutionError(errors.New("resolved app config does not match job scope"))
	}
	requestID := job.RequestID()
	if w.Cancellation != nil {
		canceled, err := w.Cancellation(ctx, tenantContext.TenantID, tenantContext.AppID, requestID)
		if err != nil {
			return Execution{}, fmt.Errorf("check execution cancellation: %w", err)
		}
		if canceled {
			return Execution{}, NewPermanentExecutionError(ErrExecutionCanceled)
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
	approval := &approvalContinuation{}
	defer func() {
		result.ApprovalPending, result.ApprovalID = approval.snapshot()
	}()
	executionStartedAt := time.Now()
	if exec.Config.Audit.Enabled {
		defer func() {
			if !errors.Is(err, tenant.ErrBudgetExceeded) {
				return
			}
			w.recordAudit(ctx, exec, platformaudit.Event{
				Decision:  "rejected",
				ErrorType: "budget_exceeded",
				EventType: platformaudit.BudgetRejected,
			})
			if w.Metrics != nil {
				w.Metrics.RecordGovernanceRejected(ctx, platformmetrics.Labels{
					TenantID: exec.Tenant.TenantID,
					AppID:    exec.Tenant.AppID,
					Channel:  exec.Tenant.Channel,
				}, platformaudit.BudgetRejected)
			}
		}()
	}
	if exec.Config.Audit.Enabled && exec.Config.Audit.RecordExecutions {
		w.recordAudit(ctx, exec, platformaudit.Event{
			Decision:  "started",
			EventType: platformaudit.ExecutionStarted,
		})
		defer func() {
			eventType := platformaudit.ExecutionCompleted
			decision := "completed"
			if err != nil {
				eventType = platformaudit.ExecutionFailed
				decision = "failed"
			}
			w.recordAudit(ctx, exec, platformaudit.Event{
				Decision:     decision,
				EventType:    eventType,
				ErrorType:    executionErrorType(err),
				InputTokens:  result.InputTokens,
				OutputTokens: result.OutputTokens,
				TotalTokens:  result.TotalTokens,
				Cost:         result.Cost,
				Latency:      time.Since(executionStartedAt),
			})
		}()
	}
	if w.Runner == nil {
		return result, NewPermanentExecutionError(errors.New("runner builder is required"))
	}
	message, err := w.runnerMessage(exec)
	if err != nil {
		return result, NewPermanentExecutionError(err)
	}
	appName, err := exec.Tenant.Scope().Key("runner")
	if err != nil {
		return result, NewPermanentExecutionError(err)
	}
	if w.SessionLocker == nil {
		return result, NewPermanentExecutionError(errors.New("session locker is required"))
	}
	lock, err := w.SessionLocker.Lock(ctx, exec.PartitionKey)
	if err != nil {
		return result, fmt.Errorf("lock session partition: %w", err)
	}
	if lock == nil {
		return result, NewPermanentExecutionError(errors.New("session lock is required"))
	}
	defer func() {
		if releaseErr := lock.Release(); releaseErr != nil {
			result.CleanupError = errors.Join(result.CleanupError, fmt.Errorf("release session partition: %w", releaseErr))
		}
	}()
	runCtx := lock.Context()
	if runCtx == nil {
		return result, NewPermanentExecutionError(errors.New("session lock context is required"))
	}
	runCtx = platformtelemetry.Extract(runCtx, map[string]string{
		"traceparent": exec.Tenant.TraceParent,
		"tracestate":  exec.Tenant.TraceState,
	})
	workerCtx, workerSpan := platformtelemetry.StartSpan(runCtx, "worker.execute",
		attribute.String("tenant_id", exec.Tenant.TenantID),
		attribute.String("app_id", exec.Tenant.AppID),
		attribute.String("config_version", exec.Tenant.ConfigVersion),
		attribute.String("request_id", exec.RequestID),
		attribute.String("channel", exec.Tenant.Channel),
	)
	defer workerSpan.End()
	modelAttempted := false
	defer func() {
		if !modelAttempted || w.Metrics == nil {
			return
		}
		result.Cost = w.Metrics.EstimateCost(exec.Config.Model.Provider, exec.Config.Model.Model, result.InputTokens, result.OutputTokens)
	}()
	r, err := w.Runner(workerCtx, exec)
	if err != nil {
		return result, err
	}
	if r == nil {
		return result, NewPermanentExecutionError(errors.New("runner is required"))
	}
	defer func() {
		if closeErr := r.Close(); closeErr != nil {
			result.CleanupError = errors.Join(result.CleanupError, fmt.Errorf("close runner: %w", closeErr))
		}
	}()
	runnerCtx, runnerSpan := platformtelemetry.StartSpan(workerCtx, "runner.run",
		attribute.String("tenant_id", exec.Tenant.TenantID),
		attribute.String("app_id", exec.Tenant.AppID),
		attribute.String("config_version", exec.Tenant.ConfigVersion),
		attribute.String("request_id", exec.RequestID),
	)
	defer runnerSpan.End()
	runnerCtx, cancelModel := context.WithTimeout(runnerCtx, w.modelTimeout())
	defer cancelModel()
	modelAttempted = true
	result.RunnerStarted = true
	// Register cancellation before Run: a managed runner may block while it
	// starts a model request, and that request must still be canceled on the
	// configured deadline.
	stopManagedCancel := cancelManagedRunnerOnContextDone(runnerCtx, r, exec.RequestID)
	defer stopManagedCancel()
	events, err := r.Run(
		runnerCtx,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		message,
		agent.WithRequestID(exec.RequestID),
		agent.WithAppName(appName),
		agent.MergeRuntimeState(runnerRuntimeState(exec)),
		agent.WithToolPermissionPolicy(w.toolPermissionPolicyWithState(exec, approval)),
		// Framework payload tracing is disabled at this boundary because this
		// service owns the safe metadata-only spans above. It prevents raw
		// prompts, tool arguments, and provider errors from entering spans.
		agent.WithDisableTracing(true),
	)
	if err != nil {
		return result, err
	}
	if events == nil {
		return result, NewSideEffectUncertainError(errors.New("runner event channel is nil"))
	}
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
		w.accumulateUsage(&result, evt)
		if w.Events == nil || sinkTimedOut || runnerCtx.Err() != nil {
			continue
		}
		eventExec := exec
		approvalPending, _ := approval.snapshot()
		if !approvalPending {
			switch {
			case evt.IsTerminalError():
				eventExec.TerminalStatus = queue.CompletionFailed
			case evt.IsRunnerCompletion():
				eventExec.TerminalStatus = queue.CompletionSucceeded
			}
		}
		if err := w.handleRunnerEvent(runnerCtx, eventExec, evt); err != nil {
			if sinkErr == nil {
				sinkErr = err
			}
			if errors.Is(err, context.DeadlineExceeded) {
				sinkTimedOut = true
			}
		}
	}
	if err := runnerCtx.Err(); err != nil {
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

func (w Worker) modelTimeout() time.Duration {
	if w.ModelTimeout > 0 {
		return w.ModelTimeout
	}
	return defaultModelTimeout
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
	stop := make(chan struct{})
	finished := make(chan struct{})
	var once sync.Once
	go func() {
		defer close(finished)
		select {
		case <-ctx.Done():
			managed.Cancel(requestID)
		case <-stop:
		}
	}()
	return func() {
		once.Do(func() {
			close(stop)
			<-finished
		})
	}
}

func (w Worker) toolPermissionPolicy(exec Execution) frameworktool.PermissionPolicy {
	return w.toolPermissionPolicyWithState(exec, nil)
}

func (w Worker) toolPermissionPolicyWithState(exec Execution, approval *approvalContinuation) frameworktool.PermissionPolicy {
	return frameworktool.PermissionPolicyFunc(func(
		ctx context.Context,
		request *frameworktool.PermissionRequest,
	) (frameworktool.PermissionDecision, error) {
		started := time.Now()
		name := permissionToolName(request)
		if err := platformtool.AuthorizeExecution(exec.Config.Tools, name); err != nil {
			decision := frameworktool.DenyPermission("tool is not authorized")
			w.recordToolDecision(ctx, exec, name, decision, started)
			return decision, nil
		}
		if exec.Config.Tools.RequiresReview(name) {
			decision, approvalID := w.reviewDecisionWithID(ctx, exec, request, name)
			if decision.Action == frameworktool.PermissionActionAsk {
				approval.mark(approvalID)
			}
			w.recordToolDecision(ctx, exec, name, decision, started)
			return decision, nil
		}
		decision, err := frameworktool.NormalizePermissionDecision(frameworktool.AllowPermission())
		if err != nil {
			return frameworktool.PermissionDecision{}, err
		}
		w.recordToolDecision(ctx, exec, name, decision, started)
		return decision, nil
	})
}

func (w Worker) reviewDecision(
	ctx context.Context,
	exec Execution,
	request *frameworktool.PermissionRequest,
	name string,
) frameworktool.PermissionDecision {
	decision, _ := w.reviewDecisionWithID(ctx, exec, request, name)
	return decision
}

func (w Worker) reviewDecisionWithID(
	ctx context.Context,
	exec Execution,
	request *frameworktool.PermissionRequest,
	name string,
) (frameworktool.PermissionDecision, string) {
	if w.Approvals == nil {
		return frameworktool.AskPermission("human review is required"), ""
	}
	toolCallID := ""
	var arguments []byte
	if request != nil {
		toolCallID = request.ToolCallID
		arguments = request.Arguments
	}
	record, err := w.Approvals.ResolveOrCreate(ctx, platformapproval.Request{
		TenantID:       exec.Tenant.TenantID,
		AppID:          exec.Tenant.AppID,
		ConfigVersion:  exec.Tenant.ConfigVersion,
		RequestID:      exec.RequestID,
		SessionID:      exec.Tenant.SessionID,
		ToolName:       name,
		ToolCallID:     toolCallID,
		ArgumentDigest: platformapproval.DigestArguments(arguments),
		ExpiresAt:      time.Now().UTC().Add(platformapproval.DefaultTTL),
	})
	if err != nil {
		// Fail closed when the durable approval store is unavailable. Returning
		// an ASK here would allow a caller to mistake an unavailable control
		// plane for an approval.
		return frameworktool.DenyPermission("human approval is unavailable"), ""
	}
	switch record.Status {
	case platformapproval.StatusApproved:
		return frameworktool.AllowPermission(), ""
	case platformapproval.StatusDenied:
		return frameworktool.DenyPermission("human approval was denied"), ""
	case platformapproval.StatusExpired:
		return frameworktool.DenyPermission("human approval expired"), ""
	default:
		return frameworktool.AskPermission("human review is required"), record.ApprovalID
	}
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
