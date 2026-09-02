// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/internal/execution"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/queue"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"go.opentelemetry.io/otel/attribute"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const defaultEventSinkTimeout = 5 * time.Second

const (
	defaultCancellationPollInterval = 500 * time.Millisecond
	cancellationOperationTimeout    = 5 * time.Second
)

// ErrExecutionCanceled means a verified recall stopped the current run.
var ErrExecutionCanceled = errors.New("execution canceled by recall")

// Execution is the prepared context for running one tenant-scoped job.
type Execution struct {
	RequestID    string
	TenantSource gateway.TenantSource
	Tenant       tenant.RuntimeContext
	Config       tenant.AppConfig
	Message      gateway.Message
	PartitionKey string
}

// RunnerResolver resolves the framework runner used for one prepared execution.
// The resolver owns runner construction, caching, and lifecycle.
type RunnerResolver interface {
	ResolveRunner(ctx context.Context, exec Execution) (runner.Runner, error)
	ReleaseRunner(runner.Runner) error
}

// InputArtifactResolver resolves the SQL-guarded artifact service used to
// validate inbound content references for one execution.
type InputArtifactResolver interface {
	ResolveArtifact(context.Context, Execution) (frameworkartifact.Service, error)
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

// ToolPermissionAuthorizer performs the execution-time authorization that
// cannot be decided from the immutable tool policy alone. Implementations may
// inspect trusted execution scope and arguments but must not log raw arguments.
type ToolPermissionAuthorizer interface {
	CheckToolPermission(
		ctx context.Context,
		exec Execution,
		request *frameworktool.PermissionRequest,
	) (frameworktool.PermissionDecision, error)
}

// CancellationController connects verified recall state to the currently
// leased execution. The worker polls the controller and asks ManagedRunner to
// stop; the controller owns the authoritative SQL transition.
type CancellationController interface {
	CancellationRequested(context.Context, string, string, string, queue.Lease) (bool, error)
	CancelExecution(context.Context, string, string, string, queue.Lease) error
}

// LogSink receives only worker-supplied allowlisted routing fields. Logging is
// best effort and does not block execution.
type LogSink interface {
	Log(ctx context.Context, message string, fields map[string]string)
}

// RunResult summarizes one worker-owned runner execution.
type RunResult struct {
	Execution       Execution
	EventCount      int
	RunnerCompleted bool
}

// Option configures a Worker.
type Option func(*Worker)

// WithRunner sets the runner resolver used by Run.
func WithRunner(resolver RunnerResolver) Option {
	return func(w *Worker) {
		w.Runner = resolver
	}
}

// WithInputArtifactResolver sets the resolver used to validate inbound
// artifact references before the runner starts.
func WithInputArtifactResolver(resolver InputArtifactResolver) Option {
	return func(w *Worker) {
		w.Artifacts = resolver
	}
}

// WithSessionLocker sets the authoritative lock used to serialize one session
// partition while its runner executes. Run requires this option.
func WithSessionLocker(locker SessionLocker) Option {
	return func(w *Worker) {
		w.SessionLocker = locker
	}
}

// WithEventSink sets the sink that receives runner events.
func WithEventSink(sink EventSink) Option {
	return func(w *Worker) {
		w.Events = sink
	}
}

// WithEventSinkTimeout sets the maximum time allowed for one event sink call.
func WithEventSinkTimeout(timeout time.Duration) Option {
	return func(w *Worker) {
		w.EventSinkTimeout = timeout
	}
}

// WithToolPermissionAuthorizer sets the execution-time authorization hook.
// The immutable policy remains enforced even when no hook is configured.
func WithToolPermissionAuthorizer(authorizer ToolPermissionAuthorizer) Option {
	return func(w *Worker) {
		w.ToolAuthorizer = authorizer
	}
}

// WithAuditSink sets the authoritative audit writer for enabled app configs.
func WithAuditSink(sink AuditSink) Option {
	return func(w *Worker) {
		w.Audit = sink
	}
}

// WithLogSink sets the best-effort logger used by the worker.
func WithLogSink(sink LogSink) Option {
	return func(w *Worker) {
		w.Logs = sink
	}
}

// WithCancellationController enables recall polling for a leased execution.
func WithCancellationController(controller CancellationController) Option {
	return func(w *Worker) {
		w.Cancellation = controller
	}
}

// WithCancellationPollInterval sets the recall polling interval.
func WithCancellationPollInterval(interval time.Duration) Option {
	return func(w *Worker) {
		if interval > 0 {
			w.CancellationPollInterval = interval
		}
	}
}

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Config                   config.Resolver
	Runner                   RunnerResolver
	Artifacts                InputArtifactResolver
	SessionLocker            SessionLocker
	Events                   EventSink
	EventSinkTimeout         time.Duration
	ToolAuthorizer           ToolPermissionAuthorizer
	Audit                    AuditSink
	Logs                     LogSink
	Cancellation             CancellationController
	CancellationPollInterval time.Duration
}

// New creates a Worker with the required config resolver. Run additionally
// requires a RunnerResolver and SessionLocker.
func New(configResolver config.Resolver, opts ...Option) *Worker {
	w := &Worker{
		Config:                   configResolver,
		CancellationPollInterval: defaultCancellationPollInterval,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
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
	return Execution{
		RequestID:    job.RequestID(),
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
	lease, hasLease := JobLeaseFromContext(ctx)
	exec, err := w.Prepare(ctx, job)
	if err != nil {
		return RunResult{}, err
	}
	result = RunResult{Execution: exec}
	if w.Runner == nil {
		return result, errors.New("runner resolver is required")
	}
	message, err := w.runnerMessage(ctx, exec)
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
	r, err := w.Runner.ResolveRunner(runCtx, exec)
	if err != nil {
		return result, err
	}
	if r == nil {
		return result, errors.New("runner is required")
	}
	defer func() {
		if releaseErr := w.Runner.ReleaseRunner(r); releaseErr != nil {
			w.logExecution(context.WithoutCancel(runCtx), exec, "runner release failed")
		}
	}()
	startedAt := time.Now()
	if err := w.recordAudit(runCtx, exec, AuditEvent{Type: AuditEventExecutionStarted}); err != nil {
		return result, err
	}
	w.logExecution(runCtx, exec, "runner started")
	auditStarted := exec.Config.Audit.Enabled && w.Audit != nil
	if auditStarted {
		defer func() {
			auditEvent := AuditEvent{
				Type:      AuditEventExecutionFailed,
				Latency:   time.Since(startedAt),
				ErrorType: AuditErrorRunnerIncomplete,
			}
			if err == nil && result.RunnerCompleted {
				auditEvent = AuditEvent{Type: AuditEventExecutionCompleted, Latency: time.Since(startedAt)}
			}
			if auditErr := w.recordAudit(context.WithoutCancel(runCtx), exec, auditEvent); auditErr != nil {
				err = errors.Join(err, auditErr)
			}
			w.logExecution(context.WithoutCancel(runCtx), exec, "runner finished")
		}()
	}
	events, err := r.Run(
		runCtx,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		message,
		agent.WithRequestID(exec.RequestID),
		agent.WithAppName(appName),
		agent.MergeRuntimeState(runnerRuntimeState(exec)),
		agent.WithToolPermissionPolicy(w.toolPermissionPolicy(exec)),
		agent.WithSpanAttributes(workerSpanAttributes(exec)...),
	)
	if err != nil {
		return result, err
	}
	if events == nil {
		return result, errors.New("runner event channel is nil")
	}
	stopManagedCancel := cancelManagedRunnerOnContextDone(runCtx, r, exec.RequestID)
	defer stopManagedCancel()
	stopCancellation, checkCancellation, cancellationRequested, cancellationErr :=
		w.watchCancellation(runCtx, r, exec, lease, hasLease)
	defer stopCancellation()
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
	stopCancellation()
	if !cancellationRequested() {
		if err := checkCancellation(); err != nil {
			return result, err
		}
	}
	if err := cancellationErr(); err != nil {
		return result, err
	}
	if cancellationRequested() {
		return result, ErrExecutionCanceled
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

func (w Worker) watchCancellation(
	runCtx context.Context,
	r runner.Runner,
	exec Execution,
	lease queue.Lease,
	hasLease bool,
) (stop func(), check func() error, requested func() bool, watchErr func() error) {
	noop := func() {}
	noopCheck := func() error { return nil }
	falseRequested := func() bool { return false }
	noError := func() error { return nil }
	controller := w.Cancellation
	if controller == nil || !hasLease || runCtx == nil {
		return noop, noopCheck, falseRequested, noError
	}
	managed, _ := r.(runner.ManagedRunner)
	watchCtx, cancelWatch := context.WithCancel(runCtx)
	watchDone := make(chan struct{})
	canceled := make(chan struct{})
	var stopOnce sync.Once
	var cancelOnce sync.Once
	var errMu sync.Mutex
	var observedErr error
	setErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if observedErr == nil {
			observedErr = err
		}
		errMu.Unlock()
	}
	requestCancellation := func() {
		cancelOnce.Do(func() {
			if managed != nil {
				managed.Cancel(exec.RequestID)
			}
			operationCtx, cancel := context.WithTimeout(
				context.WithoutCancel(runCtx), cancellationOperationTimeout,
			)
			err := controller.CancelExecution(
				operationCtx,
				exec.Tenant.TenantID,
				exec.Tenant.AppID,
				exec.RequestID,
				lease,
			)
			cancel()
			if err != nil {
				setErr(err)
				return
			}
			close(canceled)
		})
	}
	checkCancellation := func() error {
		operationCtx, cancel := context.WithTimeout(
			context.WithoutCancel(runCtx), cancellationOperationTimeout,
		)
		cancelRequested, err := controller.CancellationRequested(
			operationCtx,
			exec.Tenant.TenantID,
			exec.Tenant.AppID,
			exec.RequestID,
			lease,
		)
		cancel()
		if err != nil {
			return err
		}
		if cancelRequested {
			requestCancellation()
		}
		return nil
	}
	go func() {
		defer close(watchDone)
		ticker := time.NewTicker(w.cancellationPollInterval())
		defer ticker.Stop()
		for {
			if err := checkCancellation(); err != nil {
				setErr(err)
				if managed != nil {
					managed.Cancel(exec.RequestID)
				}
				return
			}
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	stop = func() {
		stopOnce.Do(func() {
			cancelWatch()
			<-watchDone
		})
	}
	requested = func() bool {
		select {
		case <-canceled:
			return true
		default:
			return false
		}
	}
	watchErr = func() error {
		errMu.Lock()
		defer errMu.Unlock()
		return observedErr
	}
	return stop, checkCancellation, requested, watchErr
}

func (w Worker) cancellationPollInterval() time.Duration {
	if w.CancellationPollInterval > 0 {
		return w.CancellationPollInterval
	}
	return defaultCancellationPollInterval
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
		ctx context.Context,
		request *frameworktool.PermissionRequest,
	) (frameworktool.PermissionDecision, error) {
		name := permissionToolName(request)
		if err := platformtool.AuthorizeExecution(exec.Config.Tools, name); err != nil {
			decision := frameworktool.DenyPermission("tool is not authorized")
			return decision, w.recordAudit(ctx, exec, AuditEvent{
				Type:     AuditEventToolDecision,
				ToolName: name,
				Decision: AuditDecision(decision.Action),
			})
		}
		decision := frameworktool.AllowPermission()
		var err error
		if w.ToolAuthorizer != nil {
			decision, err = w.ToolAuthorizer.CheckToolPermission(ctx, exec, request)
			if err != nil {
				return frameworktool.PermissionDecision{}, fmt.Errorf("authorize tool: %w", err)
			}
		}
		decision, err = frameworktool.NormalizePermissionDecision(decision)
		if err != nil {
			return frameworktool.PermissionDecision{}, fmt.Errorf("normalize tool permission: %w", err)
		}
		if err := w.recordAudit(ctx, exec, AuditEvent{
			Type:     AuditEventToolDecision,
			ToolName: name,
			Decision: AuditDecision(decision.Action),
		}); err != nil {
			return frameworktool.PermissionDecision{}, err
		}
		return decision, nil
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

func (w Worker) recordAudit(ctx context.Context, exec Execution, auditEvent AuditEvent) error {
	if !exec.Config.Audit.Enabled || w.Audit == nil {
		return nil
	}
	if err := auditEvent.Validate(); err != nil {
		return err
	}
	if err := w.Audit.RecordAudit(ctx, exec, auditEvent); err != nil {
		return fmt.Errorf("record audit: %w", err)
	}
	return nil
}

func (w Worker) logExecution(ctx context.Context, exec Execution, message string) {
	if w.Logs == nil {
		return
	}
	fields := platformlog.RoutingFields(exec.Tenant)
	fields["request_id"] = exec.RequestID
	w.Logs.Log(ctx, message, fields)
}

func workerSpanAttributes(exec Execution) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("tenant_id", exec.Tenant.TenantID),
		attribute.String("app_id", exec.Tenant.AppID),
		attribute.String("config_version", exec.Tenant.ConfigVersion),
		attribute.String("request_id", exec.RequestID),
	}
	if exec.Tenant.TraceID != "" {
		attrs = append(attrs, attribute.String("trace_id", exec.Tenant.TraceID))
	}
	if exec.Tenant.Channel != "" {
		attrs = append(attrs, attribute.String("channel", exec.Tenant.Channel))
	}
	if exec.Tenant.BindingID != "" {
		attrs = append(attrs, attribute.String("binding_id", exec.Tenant.BindingID))
	}
	return attrs
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

func (w Worker) runnerMessage(ctx context.Context, exec Execution) (model.Message, error) {
	message := exec.Message
	result := model.NewUserMessage(message.Text)
	if len(message.ArtifactRefs) == 0 {
		return result, nil
	}
	if w.Artifacts == nil {
		return model.Message{}, errors.New("artifact resolver is required for inbound artifacts")
	}
	service, err := w.Artifacts.ResolveArtifact(ctx, exec)
	if err != nil {
		return model.Message{}, fmt.Errorf("resolve inbound artifact service: %w", err)
	}
	if service == nil {
		return model.Message{}, errors.New("inbound artifact service is required")
	}
	for index, ref := range message.ArtifactRefs {
		_, version, err := parsePinnedArtifactRef(ref)
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

func parsePinnedArtifactRef(ref string) (string, int, error) {
	if !strings.HasPrefix(ref, "artifact://") {
		return "", 0, errors.New("artifact ref must use artifact://")
	}
	rest := strings.TrimPrefix(ref, "artifact://")
	separator := strings.LastIndex(rest, "@")
	if separator <= 0 || separator == len(rest)-1 {
		return "", 0, errors.New("artifact ref must pin a version")
	}
	name := rest[:separator]
	if strings.ContainsAny(name, "@\x00\r\n") || strings.Contains(name, "..") {
		return "", 0, errors.New("artifact ref name is invalid")
	}
	version, err := strconv.Atoi(rest[separator+1:])
	if err != nil || version < 0 {
		return "", 0, errors.New("artifact ref version is invalid")
	}
	return name, version, nil
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
