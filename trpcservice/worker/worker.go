// Package worker defines the stateless worker boundary for tenant jobs.
package worker

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

const defaultEventSinkTimeout = 5 * time.Second

// Execution is the prepared context for running one tenant-scoped job.
type Execution struct {
	RequestID    string
	TenantSource gateway.TenantSource
	Tenant       tenant.RuntimeContext
	Config       tenant.AppConfig
	Storage      storage.Handles
	Message      gateway.Message
	PartitionKey string
}

// RunnerResolver resolves the framework runner used for one prepared execution.
// The resolver owns runner construction, caching, and lifecycle.
type RunnerResolver interface {
	ResolveRunner(ctx context.Context, exec Execution) (runner.Runner, error)
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

// Option configures a Worker.
type Option func(*Worker)

// WithRunner sets the runner resolver used by Run.
func WithRunner(resolver RunnerResolver) Option {
	return func(w *Worker) {
		w.Runner = resolver
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

// Worker prepares jobs for execution without owning session state locally.
type Worker struct {
	Config           config.Resolver
	Storage          storage.Resolver
	Runner           RunnerResolver
	Events           EventSink
	EventSinkTimeout time.Duration
}

// New creates a Worker with the required config and storage resolvers.
func New(configResolver config.Resolver, storageResolver storage.Resolver, opts ...Option) *Worker {
	w := &Worker{
		Config:  configResolver,
		Storage: storageResolver,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Prepare validates a job and resolves the tenant backend_config.
func (w Worker) Prepare(ctx context.Context, job gateway.Job) (Execution, error) {
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
	if w.Storage == nil {
		return Execution{}, errors.New("storage resolver is required")
	}
	cfg, err := w.Config.ResolveAppConfig(
		ctx,
		job.Tenant.TenantID,
		job.Tenant.AppID,
		job.Tenant.ConfigVersion,
	)
	if err != nil {
		return Execution{}, err
	}
	if err := cfg.Validate(); err != nil {
		return Execution{}, fmt.Errorf("app config: %w", err)
	}
	if cfg.TenantID != job.Tenant.TenantID ||
		cfg.AppID != job.Tenant.AppID ||
		cfg.Version != job.Tenant.ConfigVersion {
		return Execution{}, errors.New("resolved app config does not match job scope")
	}
	stores, err := w.Storage.Resolve(ctx, job.Tenant, cfg.BackendConfig)
	if err != nil {
		return Execution{}, err
	}
	if err := stores.Validate(job.Tenant, cfg.BackendConfig); err != nil {
		return Execution{}, err
	}
	return Execution{
		RequestID:    job.RequestID,
		TenantSource: job.TenantSource,
		Tenant:       job.Tenant,
		Config:       cfg,
		Storage:      stores,
		Message: gateway.Message{
			Text:         job.Message.Text,
			ArtifactRefs: slices.Clone(job.Message.ArtifactRefs),
		},
		PartitionKey: partitionKey,
	}, nil
}

// Run prepares a job, calls runner.Runner, and drains the returned event channel.
// After draining, it returns the first sink error or terminal runner error.
// It uses SessionPrincipalID as the runner user ID so group and thread messages
// share a session, while UserID remains available in runtime state as the sender.
func (w Worker) Run(ctx context.Context, job gateway.Job) (RunResult, error) {
	exec, err := w.Prepare(ctx, job)
	if err != nil {
		return RunResult{}, err
	}
	result := RunResult{Execution: exec}
	if w.Runner == nil {
		return result, errors.New("runner resolver is required")
	}
	if err := validateRunExecution(exec); err != nil {
		return result, err
	}
	r, err := w.Runner.ResolveRunner(ctx, exec)
	if err != nil {
		return result, err
	}
	if r == nil {
		return result, errors.New("runner is required")
	}
	message, err := runnerMessage(exec.Message)
	if err != nil {
		return result, err
	}
	appName, err := runnerAppName(exec.Tenant)
	if err != nil {
		return result, err
	}
	events, err := r.Run(
		ctx,
		exec.Tenant.SessionPrincipalID,
		exec.Tenant.SessionID,
		message,
		agent.WithRequestID(exec.RequestID),
		agent.WithAppName(appName),
		agent.MergeRuntimeState(runnerRuntimeState(exec)),
	)
	if err != nil {
		return result, err
	}
	if events == nil {
		return result, errors.New("runner event channel is nil")
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
		if w.Events == nil || sinkTimedOut {
			continue
		}
		if err := w.handleRunnerEvent(ctx, exec, evt); err != nil {
			if sinkErr == nil {
				sinkErr = err
			}
			if errors.Is(err, context.DeadlineExceeded) {
				sinkTimedOut = true
			}
		}
	}
	if sinkErr != nil {
		return result, sinkErr
	}
	if runnerErr != nil {
		return result, runnerErr
	}
	return result, nil
}

func validateRunExecution(exec Execution) error {
	if exec.Tenant.UserID == "" {
		return errors.New("user_id is required for runner")
	}
	return nil
}

func (w Worker) handleRunnerEvent(ctx context.Context, exec Execution, evt *event.Event) error {
	sinkCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.eventSinkTimeout())
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

func runnerMessage(message gateway.Message) (model.Message, error) {
	if len(message.ArtifactRefs) > 0 {
		return model.Message{}, errors.New("artifact refs are not supported by runner boundary")
	}
	if message.Text == "" {
		return model.Message{}, errors.New("message text is required")
	}
	return model.NewUserMessage(message.Text), nil
}

func runnerAppName(tc tenant.RuntimeContext) (string, error) {
	return tc.Scope().Key("runner")
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
