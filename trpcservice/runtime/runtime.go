// Package runtime assembles tRPC-Agent-Go Runners from immutable tenant
// application configuration.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	frameworkagent "trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/model"
	modelopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/session"
	sessionexternalization "trpc.group/trpc-go/trpc-agent-go/session/externalization"
	frameworktool "trpc.group/trpc-go/trpc-agent-go/tool"
)

const (
	runtimeAgentName = "assistant"

	maxTemperature = 2
	maxTopP        = 1
	minPenalty     = -2
	maxPenalty     = 2
)

// ModelRuntime is the model and generation settings selected for one immutable
// application configuration version.
type ModelRuntime struct {
	Model            model.Model
	GenerationConfig model.GenerationConfig
}

// ModelResolver resolves a tenant-scoped model without persisting or logging
// its credentials.
type ModelResolver interface {
	ResolveModel(ctx context.Context, exec worker.Execution) (ModelRuntime, error)
}

// SessionResolver resolves the session service selected for one execution.
// The resolver owns the returned service's lifecycle.
type SessionResolver interface {
	ResolveSession(ctx context.Context, exec worker.Execution) (session.Service, error)
}

// SessionIngestorResolver resolves an optional long-term-memory ingestor for
// one execution. The resolver owns the returned ingestor's lifecycle.
type SessionIngestorResolver interface {
	ResolveSessionIngestor(ctx context.Context, exec worker.Execution) (session.Ingestor, error)
	RequiresPerExecutionRunner(exec worker.Execution) bool
}

// ArtifactResolver resolves an SQL-guarded Artifact service for one
// execution. A configured artifact backend binds the Runner to that trusted
// session and therefore prevents Runner cache reuse.
type ArtifactResolver interface {
	ResolveArtifact(ctx context.Context, exec worker.Execution) (frameworkartifact.Service, error)
}

// KnowledgeResolver resolves an SQL-guarded Knowledge implementation for one
// immutable application configuration version.
type KnowledgeResolver interface {
	ResolveKnowledge(ctx context.Context, exec worker.Execution) (frameworkknowledge.Knowledge, error)
}

// ToolResolver resolves the complete static tool set for one immutable app
// configuration. It receives the full execution scope so it can resolve tools
// and their scoped secret references for that exact config version. Returned
// tools must be safe to share between matching executions.
type ToolResolver interface {
	ResolveTools(ctx context.Context, exec worker.Execution) ([]frameworktool.Tool, error)
}

type runnerFactory func(string, frameworkagent.Agent, ...runner.Option) runner.Runner

// ModelAPIKeyResolver resolves a model API key for one execution. Implementations
// must obtain the key from an external secret store and must not log it.
type ModelAPIKeyResolver interface {
	ResolveModelAPIKey(ctx context.Context, exec worker.Execution) (string, error)
}

// ModelEndpointPolicy resolves a configured OpenAI-compatible endpoint to one
// approved for the scoped model credential. Implementations must enforce the
// operator's hostname and network egress policy.
type ModelEndpointPolicy interface {
	ResolveModelBaseURL(ctx context.Context, exec worker.Execution, configuredURL string) (string, error)
}

// OpenAIModelResolver resolves OpenAI-compatible models from immutable app
// configuration. It supports the openai provider and obtains the API key only
// through ModelAPIKeyResolver.
type OpenAIModelResolver struct {
	apiKeys        ModelAPIKeyResolver
	endpointPolicy ModelEndpointPolicy
}

// NewOpenAIModelResolver creates an OpenAI-compatible model resolver. Without
// a ModelEndpointPolicy, immutable model config cannot override the default
// OpenAI endpoint.
func NewOpenAIModelResolver(
	apiKeys ModelAPIKeyResolver,
	endpointPolicy ModelEndpointPolicy,
) (*OpenAIModelResolver, error) {
	if apiKeys == nil {
		return nil, errors.New("model api key resolver is required")
	}
	return &OpenAIModelResolver{apiKeys: apiKeys, endpointPolicy: endpointPolicy}, nil
}

// ResolveModel resolves the OpenAI model selected by exec.Config.
func (r *OpenAIModelResolver) ResolveModel(ctx context.Context, exec worker.Execution) (ModelRuntime, error) {
	if r == nil || r.apiKeys == nil {
		return ModelRuntime{}, errors.New("openai model resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ModelRuntime{}, err
	}
	if err := tenant.ValidateModelProvider(exec.Config.Model.Provider); err != nil {
		return ModelRuntime{}, err
	}
	if exec.Config.Model.Model == "" {
		return ModelRuntime{}, errors.New("model is required")
	}
	baseURL, generationConfig, err := openAIModelOptions(exec.Config.Model.Parameters)
	if err != nil {
		return ModelRuntime{}, err
	}
	if baseURL != "" {
		if r.endpointPolicy == nil {
			return ModelRuntime{}, errors.New("model parameter base_url requires an endpoint policy")
		}
		baseURL, err = r.endpointPolicy.ResolveModelBaseURL(ctx, exec, baseURL)
		if err != nil {
			return ModelRuntime{}, fmt.Errorf("resolve model base url: %w", err)
		}
		if err := validateModelBaseURL(baseURL); err != nil {
			return ModelRuntime{}, err
		}
	}
	apiKey, err := r.apiKeys.ResolveModelAPIKey(ctx, exec)
	if err != nil {
		return ModelRuntime{}, fmt.Errorf("resolve model api key: %w", err)
	}
	if apiKey == "" {
		return ModelRuntime{}, errors.New("model api key is required")
	}
	opts := []modelopenai.Option{modelopenai.WithAPIKey(apiKey)}
	if baseURL != "" {
		opts = append(opts, modelopenai.WithBaseURL(baseURL))
	}
	return ModelRuntime{
		Model:            modelopenai.New(exec.Config.Model.Model, opts...),
		GenerationConfig: generationConfig,
	}, nil
}

// RuntimeRunnerResolver constructs and caches one real framework runner for
// each immutable tenant application configuration version. Close releases the
// cached runners; the SessionResolver remains responsible for session services.
type RuntimeRunnerResolver struct {
	models    ModelResolver
	sessions  SessionResolver
	ingestors SessionIngestorResolver
	artifacts ArtifactResolver
	knowledge KnowledgeResolver
	tools     ToolResolver

	mu        sync.Mutex
	closed    bool
	runners   map[string]runner.Runner
	ephemeral map[uintptr]runner.Runner
	newRunner runnerFactory
}

// RuntimeRunnerOption configures a RuntimeRunnerResolver.
type RuntimeRunnerOption func(*RuntimeRunnerResolver)

// WithRuntimeToolResolver sets the resolver used to obtain candidate tools
// before the tenant's visible-tool policy is applied.
func WithRuntimeToolResolver(resolver ToolResolver) RuntimeRunnerOption {
	return func(runtime *RuntimeRunnerResolver) {
		runtime.tools = resolver
	}
}

// WithSessionIngestorResolver sets the resolver used to attach optional
// long-term-memory ingestion to a configured runner.
func WithSessionIngestorResolver(resolver SessionIngestorResolver) RuntimeRunnerOption {
	return func(runtime *RuntimeRunnerResolver) {
		runtime.ingestors = resolver
	}
}

// WithArtifactResolver sets the resolver used to attach a session-scoped
// Artifact service when the immutable app configuration selects one.
func WithArtifactResolver(resolver ArtifactResolver) RuntimeRunnerOption {
	return func(runtime *RuntimeRunnerResolver) {
		runtime.artifacts = resolver
	}
}

// WithKnowledgeResolver sets the resolver used to attach configured Knowledge
// to the framework agent.
func WithKnowledgeResolver(resolver KnowledgeResolver) RuntimeRunnerOption {
	return func(runtime *RuntimeRunnerResolver) {
		runtime.knowledge = resolver
	}
}

// NewRuntimeRunnerResolver creates a resolver that assembles LLMAgent, Runner,
// and Session service instances for prepared executions.
func NewRuntimeRunnerResolver(
	models ModelResolver,
	sessions SessionResolver,
	opts ...RuntimeRunnerOption,
) (*RuntimeRunnerResolver, error) {
	if models == nil {
		return nil, errors.New("model resolver is required")
	}
	if sessions == nil {
		return nil, errors.New("session resolver is required")
	}
	resolver := &RuntimeRunnerResolver{
		models:    models,
		sessions:  sessions,
		runners:   make(map[string]runner.Runner),
		ephemeral: make(map[uintptr]runner.Runner),
		newRunner: runner.NewRunner,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(resolver)
		}
	}
	return resolver, nil
}

// ResolveRunner returns the cached runner for exec's immutable app config, or
// creates one with its selected model and session service.
func (r *RuntimeRunnerResolver) ResolveRunner(
	ctx context.Context,
	exec worker.Execution,
) (runner.Runner, error) {
	if r == nil || r.models == nil || r.sessions == nil {
		return nil, errors.New("runtime runner resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	cacheable := !requiresPerExecutionRunner(r.ingestors, exec)
	cacheKey, err := exec.Tenant.Scope().Key("runner", exec.Tenant.ConfigVersion)
	if err != nil {
		return nil, err
	}
	appName, err := exec.Tenant.Scope().Key("runner")
	if err != nil {
		return nil, err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("runtime runner resolver is closed")
	}
	if cacheable && r.runners[cacheKey] != nil {
		cached := r.runners[cacheKey]
		r.mu.Unlock()
		return cached, nil
	}
	r.mu.Unlock()

	// Resolve dependencies without holding the mutex so a slow first build
	// for one tenant does not block runner lookups for all tenants.
	modelRuntime, err := r.models.ResolveModel(ctx, exec)
	if err != nil {
		return nil, err
	}
	if modelRuntime.Model == nil {
		return nil, errors.New("resolved model is required")
	}
	sessionService, err := r.sessions.ResolveSession(ctx, exec)
	if err != nil {
		return nil, err
	}
	if sessionService == nil {
		return nil, errors.New("resolved session service is required")
	}
	var ingestor session.Ingestor
	if r.ingestors != nil {
		ingestor, err = r.ingestors.ResolveSessionIngestor(ctx, exec)
		if err != nil {
			return nil, fmt.Errorf("resolve session ingestor: %w", err)
		}
	}
	var artifactService frameworkartifact.Service
	if !exec.Config.BackendConfig.Artifact.IsZero() {
		if r.artifacts == nil {
			return nil, errors.New("artifact resolver is required for configured artifact backend")
		}
		artifactService, err = r.artifacts.ResolveArtifact(ctx, exec)
		if err != nil {
			return nil, fmt.Errorf("resolve artifact service: %w", err)
		}
		if artifactService == nil {
			return nil, errors.New("configured artifact service is required")
		}
	}
	var knowledgeService frameworkknowledge.Knowledge
	if !exec.Config.BackendConfig.Knowledge.IsZero() {
		if r.knowledge == nil {
			return nil, errors.New("knowledge resolver is required for configured knowledge backend")
		}
		knowledgeService, err = r.knowledge.ResolveKnowledge(ctx, exec)
		if err != nil {
			return nil, fmt.Errorf("resolve knowledge service: %w", err)
		}
		if knowledgeService == nil {
			return nil, errors.New("configured knowledge service is required")
		}
	}
	if artifactService != nil {
		// The model must be wrapped before LLMAgent captures it. The session
		// service must likewise be wrapped before Runner options capture it;
		// otherwise the durable path would still persist inline media bytes.
		modelRuntime.Model = &artifactHydratingModel{
			Model:     modelRuntime.Model,
			artifacts: artifactService,
			info: frameworkartifact.SessionInfo{
				AppName:   appName,
				UserID:    exec.Tenant.SessionPrincipalID,
				SessionID: exec.Tenant.SessionID,
			},
		}
		sessionService = sessionexternalization.Wrap(
			sessionService,
			artifactService,
			sessionexternalization.Config{Enabled: true},
		)
	}
	agentOptions := []llmagent.Option{
		llmagent.WithModel(modelRuntime.Model),
		llmagent.WithGenerationConfig(modelRuntime.GenerationConfig),
	}
	if knowledgeService != nil {
		agentOptions = append(agentOptions, llmagent.WithKnowledge(knowledgeService))
	}
	if r.tools != nil {
		tools, err := r.tools.ResolveTools(ctx, exec)
		if err != nil {
			return nil, fmt.Errorf("resolve tools: %w", err)
		}
		visible, err := visibleTools(exec.Config.Tools, tools)
		if err != nil {
			return nil, err
		}
		agentOptions = append(agentOptions, llmagent.WithTools(visible))
	} else if len(exec.Config.Tools.VisibleTools) > 0 || len(exec.Config.Tools.ExecutableTools) > 0 {
		return nil, errors.New("tool resolver is required for configured tools")
	}
	agent := llmagent.New(runtimeAgentName, agentOptions...)
	runnerOptions := []runner.Option{runner.WithSessionService(sessionService)}
	if ingestor != nil {
		runnerOptions = append(runnerOptions, runner.WithSessionIngestor(ingestor))
	}
	if artifactService != nil {
		runnerOptions = append(runnerOptions, runner.WithArtifactService(artifactService))
	}
	if r.newRunner == nil {
		return nil, errors.New("runtime runner factory is not initialized")
	}
	resolved := r.newRunner(appName, agent, runnerOptions...)
	if resolved == nil {
		return nil, errors.New("runtime runner factory returned nil")
	}
	if !cacheable {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return closeUnpublishedRunner(resolved, errors.New("runtime runner resolver is closed"))
		}
		r.ephemeral[runnerIdentity(resolved)] = resolved
		r.mu.Unlock()
		return resolved, nil
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return closeUnpublishedRunner(resolved, errors.New("runtime runner resolver is closed"))
	}
	// Another caller may have published a runner for the same scope while
	// this one was building; prefer the cached instance.
	if cached := r.runners[cacheKey]; cached != nil {
		r.mu.Unlock()
		if closeErr := resolved.Close(); closeErr != nil {
			return nil, fmt.Errorf("close duplicate runner: %w", closeErr)
		}
		return cached, nil
	}
	r.runners[cacheKey] = resolved
	r.mu.Unlock()
	return resolved, nil
}

func closeUnpublishedRunner(resolved runner.Runner, cause error) (runner.Runner, error) {
	if closeErr := resolved.Close(); closeErr != nil {
		return nil, errors.Join(cause, fmt.Errorf("close unpublished runner: %w", closeErr))
	}
	return nil, cause
}

func requiresPerExecutionRunner(resolver SessionIngestorResolver, exec worker.Execution) bool {
	if !exec.Config.BackendConfig.Artifact.IsZero() {
		return true
	}
	return resolver != nil && resolver.RequiresPerExecutionRunner(exec)
}

// ReleaseRunner closes a Runner created for an uncached execution. Cached
// runners remain owned by RuntimeRunnerResolver.Close.
func (r *RuntimeRunnerResolver) ReleaseRunner(resolved runner.Runner) error {
	if r == nil || resolved == nil {
		return nil
	}
	r.mu.Lock()
	key := runnerIdentity(resolved)
	if key == 0 {
		r.mu.Unlock()
		return nil
	}
	_, ok := r.ephemeral[key]
	if ok {
		delete(r.ephemeral, key)
	}
	r.mu.Unlock()
	if !ok {
		return nil
	}
	return resolved.Close()
}

func visibleTools(policy tenant.ToolPolicy, tools []frameworktool.Tool) ([]frameworktool.Tool, error) {
	visible := make([]frameworktool.Tool, 0, len(tools))
	available := make(map[string]struct{}, len(tools))
	for i, candidate := range tools {
		if candidate == nil || candidate.Declaration() == nil {
			return nil, fmt.Errorf("tool %d declaration is required", i)
		}
		name := candidate.Declaration().Name
		available[name] = struct{}{}
		if err := platformtool.AuthorizeVisibility(policy, name); err != nil {
			if errors.Is(err, platformtool.ErrToolNotVisible) {
				continue
			}
			return nil, fmt.Errorf("tool %d: %w", i, err)
		}
		visible = append(visible, candidate)
	}
	for _, name := range policy.VisibleTools {
		if _, ok := available[name]; !ok {
			return nil, fmt.Errorf("configured visible tool %q is not available", name)
		}
	}
	for _, name := range policy.ExecutableTools {
		if _, ok := available[name]; !ok {
			return nil, fmt.Errorf("configured executable tool %q is not available", name)
		}
	}
	return visible, nil
}

// Close closes all cached runners. It does not close session services because
// their owning SessionResolver can share them across runner cache entries.
func (r *RuntimeRunnerResolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	runners := r.runners
	ephemeral := r.ephemeral
	r.runners = nil
	r.ephemeral = nil
	r.mu.Unlock()

	var errs []error
	for _, resolved := range runners {
		if err := resolved.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	for _, resolved := range ephemeral {
		if err := resolved.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func runnerIdentity(resolved runner.Runner) uintptr {
	if resolved == nil {
		return 0
	}
	value := reflect.ValueOf(resolved)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return 0
	}
	return value.Pointer()
}

func openAIModelOptions(parameters map[string]string) (string, model.GenerationConfig, error) {
	var generationConfig model.GenerationConfig
	var baseURL string
	for key, value := range parameters {
		value = strings.TrimSpace(value)
		switch key {
		case "base_url":
			if value == "" {
				return "", model.GenerationConfig{}, errors.New("model parameter base_url is required")
			}
			baseURL = value
		case "temperature":
			parsed, err := parseFloatParameter(key, value, 0, maxTemperature)
			if err != nil {
				return "", model.GenerationConfig{}, err
			}
			generationConfig.Temperature = &parsed
		case "top_p":
			parsed, err := parseFloatParameter(key, value, 0, maxTopP)
			if err != nil {
				return "", model.GenerationConfig{}, err
			}
			generationConfig.TopP = &parsed
		case "max_tokens":
			parsed, err := parsePositiveIntParameter(key, value)
			if err != nil {
				return "", model.GenerationConfig{}, err
			}
			generationConfig.MaxTokens = &parsed
		case "presence_penalty":
			parsed, err := parseFloatParameter(key, value, minPenalty, maxPenalty)
			if err != nil {
				return "", model.GenerationConfig{}, err
			}
			generationConfig.PresencePenalty = &parsed
		case "frequency_penalty":
			parsed, err := parseFloatParameter(key, value, minPenalty, maxPenalty)
			if err != nil {
				return "", model.GenerationConfig{}, err
			}
			generationConfig.FrequencyPenalty = &parsed
		case "reasoning_effort":
			if value == "" {
				return "", model.GenerationConfig{}, errors.New("model parameter reasoning_effort is required")
			}
			generationConfig.ReasoningEffort = &value
		case "thinking_enabled":
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return "", model.GenerationConfig{}, fmt.Errorf("model parameter thinking_enabled is invalid: %w", err)
			}
			generationConfig.ThinkingEnabled = &parsed
		case "thinking_tokens":
			parsed, err := parsePositiveIntParameter(key, value)
			if err != nil {
				return "", model.GenerationConfig{}, err
			}
			generationConfig.ThinkingTokens = &parsed
		case "thinking_level":
			if value == "" {
				return "", model.GenerationConfig{}, errors.New("model parameter thinking_level is required")
			}
			generationConfig.ThinkingLevel = &value
		default:
			return "", model.GenerationConfig{}, fmt.Errorf("unsupported model parameter %q", key)
		}
	}
	return baseURL, generationConfig, nil
}

func parseFloatParameter(name, value string, min, max float64) (float64, error) {
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed < min || parsed > max {
		return 0, fmt.Errorf("model parameter %s must be between %g and %g", name, min, max)
	}
	return parsed, nil
}

func parsePositiveIntParameter(name, value string) (int, error) {
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("model parameter %s must be positive", name)
	}
	return parsed, nil
}

func validateModelBaseURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.Fragment != "" {
		return errors.New("resolved model base url must be an absolute https url without credentials or fragment")
	}
	return nil
}

var _ ModelResolver = (*OpenAIModelResolver)(nil)
var _ worker.RunnerResolver = (*RuntimeRunnerResolver)(nil)
