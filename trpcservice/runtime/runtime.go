// Package runtime assembles tRPC-Agent-Go Runners from immutable tenant
// application configuration.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"strconv"
	"strings"

	platformartifact "github.com/liuzengh/trpc-agent-service/trpcservice/artifact"
	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	memorytencentdb "github.com/liuzengh/trpc-agent-service/trpcservice/memory/tencentdb"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	platformsession "github.com/liuzengh/trpc-agent-service/trpcservice/session"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	frameworkknowledge "trpc.group/trpc-go/trpc-agent-go/knowledge"
	"trpc.group/trpc-go/trpc-agent-go/model"
	modelopenai "trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	frameworksession "trpc.group/trpc-go/trpc-agent-go/session"
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

// ModelEndpointPolicy resolves a configured OpenAI-compatible endpoint to one
// approved for the scoped model credential. Implementations must enforce the
// operator's hostname and network egress policy.
type ModelEndpointPolicy interface {
	ResolveModelBaseURL(ctx context.Context, exec worker.Execution, configuredURL string) (string, error)
}

// DefaultEndpointPolicy allows operator-configured HTTPS endpoints while
// blocking addresses that enable server-side request forgery.
type DefaultEndpointPolicy struct{}

func (DefaultEndpointPolicy) ResolveModelBaseURL(
	_ context.Context,
	_ worker.Execution,
	configuredURL string,
) (string, error) {
	parsed, err := url.Parse(configuredURL)
	if err != nil {
		return "", fmt.Errorf("parse model base url: %w", err)
	}
	host := parsed.Hostname()
	if parsed.Scheme != "https" || host == "" {
		return "", errors.New("model base url must use https")
	}
	if ip := net.ParseIP(host); ip != nil {
		if blockedEndpointIP(ip) {
			return "", errors.New("model base url resolves to a blocked address")
		}
		return configuredURL, nil
	}
	addresses, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("resolve model base url host: %w", err)
	}
	for _, address := range addresses {
		if blockedEndpointIP(address) {
			return "", errors.New("model base url resolves to a blocked address")
		}
	}
	return configuredURL, nil
}

func blockedEndpointIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

var _ ModelEndpointPolicy = DefaultEndpointPolicy{}

// OpenAIModelResolver resolves OpenAI-compatible models from immutable app
// configuration. It supports the openai provider and obtains the API key only
// through the scoped SecretProvider.
type OpenAIModelResolver struct {
	secrets        platformsecret.SecretProvider
	endpointPolicy ModelEndpointPolicy
}

// NewOpenAIModelResolver creates an OpenAI-compatible model resolver. Without
// a ModelEndpointPolicy, immutable model config cannot override the default
// OpenAI endpoint.
func NewOpenAIModelResolver(
	secrets platformsecret.SecretProvider,
	endpointPolicy ModelEndpointPolicy,
) (*OpenAIModelResolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	return &OpenAIModelResolver{secrets: secrets, endpointPolicy: endpointPolicy}, nil
}

// ResolveModel resolves the OpenAI model selected by exec.Config.
func (r *OpenAIModelResolver) ResolveModel(ctx context.Context, exec worker.Execution) (ModelRuntime, error) {
	if r == nil || r.secrets == nil {
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
	if err := exec.Tenant.Scope().Validate(); err != nil {
		return ModelRuntime{}, fmt.Errorf("execution scope: %w", err)
	}
	ref := exec.Config.Model.APIKeyRef
	if err := ref.Validate(); err != nil {
		return ModelRuntime{}, fmt.Errorf("model api key ref: %w", err)
	}
	apiKey, err := r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), ref)
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

// Runtime constructs one framework runner per execution. Long-
// lived session, memory, artifact, and knowledge services are owned by their
// respective resolvers; the worker owns and closes each returned Runner.
type Runtime struct {
	models    *OpenAIModelResolver
	sessions  *platformsession.Router
	ingestors *memorytencentdb.Resolver
	artifacts *platformartifact.ExecutionResolver
	knowledge *knowledgeqdrant.Resolver
	tools     *ToolCatalog
}

// NewRuntime creates a runner builder that assembles LLMAgent,
// Runner, and Session service instances for prepared executions.
func NewRuntime(
	models *OpenAIModelResolver,
	sessions *platformsession.Router,
	ingestors *memorytencentdb.Resolver,
	artifacts *platformartifact.ExecutionResolver,
	knowledge *knowledgeqdrant.Resolver,
	tools *ToolCatalog,
) (*Runtime, error) {
	if models == nil {
		return nil, errors.New("model resolver is required")
	}
	if sessions == nil {
		return nil, errors.New("session resolver is required")
	}
	runtime := &Runtime{
		models:    models,
		sessions:  sessions,
		ingestors: ingestors,
		artifacts: artifacts,
		knowledge: knowledge,
		tools:     tools,
	}
	return runtime, nil
}

// BuildRunner creates a runner with dependencies selected by exec's
// immutable configuration. The caller owns the returned runner.
func (r *Runtime) BuildRunner(
	ctx context.Context,
	exec worker.Execution,
) (runner.Runner, error) {
	if r == nil || r.models == nil || r.sessions == nil {
		return nil, errors.New("runtime is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	appName, err := exec.Tenant.Scope().Key("runner")
	if err != nil {
		return nil, err
	}

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
	var ingestor frameworksession.Ingestor
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
	resolved := runner.NewRunner(appName, agent, runnerOptions...)
	if resolved == nil {
		return nil, errors.New("runtime runner construction returned nil")
	}
	return resolved, nil
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
