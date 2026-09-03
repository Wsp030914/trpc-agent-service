// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// Status is the lifecycle state of a tenant or agent application.
type Status string

const (
	// StatusActive allows new tenant traffic.
	StatusActive Status = "ACTIVE"
	// StatusSuspended rejects new tenant traffic while preserving stored data.
	StatusSuspended Status = "SUSPENDED"
)

// Tenant describes the top-level isolation boundary for platform data.
type Tenant struct {
	ID     string `json:"tenant_id"`
	Name   string `json:"name"`
	Status Status `json:"status"`
}

// Validate checks the persisted tenant configuration. The zero value is invalid.
func (t Tenant) Validate() error {
	if t.ID == "" {
		return errors.New("tenant_id is required")
	}
	if t.Name == "" {
		return errors.New("tenant name is required")
	}
	if !validStatus(t.Status) {
		return errors.New("tenant status is invalid")
	}
	return nil
}

// Scope returns the tenant and application scope for app-local resources.
func (t Tenant) Scope(appID string) Scope {
	return Scope{TenantID: t.ID, AppID: appID}
}

// AgentApp describes an agent application owned by a tenant.
type AgentApp struct {
	TenantID            string `json:"tenant_id"`
	AppID               string `json:"app_id"`
	Name                string `json:"name"`
	ActiveConfigVersion string `json:"active_config_version"`
	Status              Status `json:"status"`
}

// Validate checks the persisted agent application metadata. The zero value is invalid.
func (a AgentApp) Validate() error {
	if a.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if a.AppID == "" {
		return errors.New("app_id is required")
	}
	if a.Name == "" {
		return errors.New("app name is required")
	}
	if a.ActiveConfigVersion == "" {
		return errors.New("active_config_version is required")
	}
	if !validStatus(a.Status) {
		return errors.New("app status is invalid")
	}
	return nil
}

// AppConfig is an immutable version of tenant application configuration.
type AppConfig struct {
	TenantID         string        `json:"tenant_id"`
	AppID            string        `json:"app_id"`
	Version          string        `json:"version"`
	Model            ModelConfig   `json:"model"`
	Tools            ToolPolicy    `json:"tools"`
	BackendConfig    BackendConfig `json:"backend_config"`
	SecretRefs       []SecretRef   `json:"secret_refs"`
	ChannelBinding   []string      `json:"channel_binding"`
	KnowledgeBaseIDs []string      `json:"knowledge_base_ids"`
}

// Clone returns a deep copy of caller-owned slices and maps in the config.
func (c AppConfig) Clone() AppConfig {
	cloned := c
	cloned.Model = c.Model.Clone()
	cloned.Tools = c.Tools.Clone()
	cloned.BackendConfig = c.BackendConfig.Clone()
	cloned.SecretRefs = cloneSecretRefs(c.SecretRefs)
	cloned.ChannelBinding = slices.Clone(c.ChannelBinding)
	cloned.KnowledgeBaseIDs = slices.Clone(c.KnowledgeBaseIDs)
	return cloned
}

// Validate checks one immutable tenant application config version.
//
// TenantID, AppID, Version, Model, and the session backend are required. Nil
// slices and maps are valid and mean no optional values are configured.
func (c AppConfig) Validate() error {
	if c.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if c.AppID == "" {
		return errors.New("app_id is required")
	}
	if c.Version == "" {
		return errors.New("config version is required")
	}
	if err := c.Model.Validate(); err != nil {
		return fmt.Errorf("model config: %w", err)
	}
	if err := c.Tools.Validate(); err != nil {
		return fmt.Errorf("tool policy: %w", err)
	}
	if err := c.BackendConfig.Validate(); err != nil {
		return fmt.Errorf("backend_config: %w", err)
	}
	if err := validateUniqueStrings(c.KnowledgeBaseIDs, "knowledge base"); err != nil {
		return err
	}
	for i, ref := range c.SecretRefs {
		if err := ref.Validate(); err != nil {
			return fmt.Errorf("secret_ref %d: %w", i, err)
		}
	}
	for i, bindingID := range c.ChannelBinding {
		if bindingID == "" {
			return fmt.Errorf("channel_binding %d is required", i)
		}
	}
	return nil
}

// ModelConfig identifies the model runtime and scoped API key selected by a
// tenant application.
type ModelConfig struct {
	Provider string `json:"provider"`
	Model    string `json:"model"`
	// APIKeyRef is the external secret selected for this model. It is required
	// because the production worker resolves the model credential only through
	// a scoped secret reference.
	APIKeyRef  SecretRef         `json:"api_key_ref"`
	Parameters map[string]string `json:"parameters"`
}

// ModelProviderOpenAI is the only model provider constructed by the
// production runtime.
const ModelProviderOpenAI = "openai"

// ValidateModelProvider checks that a configured provider is executable by
// the deployed runtime.
func ValidateModelProvider(provider string) error {
	switch strings.TrimSpace(provider) {
	case ModelProviderOpenAI:
		return nil
	case "":
		return errors.New("model provider is required")
	default:
		return fmt.Errorf("unsupported model provider %q", provider)
	}
}

// Clone returns a deep copy of the model config.
func (c ModelConfig) Clone() ModelConfig {
	cloned := c
	cloned.Parameters = cloneStringMap(c.Parameters)
	return cloned
}

// Validate checks model provider and model name fields. Parameters may be nil.
// The API key reference is required.
func (c ModelConfig) Validate() error {
	if err := ValidateModelProvider(c.Provider); err != nil {
		return err
	}
	if c.Model == "" {
		return errors.New("model is required")
	}
	if err := c.APIKeyRef.Validate(); err != nil {
		return fmt.Errorf("api key ref: %w", err)
	}
	if err := validateStringMap(c.Parameters, "model parameter"); err != nil {
		return err
	}
	return nil
}

// ToolPolicy declares the tool contract for a tenant application.
type ToolPolicy struct {
	VisibleTools    []string `json:"visible_tools"`
	ExecutableTools []string `json:"executable_tools"`
}

// Clone returns a deep copy of the tool policy.
func (p ToolPolicy) Clone() ToolPolicy {
	return ToolPolicy{
		VisibleTools:    slices.Clone(p.VisibleTools),
		ExecutableTools: slices.Clone(p.ExecutableTools),
	}
}

// Validate checks that configured tool names are non-empty and unique.
// Nil tool slices are valid and mean no tools are configured.
func (p ToolPolicy) Validate() error {
	if err := validateUniqueStrings(p.VisibleTools, "visible tool"); err != nil {
		return err
	}
	if err := validateUniqueStrings(p.ExecutableTools, "executable tool"); err != nil {
		return err
	}
	return nil
}

// CanView reports whether a tool declaration may be exposed to the tenant app.
func (p ToolPolicy) CanView(name string) bool {
	return containsString(p.VisibleTools, name)
}

// CanExecute reports whether the tenant app may execute a tool.
func (p ToolPolicy) CanExecute(name string) bool {
	return containsString(p.ExecutableTools, name)
}

// BackendKind identifies a storage backend family.
type BackendKind string

const (
	// BackendInMemory is only suitable for tests and local development.
	BackendInMemory BackendKind = "inmemory"
	// BackendSQL stores strongly consistent relational state.
	BackendSQL BackendKind = "sql"
	// BackendRedis stores cache, queue, or Redis-backed session state.
	BackendRedis BackendKind = "redis"
	// BackendVector stores derived retrieval indexes.
	BackendVector BackendKind = "vector"
	// BackendObject stores artifacts and knowledge objects.
	BackendObject BackendKind = "object"
	// BackendExternal stores data through an external managed service.
	BackendExternal BackendKind = "external"
)

// BackendRef references one concrete backend without exposing its secret.
type BackendRef struct {
	Kind     BackendKind `json:"kind"`
	Provider string      `json:"provider"`
	Name     string      `json:"name"`
	// SecretRef identifies credentials owned by the tenant application scope.
	SecretRef SecretRef         `json:"secret_ref,omitempty"`
	Options   map[string]string `json:"options"`
}

// Clone returns a deep copy of the backend reference.
func (r BackendRef) Clone() BackendRef {
	cloned := r
	cloned.Options = cloneStringMap(r.Options)
	return cloned
}

// IsZero reports whether the backend reference is not configured.
func (r BackendRef) IsZero() bool {
	return r.Kind == "" && r.Provider == "" && r.Name == "" &&
		r.SecretRef == (SecretRef{}) && len(r.Options) == 0
}

// Validate checks that the backend reference can be resolved later.
// The zero value is invalid when Validate is called directly.
func (r BackendRef) Validate() error {
	if !validBackendKind(r.Kind) {
		return errors.New("backend kind is invalid")
	}
	if r.Provider == "" {
		return errors.New("backend provider is required")
	}
	if r.Name == "" {
		return errors.New("backend name is required")
	}
	if r.SecretRef != (SecretRef{}) {
		if err := r.SecretRef.Validate(); err != nil {
			return fmt.Errorf("backend secret_ref: %w", err)
		}
	}
	if err := validateStringMap(r.Options, "backend option"); err != nil {
		return err
	}
	return nil
}

// BackendConfig groups the backends used by one app config version.
type BackendConfig struct {
	Name      string     `json:"name"`
	Session   BackendRef `json:"session"`
	Memory    BackendRef `json:"memory"`
	Knowledge BackendRef `json:"knowledge"`
	Artifact  BackendRef `json:"artifact"`
}

// Clone returns a deep copy of backend references in the config.
func (c BackendConfig) Clone() BackendConfig {
	return BackendConfig{
		Name:      c.Name,
		Session:   c.Session.Clone(),
		Memory:    c.Memory.Clone(),
		Knowledge: c.Knowledge.Clone(),
		Artifact:  c.Artifact.Clone(),
	}
}

// Validate checks the required session backend and any optional backend refs.
// The zero value is invalid because a stateless worker needs a session backend.
func (c BackendConfig) Validate() error {
	if c.Name == "" {
		return errors.New("backend_config name is required")
	}
	if err := c.Session.Validate(); err != nil {
		return fmt.Errorf("session backend: %w", err)
	}
	if err := validateOptionalBackendRef("memory backend", c.Memory); err != nil {
		return err
	}
	if err := validateOptionalBackendRef("knowledge backend", c.Knowledge); err != nil {
		return err
	}
	if err := validateOptionalBackendRef("artifact backend", c.Artifact); err != nil {
		return err
	}
	return nil
}

// SecretRef points to a secret managed outside the database.
type SecretRef struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// Validate checks that the secret reference has a stable name.
// The zero value is invalid when present in a config.
func (r SecretRef) Validate() error {
	if r.Name == "" {
		return errors.New("secret name is required")
	}
	return nil
}

// RuntimeContext carries trusted tenant routing metadata through one request.
type RuntimeContext struct {
	TenantID      string `json:"tenant_id"`
	AppID         string `json:"app_id"`
	ConfigVersion string `json:"config_version"`
	Channel       string `json:"channel"`
	BindingID     string `json:"binding_id"`
	// BindingRevision identifies the authorization snapshot for a channel
	// execution. It is zero for non-channel contexts.
	BindingRevision int64  `json:"binding_revision,omitempty"`
	SessionID       string `json:"session_id"`
	// SessionPrincipalID identifies the owner of the conversation session. It
	// equals UserID for a private conversation and identifies the group or
	// thread for a shared conversation.
	SessionPrincipalID string `json:"session_principal_id"`
	// UserID identifies the user who sent the current message.
	UserID string `json:"user_id"`
	// TraceID identifies the end-to-end trace for this request.
	TraceID string `json:"trace_id"`
}

// Scope returns the tenant and application scope for persistence keys.
func (c RuntimeContext) Scope() Scope {
	return Scope{TenantID: c.TenantID, AppID: c.AppID}
}

// Validate checks the routing, sender, and tracing fields required for stateless workers.
func (c RuntimeContext) Validate() error {
	if c.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if c.AppID == "" {
		return errors.New("app_id is required")
	}
	if c.ConfigVersion == "" {
		return errors.New("config_version is required")
	}
	if c.SessionID == "" {
		return errors.New("session_id is required")
	}
	if c.SessionPrincipalID == "" {
		return errors.New("session_principal_id is required")
	}
	if c.UserID == "" {
		return errors.New("user_id is required")
	}
	if c.TraceID == "" {
		return errors.New("trace_id is required")
	}
	return nil
}

// Scope identifies the tenant and application prefix used by shared backends.
type Scope struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
}

// NewScope creates a validated tenant application scope.
func NewScope(tenantID, appID string) (Scope, error) {
	s := Scope{TenantID: tenantID, AppID: appID}
	if err := s.Validate(); err != nil {
		return Scope{}, err
	}
	return s, nil
}

// Validate checks that the scope can safely prefix shared backend keys.
func (s Scope) Validate() error {
	if s.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if s.AppID == "" {
		return errors.New("app_id is required")
	}
	return nil
}

// Key builds a tenant-scoped key for persistence, cache, object, or vector use.
func (s Scope) Key(namespace string, parts ...string) (string, error) {
	if err := s.Validate(); err != nil {
		return "", err
	}
	if namespace == "" {
		return "", errors.New("namespace is required")
	}
	segments := []string{
		"tenant", s.TenantID,
		"app", s.AppID,
		namespace,
	}
	segments = append(segments, parts...)
	for i, segment := range segments {
		if segment == "" {
			return "", fmt.Errorf("key segment %d is required", i)
		}
		segments[i] = escapeKeySegment(segment)
	}
	return strings.Join(segments, ":"), nil
}

func escapeKeySegment(segment string) string {
	return strings.ReplaceAll(url.PathEscape(segment), ":", "%3A")
}

func validStatus(status Status) bool {
	return status == StatusActive || status == StatusSuspended
}

func validBackendKind(kind BackendKind) bool {
	switch kind {
	case BackendInMemory, BackendSQL, BackendRedis, BackendVector, BackendObject, BackendExternal:
		return true
	default:
		return false
	}
}

func validateOptionalBackendRef(label string, ref BackendRef) error {
	if ref.IsZero() {
		return nil
	}
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

func validateUniqueStrings(values []string, label string) error {
	seen := make(map[string]struct{}, len(values))
	for i, value := range values {
		if value == "" {
			return fmt.Errorf("%s %d is required", label, i)
		}
		if _, ok := seen[value]; ok {
			return fmt.Errorf("%s %q is duplicated", label, value)
		}
		seen[value] = struct{}{}
	}
	return nil
}

func validateStringMap(values map[string]string, label string) error {
	for key := range values {
		if key == "" {
			return fmt.Errorf("%s key is required", label)
		}
	}
	return nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for k, v := range values {
		cloned[k] = v
	}
	return cloned
}

func cloneSecretRefs(values []SecretRef) []SecretRef {
	if values == nil {
		return nil
	}
	cloned := make([]SecretRef, len(values))
	copy(cloned, values)
	return cloned
}

func containsString(values []string, target string) bool {
	if target == "" {
		return false
	}
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
