// Package tenant models multi-tenant isolation for config, data, tools, and keys.
package tenant

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// Status is the lifecycle state of a tenant.
type Status string

const (
	// StatusActive allows new tenant traffic.
	StatusActive Status = "ACTIVE"
	// StatusSuspended rejects new tenant traffic while preserving stored data.
	StatusSuspended Status = "SUSPENDED"
)

// Tenant describes the top-level isolation boundary for platform data.
type Tenant struct {
	ID     string
	Name   string
	Status Status
	Audit  AuditPolicy
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
	if err := t.Audit.Validate(); err != nil {
		return fmt.Errorf("audit policy: %w", err)
	}
	return nil
}

// Scope returns the tenant and application scope for app-local resources.
func (t Tenant) Scope(appID string) Scope {
	return Scope{TenantID: t.ID, AppID: appID}
}

// AgentApp describes an agent application owned by a tenant.
type AgentApp struct {
	TenantID            string
	AppID               string
	Name                string
	ActiveConfigVersion string
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
	return nil
}

// AppConfig is an immutable version of tenant application configuration.
type AppConfig struct {
	TenantID       string
	AppID          string
	Version        string
	Model          ModelConfig
	Tools          ToolPolicy
	BackendConfig  BackendConfig
	Audit          AuditPolicy
	SecretRefs     []SecretRef
	ChannelBinding []string
}

// Clone returns a deep copy of caller-owned slices and maps in the config.
func (c AppConfig) Clone() AppConfig {
	cloned := c
	cloned.Model = c.Model.Clone()
	cloned.Tools = c.Tools.Clone()
	cloned.BackendConfig = c.BackendConfig.Clone()
	cloned.SecretRefs = cloneSecretRefs(c.SecretRefs)
	cloned.ChannelBinding = cloneStrings(c.ChannelBinding)
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
	if err := c.Audit.Validate(); err != nil {
		return fmt.Errorf("audit policy: %w", err)
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

// ModelConfig identifies the model runtime selected by a tenant application.
type ModelConfig struct {
	Provider   string
	Model      string
	Parameters map[string]string
}

// Clone returns a deep copy of the model config.
func (c ModelConfig) Clone() ModelConfig {
	cloned := c
	cloned.Parameters = cloneStringMap(c.Parameters)
	return cloned
}

// Validate checks model provider and model name fields. Parameters may be nil.
func (c ModelConfig) Validate() error {
	if c.Provider == "" {
		return errors.New("model provider is required")
	}
	if c.Model == "" {
		return errors.New("model is required")
	}
	if err := validateStringMap(c.Parameters, "model parameter"); err != nil {
		return err
	}
	return nil
}

// ToolPolicy declares the tool contract for a tenant application.
type ToolPolicy struct {
	VisibleTools    []string
	ExecutableTools []string
}

// Clone returns a deep copy of the tool policy.
func (p ToolPolicy) Clone() ToolPolicy {
	return ToolPolicy{
		VisibleTools:    cloneStrings(p.VisibleTools),
		ExecutableTools: cloneStrings(p.ExecutableTools),
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
	// BackendObject stores artifacts and knowledge source objects.
	BackendObject BackendKind = "object"
)

// BackendRef references one concrete backend without exposing its secret.
type BackendRef struct {
	Kind    BackendKind
	Name    string
	DSNRef  string
	Options map[string]string
}

// Clone returns a deep copy of the backend reference.
func (r BackendRef) Clone() BackendRef {
	cloned := r
	cloned.Options = cloneStringMap(r.Options)
	return cloned
}

// Validate checks that the backend reference can be resolved later.
// The zero value is invalid when Validate is called directly.
func (r BackendRef) Validate() error {
	if !validBackendKind(r.Kind) {
		return errors.New("backend kind is invalid")
	}
	if r.Name == "" {
		return errors.New("backend name is required")
	}
	if err := validateStringMap(r.Options, "backend option"); err != nil {
		return err
	}
	return nil
}

// BackendConfig groups the backends used by one app config version.
type BackendConfig struct {
	Name      string
	Session   BackendRef
	Memory    BackendRef
	Knowledge BackendRef
	Artifact  BackendRef
	Audit     BackendRef
}

// Clone returns a deep copy of backend references in the config.
func (c BackendConfig) Clone() BackendConfig {
	return BackendConfig{
		Name:      c.Name,
		Session:   c.Session.Clone(),
		Memory:    c.Memory.Clone(),
		Knowledge: c.Knowledge.Clone(),
		Artifact:  c.Artifact.Clone(),
		Audit:     c.Audit.Clone(),
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
	if err := validateOptionalBackendRef("audit backend", c.Audit); err != nil {
		return err
	}
	return nil
}

// AuditPolicy controls tenant audit behavior.
type AuditPolicy struct {
	Enabled       bool
	RetentionDays int
	RedactPII     bool
}

// Validate checks audit retention values. The zero value disables audit.
func (p AuditPolicy) Validate() error {
	if p.RetentionDays < 0 {
		return errors.New("retention_days must be non-negative")
	}
	return nil
}

// SecretRef points to a secret managed outside the database.
type SecretRef struct {
	Name    string
	Version string
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
	TenantID           string
	AppID              string
	ConfigVersion      string
	Channel            string
	BindingID          string
	SessionID          string
	SessionPrincipalID string
	UserID             string
	TraceID            string
}

// Scope returns the tenant and application scope for persistence keys.
func (c RuntimeContext) Scope() Scope {
	return Scope{TenantID: c.TenantID, AppID: c.AppID}
}

// Validate checks the minimum routing fields required for stateless workers.
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
	return nil
}

// Scope identifies the tenant and application prefix used by shared backends.
type Scope struct {
	TenantID string
	AppID    string
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
	case BackendInMemory, BackendSQL, BackendRedis, BackendVector, BackendObject:
		return true
	default:
		return false
	}
}

func validateOptionalBackendRef(label string, ref BackendRef) error {
	if ref.isZero() {
		return nil
	}
	if err := ref.Validate(); err != nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	return nil
}

func (r BackendRef) isZero() bool {
	return r.Kind == "" && r.Name == "" && r.DSNRef == "" && len(r.Options) == 0
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

func cloneStrings(values []string) []string {
	if values == nil {
		return nil
	}
	cloned := make([]string, len(values))
	copy(cloned, values)
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
