package admin_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const testAdminToken = "test-admin-token"

func TestHTTPHandlerRequiresIndependentAdminToken(t *testing.T) {
	handler := newAdminHandler(t, &recordingRepository{})
	request := httptest.NewRequest(http.MethodPost, "/admin/v1/tenants", strings.NewReader(`{"tenant":{}}`))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusUnauthorized)
	}
}

func TestHTTPHandlerCreatesControlPlaneScopeAndOneTimeCredential(t *testing.T) {
	repository := &recordingRepository{}
	handler := newAdminHandler(t, repository)
	tenantValue := tenant.Tenant{ID: "tenant-a", Name: "Tenant A", Status: tenant.StatusActive}
	postAdminJSON(t, handler, "/admin/v1/tenants", struct {
		Tenant tenant.Tenant `json:"tenant"`
	}{Tenant: tenantValue}, http.StatusCreated, nil)

	initial := httpTestAppConfig()
	app := tenant.AgentApp{
		TenantID:            initial.TenantID,
		AppID:               initial.AppID,
		Name:                "Support",
		ActiveConfigVersion: initial.Version,
		Status:              tenant.StatusActive,
	}
	postAdminJSON(t, handler, "/admin/v1/apps", struct {
		App           tenant.AgentApp  `json:"app"`
		InitialConfig tenant.AppConfig `json:"initial_config"`
	}{App: app, InitialConfig: initial}, http.StatusCreated, nil)
	binding := httpTestBinding()
	binding.PublicRouteID = ""
	binding.BindingRevision = 0
	postAdminJSON(t, handler, "/admin/v1/channel-bindings", struct {
		Binding channels.Binding `json:"binding"`
	}{Binding: binding}, http.StatusCreated, nil)
	published := initial.Clone()
	published.Version = "v2"
	published.ChannelBinding = []string{binding.BindingID}
	postAdminJSON(t, handler, "/admin/v1/configs", struct {
		Config tenant.AppConfig `json:"config"`
	}{Config: published}, http.StatusCreated, nil)
	postAdminJSON(t, handler, "/admin/v1/configs/activate", map[string]string{
		"tenant_id": initial.TenantID,
		"app_id":    initial.AppID,
		"version":   published.Version,
	}, http.StatusNoContent, nil)

	var issued struct {
		Credential struct {
			ID string `json:"id"`
		} `json:"credential"`
		APIKey string `json:"api_key"`
	}
	postAdminJSON(t, handler, "/admin/v1/credentials", map[string]string{
		"tenant_id": initial.TenantID,
		"app_id":    initial.AppID,
	}, http.StatusCreated, &issued)

	if issued.APIKey == "" || !strings.HasPrefix(issued.APIKey, "tas_") || issued.Credential.ID == "" {
		t.Fatalf("issued credential = %#v", issued)
	}
	digest, err := auth.DigestAPIKey(issued.APIKey)
	if err != nil {
		t.Fatalf("digest issued key: %v", err)
	}
	if repository.digest != digest || repository.credential.ID != issued.Credential.ID {
		t.Fatalf("persisted credential = %#v", repository.credential)
	}
	postAdminJSON(t, handler, "/admin/v1/credentials/revoke", map[string]string{
		"tenant_id":     initial.TenantID,
		"app_id":        initial.AppID,
		"credential_id": issued.Credential.ID,
	}, http.StatusNoContent, nil)
	if repository.revokedCredentialID != issued.Credential.ID {
		t.Fatalf("revoked credential = %q", repository.revokedCredentialID)
	}
}

func TestHTTPHandlerRejectsUnknownFieldsBeforeRepositoryMutation(t *testing.T) {
	repository := &recordingRepository{}
	handler := newAdminHandler(t, repository)
	request := httptest.NewRequest(
		http.MethodPost,
		"/admin/v1/tenants",
		strings.NewReader(`{"tenant":{"id":"tenant-a"},"unexpected":true}`),
	)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if repository.tenant.ID != "" {
		t.Fatalf("repository mutated: %#v", repository.tenant)
	}
}

func TestHTTPHandlerListsAuditEventsByExactTenantAndAppScope(t *testing.T) {
	repository := &recordingRepository{auditEvents: []platformaudit.Event{
		{TenantID: "tenant-a", AppID: "support", EventType: platformaudit.ExecutionStarted},
		{TenantID: "tenant-a", AppID: "billing", EventType: platformaudit.ExecutionFailed},
		{TenantID: "tenant-b", AppID: "support", EventType: platformaudit.ExecutionCompleted},
	}}
	handler := newAdminHandler(t, repository)
	request := httptest.NewRequest(http.MethodGet, "/admin/v1/audit-events?tenant_id=tenant-a&app_id=support&limit=25", nil)
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	var body struct {
		Events []platformaudit.Event `json:"events"`
	}
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode audit response: %v", err)
	}
	if len(body.Events) != 1 || body.Events[0].TenantID != "tenant-a" || body.Events[0].AppID != "support" {
		t.Fatalf("audit events = %#v", body.Events)
	}
	if repository.auditTenantID != "tenant-a" || repository.auditAppID != "support" || repository.auditLimit != 25 {
		t.Fatalf("audit scope = %q/%q limit=%d", repository.auditTenantID, repository.auditAppID, repository.auditLimit)
	}
}

func newAdminHandler(t *testing.T, repository *recordingRepository) http.Handler {
	t.Helper()
	handler, err := admin.NewHTTPHandler(admin.API{Bindings: repository, Repository: repository}, testAdminToken)
	if err != nil {
		t.Fatalf("new admin handler: %v", err)
	}
	return handler
}

func httpTestBinding() channels.Binding {
	return channels.Binding{
		TenantID:         "tenant-a",
		AppID:            "support",
		BindingID:        "wecom-support",
		Channel:          channels.ChannelWeCom,
		ExternalAccount:  "corp-agent-support",
		Secret:           tenant.SecretRef{Name: "wecom-bot-secret", Version: "v1"},
		PublicRouteID:    "route-wecom-support",
		BindingRevision:  1,
		Status:           channels.BindingActive,
	}
}

func postAdminJSON(t *testing.T, handler http.Handler, path string, body any, wantStatus int, target any) {
	t.Helper()
	encoded, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(encoded))
	request.Header.Set("Authorization", "Bearer "+testAdminToken)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != wantStatus {
		t.Fatalf("POST %s status = %d, want %d: %s", path, response.Code, wantStatus, response.Body.String())
	}
	if target != nil {
		if err := json.NewDecoder(response.Body).Decode(target); err != nil {
			t.Fatalf("decode response: %v", err)
		}
	}
}

func httpTestAppConfig() tenant.AppConfig {
	return tenant.AppConfig{
		TenantID: "tenant-a",
		AppID:    "support",
		Version:  "v1",
		Model: tenant.ModelConfig{
			Provider:  "openai",
			APIKeyRef: tenant.SecretRef{Name: "model-key"},
			Model:     "gpt-4.1-mini",
		},
		BackendConfig: tenant.BackendConfig{
			Name: "shared",
			Session: tenant.BackendRef{
				Kind:     tenant.BackendSQL,
				Provider: "postgres",
				Name:     "session-sql",
			},
		},
	}
}
