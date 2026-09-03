package admin

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	platformaudit "github.com/liuzengh/trpc-agent-service/trpcservice/audit"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

const maxAdminRequestBytes = 1 << 20

// NewHTTPHandler creates the token-protected control-plane endpoints for
// creating tenants, applications with their initial configuration, and API
// credentials. The token is an independent administrator secret and is never
// accepted as a data-plane API credential.
func NewHTTPHandler(api API, token string) (http.Handler, error) {
	if api.Repository == nil {
		return nil, errors.New("admin repository is required")
	}
	if strings.TrimSpace(token) == "" {
		return nil, errors.New("admin token is required")
	}

	handler := adminHTTPHandler{api: api, token: strings.TrimSpace(token)}
	mux := http.NewServeMux()
	mux.HandleFunc("/admin/v1/tenants", methodHandler(http.MethodPost, handler.createTenant))
	mux.HandleFunc("/admin/v1/apps", methodHandler(http.MethodPost, handler.createAgentApp))
	mux.HandleFunc("/admin/v1/channel-bindings", methodHandler(http.MethodPost, handler.createChannelBinding))
	mux.HandleFunc("/admin/v1/configs", methodHandler(http.MethodPost, handler.publishAppConfig))
	mux.HandleFunc("/admin/v1/configs/activate", methodHandler(http.MethodPost, handler.activateAppConfig))
	mux.HandleFunc("/admin/v1/data-migrations", methodHandler(http.MethodPost, handler.createDataMigration))
	mux.HandleFunc("/admin/v1/data-migrations/begin", methodHandler(http.MethodPost, handler.beginDataMigration))
	mux.HandleFunc("/admin/v1/credentials", methodHandler(http.MethodPost, handler.issueCredential))
	mux.HandleFunc("/admin/v1/credentials/revoke", methodHandler(http.MethodPost, handler.revokeCredential))
	mux.HandleFunc("/admin/v1/audit-events", methodHandler(http.MethodGet, handler.listAuditEvents))
	return handler.authorize(mux), nil
}

func methodHandler(method string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		next(w, r)
	}
}

type adminHTTPHandler struct {
	api   API
	token string
}

type createTenantRequest struct {
	Tenant tenant.Tenant `json:"tenant"`
}

type createAgentAppRequest struct {
	App           tenant.AgentApp  `json:"app"`
	InitialConfig tenant.AppConfig `json:"initial_config"`
}

type createChannelBindingRequest struct {
	Binding channels.Binding `json:"binding"`
}

type issueCredentialRequest struct {
	TenantID  string     `json:"tenant_id"`
	AppID     string     `json:"app_id"`
	ExpiresAt *time.Time `json:"expires_at"`
}

type publishAppConfigRequest struct {
	Config tenant.AppConfig `json:"config"`
}

type activateAppConfigRequest struct {
	TenantID string `json:"tenant_id"`
	AppID    string `json:"app_id"`
	Version  string `json:"version"`
}

type revokeCredentialRequest struct {
	TenantID     string `json:"tenant_id"`
	AppID        string `json:"app_id"`
	CredentialID string `json:"credential_id"`
}

type createDataMigrationRequest struct {
	TenantID      string `json:"tenant_id"`
	AppID         string `json:"app_id"`
	SourceVersion string `json:"source_config_version"`
	TargetVersion string `json:"target_config_version"`
}

type beginDataMigrationRequest struct {
	TenantID      string    `json:"tenant_id"`
	AppID         string    `json:"app_id"`
	MigrationID   string    `json:"migration_id"`
	Owner         string    `json:"owner"`
	DrainDeadline time.Time `json:"drain_deadline"`
	LeaseDuration string    `json:"lease_duration"`
}

type issueCredentialResponse struct {
	Credential credentialResponse `json:"credential"`
	APIKey     string             `json:"api_key"`
}

type credentialResponse struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	AppID     string     `json:"app_id"`
	KeyPrefix string     `json:"key_prefix"`
	Status    string     `json:"status"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

type auditEventsResponse struct {
	Events []platformaudit.Event `json:"events"`
}

func (h adminHTTPHandler) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.validAuthorization(r) {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeJSONError(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (h adminHTTPHandler) validAuthorization(r *http.Request) bool {
	if r == nil {
		return false
	}
	values := r.Header.Values("Authorization")
	if len(values) != 1 {
		return false
	}
	parts := strings.Fields(values[0])
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || parts[1] == "" {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(parts[1]), []byte(h.token)) == 1
}

func (h adminHTTPHandler) createTenant(w http.ResponseWriter, r *http.Request) {
	var request createTenantRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid tenant")
		return
	}
	if err := h.api.CreateTenant(r.Context(), request.Tenant); err != nil {
		writeAdminOperationError(w, err, "create tenant failed")
		return
	}
	writeJSON(w, http.StatusCreated, request.Tenant)
}

func (h adminHTTPHandler) createAgentApp(w http.ResponseWriter, r *http.Request) {
	var request createAgentAppRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid agent app")
		return
	}
	if err := h.api.CreateAgentApp(r.Context(), request.App, request.InitialConfig); err != nil {
		writeAdminOperationError(w, err, "create agent app failed")
		return
	}
	writeJSON(w, http.StatusCreated, request.App)
}

func (h adminHTTPHandler) createChannelBinding(w http.ResponseWriter, r *http.Request) {
	var request createChannelBindingRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid channel binding")
		return
	}
	binding, err := h.api.ProvisionChannelBinding(r.Context(), request.Binding)
	if err != nil {
		writeAdminOperationError(w, err, "create channel binding failed")
		return
	}
	writeJSON(w, http.StatusCreated, binding)
}

func (h adminHTTPHandler) publishAppConfig(w http.ResponseWriter, r *http.Request) {
	var request publishAppConfigRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid app config")
		return
	}
	if err := h.api.PublishAppConfig(r.Context(), request.Config); err != nil {
		writeAdminOperationError(w, err, "publish app config failed")
		return
	}
	writeJSON(w, http.StatusCreated, request.Config)
}

func (h adminHTTPHandler) activateAppConfig(w http.ResponseWriter, r *http.Request) {
	var request activateAppConfigRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid app config activation")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if err := h.api.ActivateAppConfig(r.Context(), scope, request.Version); err != nil {
		writeAdminOperationError(w, err, "activate app config failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h adminHTTPHandler) createDataMigration(w http.ResponseWriter, r *http.Request) {
	var request createDataMigrationRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid data migration")
		return
	}
	record, err := h.api.CreateDataMigration(r.Context(), tenant.Scope{
		TenantID: request.TenantID,
		AppID:    request.AppID,
	}, request.SourceVersion, request.TargetVersion)
	if err != nil {
		writeAdminOperationError(w, err, "create data migration failed")
		return
	}
	writeJSON(w, http.StatusCreated, record)
}

func (h adminHTTPHandler) beginDataMigration(w http.ResponseWriter, r *http.Request) {
	var request beginDataMigrationRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid data migration")
		return
	}
	leaseDuration, err := time.ParseDuration(request.LeaseDuration)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid data migration")
		return
	}
	record, err := h.api.BeginDataMigration(r.Context(), tenant.Scope{
		TenantID: request.TenantID,
		AppID:    request.AppID,
	}, request.MigrationID, request.Owner, request.DrainDeadline, leaseDuration)
	if err != nil {
		writeAdminOperationError(w, err, "begin data migration failed")
		return
	}
	writeJSON(w, http.StatusOK, record)
}

func (h adminHTTPHandler) issueCredential(w http.ResponseWriter, r *http.Request) {
	var request issueCredentialRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid credential request")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	expiresAt := time.Time{}
	if request.ExpiresAt != nil {
		expiresAt = *request.ExpiresAt
	}
	issued, err := h.api.IssueCredential(r.Context(), scope, expiresAt)
	if err != nil {
		writeAdminOperationError(w, err, "issue credential failed")
		return
	}
	var responseExpiresAt *time.Time
	if !issued.Credential.ExpiresAt.IsZero() {
		value := issued.Credential.ExpiresAt
		responseExpiresAt = &value
	}
	writeJSON(w, http.StatusCreated, issueCredentialResponse{
		Credential: credentialResponse{
			ID:        issued.Credential.ID,
			TenantID:  issued.Credential.TenantID,
			AppID:     issued.Credential.AppID,
			KeyPrefix: issued.Credential.KeyPrefix,
			Status:    string(issued.Credential.Status),
			ExpiresAt: responseExpiresAt,
		},
		APIKey: issued.APIKey,
	})
}

func (h adminHTTPHandler) revokeCredential(w http.ResponseWriter, r *http.Request) {
	var request revokeCredentialRequest
	if !decodeJSON(w, r, &request) {
		writeJSONError(w, http.StatusBadRequest, "invalid credential revocation")
		return
	}
	scope := tenant.Scope{TenantID: request.TenantID, AppID: request.AppID}
	if err := h.api.RevokeCredential(r.Context(), scope, request.CredentialID); err != nil {
		writeAdminOperationError(w, err, "revoke credential failed")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h adminHTTPHandler) listAuditEvents(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 0
	if rawLimit := strings.TrimSpace(query.Get("limit")); rawLimit != "" {
		parsed, err := strconv.Atoi(rawLimit)
		if err != nil || parsed <= 0 || parsed > 1000 {
			writeJSONError(w, http.StatusBadRequest, "invalid audit limit")
			return
		}
		limit = parsed
	}
	events, err := h.api.ListAuditEvents(r.Context(), tenant.Scope{
		TenantID: strings.TrimSpace(query.Get("tenant_id")),
		AppID:    strings.TrimSpace(query.Get("app_id")),
	}, limit)
	if err != nil {
		writeAdminOperationError(w, err, "list audit events failed")
		return
	}
	writeJSON(w, http.StatusOK, auditEventsResponse{Events: events})
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	if r == nil || r.Body == nil {
		return false
	}
	body := http.MaxBytesReader(w, r.Body, maxAdminRequestBytes)
	defer func() {
		if err := body.Close(); err != nil {
			log.Printf("close admin request body: %v", err)
		}
	}()
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	var trailing any
	return decoder.Decode(&trailing) == io.EOF
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write admin json response: %v", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, struct {
		Error string `json:"error"`
	}{Error: message})
}

func writeAdminOperationError(w http.ResponseWriter, err error, message string) {
	status := http.StatusInternalServerError
	var inputErr *inputError
	if errors.As(err, &inputErr) {
		status = http.StatusBadRequest
	}
	writeJSONError(w, status, message)
}
