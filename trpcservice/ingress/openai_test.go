package ingress

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

func TestOpenAIHandlerDerivesCredentialServiceIdentity(t *testing.T) {
	handler, admitter, source, _ := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("response status = %d, want %d: %s", response.Code, http.StatusOK, response.Body.String())
	}
	identity := admitter.request.Identity.Tenant
	if identity.UserID != "service:credential-1" || identity.SessionPrincipalID != "service:credential-1" ||
		identity.SessionID != "session-1" || identity.TraceID != "request-1" {
		t.Fatalf("admission identity = %#v", identity)
	}
	if admitter.request.RequestID != "request-1" || admitter.request.IdempotencyKey != "idempotency-1" {
		t.Fatalf("admission request = %#v", admitter.request)
	}
	if source.scope != (tenant.Scope{TenantID: "tenant-a", AppID: "support"}) || source.requestID != "request-1" {
		t.Fatalf("event subscription = scope %#v request %q", source.scope, source.requestID)
	}
}

func TestOpenAIHandlerRejectsCallerIdentityHeaders(t *testing.T) {
	tests := []struct {
		name   string
		header string
	}{
		{name: "user", header: headerUserID},
		{name: "session principal", header: headerSessionPrincipal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, admitter, _, credentials := newTestOpenAIHandler(t)
			request := validOpenAIRequest()
			request.Header.Set(tt.header, "caller-selected")
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != http.StatusBadRequest {
				t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
			}
			if admitter.called || credentials.called {
				t.Fatal("caller identity header reached authentication or admission")
			}
		})
	}
}

func TestOpenAIHandlerRejectsMissingRequestIdentity(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Header.Del(headerSessionID)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if admitter.called || credentials.called {
		t.Fatal("invalid request reached authentication or admission")
	}
}

func TestOpenAIHandlerMapsAuthenticationFailure(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	credentials.err = errors.New("credential store unavailable")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, validOpenAIRequest())

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if !credentials.called || admitter.called {
		t.Fatal("authentication failure did not stop before admission")
	}
}

func TestOpenAIHandlerMapsDrainingAdmission(t *testing.T) {
	handler, admitter, source, _ := newTestOpenAIHandler(t)
	admitter.err = gateway.ErrAdmissionDraining
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, validOpenAIRequest())

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusServiceUnavailable)
	}
	if response.Header().Get("Retry-After") != retryAfterSeconds {
		t.Fatalf("retry after = %q", response.Header().Get("Retry-After"))
	}
	if !admitter.called || source.requestID != "" {
		t.Fatal("draining admission reached event subscription")
	}
}

func TestOpenAIHandlerDoesNotAdmitOtherRoutes(t *testing.T) {
	handler, admitter, _, _ := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.URL.Path = "/v1/not-found"
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusNotFound {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusNotFound)
	}
	if admitter.called {
		t.Fatal("unknown route reached admission")
	}
}

func TestOpenAIHandlerRejectsUnsupportedToolCallsBeforeAdmission(t *testing.T) {
	handler, admitter, _, _ := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Body = io.NopCloser(bytes.NewBufferString(`{
"messages":[{"role":"user","content":"hello","tool_calls":[{"id":"call-1"}]}]
}`))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if admitter.called {
		t.Fatal("unsupported tool call reached admission")
	}
}

func TestOpenAIHandlerMapsAdmissionErrors(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code int
	}{
		{name: "revoked credential", err: auth.ErrCredentialInactive, code: http.StatusForbidden},
		{name: "idempotency conflict", err: gateway.ErrIdempotencyConflict, code: http.StatusConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, admitter, _, _ := newTestOpenAIHandler(t)
			admitter.err = tt.err
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, validOpenAIRequest())

			if response.Code != tt.code {
				t.Fatalf("response status = %d, want %d", response.Code, tt.code)
			}
		})
	}
}

func TestOpenAIHandlerRejectsUnpersistablePayload(t *testing.T) {
	handler, admitter, _, credentials := newTestOpenAIHandler(t)
	request := validOpenAIRequest()
	request.Body = io.NopCloser(bytes.NewBufferString(
		`{"messages":[{"role":"system","content":"ignored"},{"role":"user","content":"hello"}]}`,
	))
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusBadRequest)
	}
	if !credentials.called || admitter.called {
		t.Fatal("unsupported payload did not stop before admission")
	}
}

func validOpenAIRequest() *http.Request {
	request := httptest.NewRequest(
		http.MethodPost,
		"/v1/chat/completions",
		bytes.NewBufferString(`{"model":"ignored","user":"payload-user","messages":[{"role":"user","content":"hello"}]}`),
	)
	request.Header.Set("Authorization", "Bearer test-key")
	request.Header.Set(headerRequestID, "request-1")
	request.Header.Set(headerIdempotencyKey, "idempotency-1")
	request.Header.Set(headerSessionID, "session-1")
	return request
}

func newTestOpenAIHandler(t *testing.T) (
	http.Handler,
	*recordingAdmitter,
	*recordingEventSource,
	*recordingCredentialStore,
) {
	t.Helper()
	credentials := &recordingCredentialStore{credential: auth.Credential{
		ID:       "credential-1",
		TenantID: "tenant-a",
		AppID:    "support",
		Status:   auth.CredentialActive,
	}}
	directory := testDirectory{
		tenant: tenant.Tenant{ID: "tenant-a", Name: "Tenant A", Status: tenant.StatusActive},
		app: tenant.AgentApp{
			TenantID:            "tenant-a",
			AppID:               "support",
			Name:                "Support",
			ActiveConfigVersion: "v1",
			Status:              tenant.StatusActive,
		},
	}
	admitter := &recordingAdmitter{}
	source := &recordingEventSource{}
	queued, err := gateway.NewQueuedRunner(gateway.New(gateway.WithAdmitter(admitter)), source)
	if err != nil {
		t.Fatalf("new queued runner: %v", err)
	}
	handler, err := NewOpenAIHandler(auth.HTTPAPIKeyResolver{
		Credentials: credentials,
		Directory:   directory,
	}, queued)
	if err != nil {
		t.Fatalf("new OpenAI handler: %v", err)
	}
	return handler, admitter, source, credentials
}

type recordingCredentialStore struct {
	credential auth.Credential
	err        error
	called     bool
}

func (s *recordingCredentialStore) ResolveAPIKey(context.Context, auth.APIKeyDigest) (auth.Credential, error) {
	s.called = true
	if s.err != nil {
		return auth.Credential{}, s.err
	}
	return s.credential, nil
}

type testDirectory struct {
	tenant tenant.Tenant
	app    tenant.AgentApp
}

func (d testDirectory) ResolveTenant(context.Context, string) (tenant.Tenant, error) {
	return d.tenant, nil
}

func (d testDirectory) ResolveAgentApp(context.Context, string, string) (tenant.AgentApp, error) {
	return d.app, nil
}

type recordingAdmitter struct {
	called  bool
	request gateway.AdmissionRequest
	err     error
}

func (a *recordingAdmitter) Admit(
	_ context.Context,
	request gateway.AdmissionRequest,
) (gateway.AdmissionResult, error) {
	a.called = true
	a.request = request
	if a.err != nil {
		return gateway.AdmissionResult{}, a.err
	}
	return gateway.AdmissionResult{
		RequestID:     request.RequestID,
		ConfigVersion: "v1",
		TurnSeq:       1,
	}, nil
}

type recordingEventSource struct {
	scope     tenant.Scope
	requestID string
}

func (s *recordingEventSource) SubscribeExecutionEvents(
	_ context.Context,
	scope tenant.Scope,
	requestID string,
	_ int64,
) (<-chan gateway.ExecutionEvent, error) {
	s.scope = scope
	s.requestID = requestID
	events := make(chan gateway.ExecutionEvent, 1)
	events <- gateway.ExecutionEvent{
		Sequence: 1,
		Event: &event.Event{Response: &model.Response{
			Choices: []model.Choice{{
				Message: model.Message{Role: model.RoleAssistant, Content: "accepted"},
			}},
			Done: true,
		}},
	}
	close(events)
	return events, nil
}
