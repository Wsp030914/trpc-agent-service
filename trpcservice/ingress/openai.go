// Package ingress adapts authenticated HTTP protocols to the platform gateway.
package ingress

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/auth"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	openaiserver "trpc.group/trpc-go/trpc-agent-go/server/openai"
)

const (
	maxOpenAIRequestBytes = 1 << 20

	headerRequestID        = "X-Request-ID"
	headerIdempotencyKey   = "Idempotency-Key"
	headerSessionID        = "X-Session-ID"
	headerUserID           = "X-User-ID"
	headerSessionPrincipal = "X-Session-Principal-ID"
	headerTraceID          = "X-Trace-ID"
)

var errInvalidRequestIdentity = errors.New("invalid request identity")

// NewOpenAIHandler creates the OpenAI-compatible endpoint at
// /v1/chat/completions. It requires Authorization: Bearer, X-Request-ID,
// Idempotency-Key, and X-Session-ID headers. X-Trace-ID defaults to
// X-Request-ID. The authenticated credential determines tenant, application,
// user, and session principal; callers can select only a session within that
// service principal. The OpenAI request payload cannot select tenant,
// application, session, user, or config scope. This entry accepts one user
// text message and no client-declared tools or conversation history because
// the durable backend owns those concerns.
func NewOpenAIHandler(authenticator auth.HTTPAPIKeyResolver, queued *gateway.QueuedRunner) (http.Handler, error) {
	if authenticator.Credentials == nil {
		return nil, errors.New("credential store is required")
	}
	if authenticator.Directory == nil {
		return nil, errors.New("tenant directory is required")
	}
	if queued == nil {
		return nil, errors.New("queued runner is required")
	}
	server, err := openaiserver.New(openaiserver.WithRunner(queued))
	if err != nil {
		return nil, err
	}
	return openAIHandler{authenticator: authenticator, next: server.Handler()}, nil
}

type openAIHandler struct {
	authenticator auth.HTTPAPIKeyResolver
	next          http.Handler
}

func (h openAIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		h.next.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		h.next.ServeHTTP(w, r)
		return
	}
	request, err := h.authenticatedRequest(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	if err := validateQueuedOpenAIRequest(w, r); err != nil {
		http.Error(w, "unsupported chat request", http.StatusBadRequest)
		return
	}
	ctx, err := gateway.ContextWithAuthenticatedRequest(r.Context(), request)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	h.next.ServeHTTP(w, r.WithContext(ctx))
}

type openAIRequestEnvelope struct {
	Messages []openAIRequestMessage `json:"messages"`
	Tools    json.RawMessage        `json:"tools"`
}

type openAIRequestMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

func validateQueuedOpenAIRequest(w http.ResponseWriter, r *http.Request) error {
	if r == nil || r.Body == nil {
		return errors.New("request body is required")
	}
	body := http.MaxBytesReader(w, r.Body, maxOpenAIRequestBytes)
	encoded, err := io.ReadAll(body)
	if err != nil {
		return err
	}
	if err := body.Close(); err != nil {
		return err
	}
	r.Body = io.NopCloser(bytes.NewReader(encoded))

	var envelope openAIRequestEnvelope
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	if err := decoder.Decode(&envelope); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return errors.New("request body contains multiple values")
	}
	if len(envelope.Messages) != 1 || envelope.Messages[0].Role != "user" {
		return errors.New("exactly one user message is required")
	}
	var content string
	if err := json.Unmarshal(envelope.Messages[0].Content, &content); err != nil || content == "" {
		return errors.New("user message content is required")
	}
	if len(envelope.Tools) != 0 && string(envelope.Tools) != "null" {
		return errors.New("client tools are not supported")
	}
	return nil
}

func (h openAIHandler) authenticatedRequest(r *http.Request) (gateway.AuthenticatedRequest, error) {
	if r == nil {
		return gateway.AuthenticatedRequest{}, errInvalidRequestIdentity
	}
	requestID, err := requiredHeader(r, headerRequestID)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	idempotencyKey, err := requiredHeader(r, headerIdempotencyKey)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	sessionID, err := requiredHeader(r, headerSessionID)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	if err := rejectHeader(r, headerUserID); err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	if err := rejectHeader(r, headerSessionPrincipal); err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	traceID, err := optionalHeader(r, headerTraceID)
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	if traceID == "" {
		traceID = requestID
	}
	tenantResolver, err := h.authenticator.Resolve(r.Context(), r, auth.RequestIdentity{
		SessionID: sessionID,
		TraceID:   traceID,
	})
	if err != nil {
		return gateway.AuthenticatedRequest{}, err
	}
	return gateway.AuthenticatedRequest{
		RequestID:      requestID,
		IdempotencyKey: idempotencyKey,
		Tenant:         tenantResolver,
	}, nil
}

func requiredHeader(r *http.Request, name string) (string, error) {
	value, err := optionalHeader(r, name)
	if err != nil || value == "" {
		return "", errInvalidRequestIdentity
	}
	return value, nil
}

func optionalHeader(r *http.Request, name string) (string, error) {
	values := r.Header.Values(name)
	if len(values) > 1 {
		return "", errInvalidRequestIdentity
	}
	if len(values) == 0 {
		return "", nil
	}
	return strings.TrimSpace(values[0]), nil
}

func rejectHeader(r *http.Request, name string) error {
	if len(r.Header.Values(name)) != 0 {
		return errInvalidRequestIdentity
	}
	return nil
}

func writeAuthenticationError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, errInvalidRequestIdentity):
		http.Error(w, "invalid request identity", http.StatusBadRequest)
	case errors.Is(err, auth.ErrUnauthenticated):
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	case errors.Is(err, auth.ErrCredentialInactive),
		errors.Is(err, auth.ErrTenantInactive),
		errors.Is(err, auth.ErrAppInactive):
		http.Error(w, "forbidden", http.StatusForbidden)
	default:
		http.Error(w, "authentication unavailable", http.StatusServiceUnavailable)
	}
}
