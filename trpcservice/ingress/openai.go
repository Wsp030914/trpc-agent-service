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
	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
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
	retryAfterSeconds      = "60"
	openAIChatPath         = "/v1/chat/completions"
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
	return openAIHandler{authenticator: authenticator, queued: queued, next: server.Handler()}, nil
}

type openAIHandler struct {
	authenticator auth.HTTPAPIKeyResolver
	queued        *gateway.QueuedRunner
	next          http.Handler
}

func (h openAIHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != openAIChatPath && r.URL.Path != openAIChatPath+"/" {
		h.next.ServeHTTP(w, r)
		return
	}
	if r.Method == http.MethodOptions {
		h.next.ServeHTTP(w, r)
		return
	}
	if r.Method != http.MethodPost {
		h.next.ServeHTTP(w, r)
		return
	}
	r = r.WithContext(platformtelemetry.ExtractHTTP(r.Context(), map[string]string{
		"traceparent": r.Header.Get("traceparent"),
		"tracestate":  r.Header.Get("tracestate"),
	}))
	request, err := h.authenticatedRequest(r)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	message, err := validateQueuedOpenAIRequest(w, r)
	if err != nil {
		http.Error(w, "unsupported chat request", http.StatusBadRequest)
		return
	}
	ctx, err := gateway.ContextWithAuthenticatedRequest(r.Context(), request)
	if err != nil {
		writeAuthenticationError(w, err)
		return
	}
	ctx, err = h.queued.Admit(ctx, gateway.Message{Text: message})
	if err != nil {
		writeAdmissionError(w, err)
		return
	}
	h.next.ServeHTTP(w, r.WithContext(ctx))
}

type queuedOpenAIRequest struct {
	Model            string                       `json:"model"`
	Messages         []queuedOpenAIRequestMessage `json:"messages"`
	Temperature      *float64                     `json:"temperature,omitempty"`
	MaxTokens        *int                         `json:"max_tokens,omitempty"`
	Stream           bool                         `json:"stream,omitempty"`
	Tools            json.RawMessage              `json:"tools"`
	ToolChoice       json.RawMessage              `json:"tool_choice"`
	TopP             *float64                     `json:"top_p,omitempty"`
	Stop             []string                     `json:"stop,omitempty"`
	PresencePenalty  *float64                     `json:"presence_penalty,omitempty"`
	FrequencyPenalty *float64                     `json:"frequency_penalty,omitempty"`
	User             string                       `json:"user,omitempty"`
}

type queuedOpenAIRequestMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func validateQueuedOpenAIRequest(w http.ResponseWriter, r *http.Request) (string, error) {
	if r == nil || r.Body == nil {
		return "", errors.New("request body is required")
	}
	body := http.MaxBytesReader(w, r.Body, maxOpenAIRequestBytes)
	encoded, err := io.ReadAll(body)
	if err != nil {
		return "", err
	}
	if err := body.Close(); err != nil {
		return "", err
	}
	r.Body = io.NopCloser(bytes.NewReader(encoded))

	var request queuedOpenAIRequest
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return "", err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", errors.New("request body contains multiple values")
	}
	if len(request.Messages) != 1 || request.Messages[0].Role != "user" {
		return "", errors.New("exactly one user message is required")
	}
	if request.Messages[0].Content == "" {
		return "", errors.New("user message content is required")
	}
	if hasJSONValue(request.Tools) || hasJSONValue(request.ToolChoice) {
		return "", errors.New("client tools are not supported")
	}
	return request.Messages[0].Content, nil
}

func hasJSONValue(value json.RawMessage) bool {
	return len(value) != 0 && !bytes.Equal(bytes.TrimSpace(value), []byte("null"))
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

func writeAdmissionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, gateway.ErrAdmissionDraining):
		w.Header().Set("Retry-After", retryAfterSeconds)
		http.Error(w, "request admission is draining", http.StatusServiceUnavailable)
	case errors.Is(err, gateway.ErrIdempotencyConflict):
		http.Error(w, "request idempotency conflict", http.StatusConflict)
	case errors.Is(err, auth.ErrUnauthenticated),
		errors.Is(err, auth.ErrCredentialInactive),
		errors.Is(err, auth.ErrTenantInactive),
		errors.Is(err, auth.ErrAppInactive):
		writeAuthenticationError(w, err)
	default:
		http.Error(w, "request admission unavailable", http.StatusServiceUnavailable)
	}
}
