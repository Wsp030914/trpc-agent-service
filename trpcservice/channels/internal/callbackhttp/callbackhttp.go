// Package callbackhttp contains HTTP plumbing shared by channel callbacks.
package callbackhttp

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
)

var (
	// ErrBodyRequired means a callback did not contain a non-blank body.
	ErrBodyRequired = errors.New("callback body is required")
	// ErrBodyTooLarge means a callback exceeded its configured body limit.
	ErrBodyTooLarge = errors.New("callback body is too large")
	// ErrInvalidContentType means a callback was not sent as JSON.
	ErrInvalidContentType = errors.New("callback content type is invalid")
	errInvalidPath        = errors.New("invalid callback path")
)

// StatusRecorder preserves the response status so adapters can classify a
// callback after all verification and admission work has completed. It
// forwards the underlying writer through Unwrap for HTTP response helpers.
type StatusRecorder struct {
	http.ResponseWriter
	status int
}

// NewStatusRecorder wraps a callback response writer.
func NewStatusRecorder(w http.ResponseWriter) *StatusRecorder {
	return &StatusRecorder{ResponseWriter: w}
}

// WriteHeader records the first response status, matching net/http behavior.
func (w *StatusRecorder) WriteHeader(status int) {
	if w == nil || w.ResponseWriter == nil || w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

// Write records the implicit successful status used by net/http.
func (w *StatusRecorder) Write(payload []byte) (int, error) {
	if w == nil || w.ResponseWriter == nil {
		return 0, errors.New("response writer is unavailable")
	}
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(payload)
}

// Status returns the emitted status, defaulting to 200 when no header was
// written yet.
func (w *StatusRecorder) Status() int {
	if w == nil || w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

// Unwrap exposes the original writer to http.ResponseController.
func (w *StatusRecorder) Unwrap() http.ResponseWriter {
	if w == nil {
		return nil
	}
	return w.ResponseWriter
}

// CallbackErrorType maps the finite callback response classes to stable,
// low-cardinality metric error types.
func CallbackErrorType(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid_callback"
	case http.StatusForbidden:
		return "binding_forbidden"
	case http.StatusNotFound:
		return "route_not_found"
	case http.StatusMethodNotAllowed:
		return "method_not_allowed"
	case http.StatusConflict:
		return "idempotency_conflict"
	case http.StatusRequestEntityTooLarge:
		return "body_too_large"
	case http.StatusUnauthorized:
		return "unauthenticated"
	default:
		if status >= http.StatusInternalServerError {
			return "callback_unavailable"
		}
		if status >= http.StatusBadRequest {
			return "callback_rejected"
		}
		return ""
	}
}

// RouteKey extracts the public route key from a channel callback path.
func RouteKey(r *http.Request, channel channels.Channel) (string, error) {
	if r == nil || r.URL == nil {
		return "", errInvalidPath
	}
	path := strings.TrimSuffix(r.URL.Path, "/")
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) != 3 || parts[0] != "im" || parts[1] != string(channel) || parts[2] == "" {
		return "", errInvalidPath
	}
	return parts[2], nil
}

// ValidateJSONContentType accepts only the JSON media type used by callbacks.
func ValidateJSONContentType(r *http.Request) error {
	if r == nil {
		return ErrInvalidContentType
	}
	value := strings.TrimSpace(r.Header.Get("Content-Type"))
	if value == "" {
		return ErrInvalidContentType
	}
	mediaType, _, err := mime.ParseMediaType(value)
	if err != nil || mediaType != "application/json" {
		return ErrInvalidContentType
	}
	return nil
}

// ReadBody reads a callback body while enforcing the configured byte limit.
func ReadBody(w http.ResponseWriter, r *http.Request, maxBytes int64) ([]byte, error) {
	if r == nil || r.Body == nil {
		return nil, ErrBodyRequired
	}
	if maxBytes <= 0 {
		return nil, ErrBodyTooLarge
	}
	limited := http.MaxBytesReader(w, r.Body, maxBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return nil, ErrBodyTooLarge
		}
		return nil, ErrBodyRequired
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, ErrBodyRequired
	}
	return body, nil
}

// WriteRouteError maps route resolution failures to callback HTTP responses.
func WriteRouteError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, channels.ErrBindingNotFound), errors.Is(err, channels.ErrBindingChannelMismatch):
		http.NotFound(w, r)
	case errors.Is(err, channels.ErrBindingInactive):
		http.Error(w, "callback binding is inactive", http.StatusForbidden)
	default:
		http.Error(w, "callback route unavailable", http.StatusServiceUnavailable)
	}
}

// WriteProtocolError maps callback verification failures to HTTP responses.
func WriteProtocolError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrBodyTooLarge) {
		http.Error(w, "callback body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "invalid callback", http.StatusBadRequest)
}

// WriteAdmissionError maps Gateway admission failures to callback responses.
func WriteAdmissionError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, gateway.ErrIdempotencyConflict):
		http.Error(w, "callback idempotency conflict", http.StatusConflict)
	case errors.Is(err, channels.ErrBindingInactive), errors.Is(err, gateway.ErrChannelBindingSnapshotStale):
		http.Error(w, "callback binding changed", http.StatusServiceUnavailable)
	default:
		http.Error(w, "callback admission unavailable", http.StatusServiceUnavailable)
	}
}
