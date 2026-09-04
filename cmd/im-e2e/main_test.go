package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRunRequiresProviderConfirmedReply(t *testing.T) {
	var received confirmationRequest
	var receivedAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/service/readyz":
			w.WriteHeader(http.StatusOK)
		case "/confirm":
			receivedAuthorization = r.Header.Get("Authorization")
			if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
				http.Error(w, "invalid request", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(confirmationResponse{
				Confirmed:       true,
				Provider:        received.Provider,
				CorrelationID:   received.CorrelationID,
				EventAccepted:   true,
				EventStatus:     http.StatusOK,
				ExecutionStatus: "SUCCEEDED",
				ReplyConfirmed:  true,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	opts := options{
		provider:          "feishu",
		baseURL:           server.URL + "/service",
		confirmationURL:   server.URL + "/confirm",
		apiKey:            "runner-token",
		allowInsecureHTTP: true,
		correlationID:     "corr-1",
		message:           "hello",
		timeout:           time.Second,
	}
	if err := run(context.Background(), opts, server.Client()); err != nil {
		t.Fatalf("run: %v", err)
	}
	if receivedAuthorization != "Bearer runner-token" {
		t.Fatalf("authorization = %q", receivedAuthorization)
	}
	if received.ServiceBaseURL != opts.baseURL || received.Message != opts.message {
		t.Fatalf("confirmation request = %+v", received)
	}
}

func TestRunRejectsUnconfirmedReplyWithoutLeakingBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/readyz") {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"confirmed":false,"error":"sk-test-super-secret"}`))
	}))
	defer server.Close()

	opts := options{
		provider:          "wecom",
		baseURL:           server.URL,
		confirmationURL:   server.URL + "/confirm",
		allowInsecureHTTP: true,
		correlationID:     "corr-2",
		message:           "hello",
		timeout:           time.Second,
	}
	err := run(context.Background(), opts, server.Client())
	if err == nil || strings.Contains(err.Error(), "sk-test-super-secret") {
		t.Fatalf("error = %v", err)
	}
}

func TestParseEndpointRejectsCredentials(t *testing.T) {
	if _, err := parseEndpoint("https://user:password@example.test", "base-url", false); err == nil {
		t.Fatal("endpoint with credentials was accepted")
	}
}

func TestValidateOptionsRequiresHTTPSByDefault(t *testing.T) {
	opts := options{
		provider:        "feishu",
		baseURL:         "http://127.0.0.1:8080",
		confirmationURL: "http://127.0.0.1:8081/confirm",
		correlationID:   "corr",
		message:         "hello",
		timeout:         time.Second,
	}
	if err := validateOptions(opts); err == nil || !strings.Contains(err.Error(), "must use https") {
		t.Fatalf("validate options error = %v", err)
	}
	opts.allowInsecureHTTP = true
	if err := validateOptions(opts); err != nil {
		t.Fatalf("loopback HTTP test endpoint rejected: %v", err)
	}
}
