package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPercentileUsesSortedLatencySamples(t *testing.T) {
	values := []time.Duration{time.Millisecond, 2 * time.Millisecond, 4 * time.Millisecond, 8 * time.Millisecond}
	if got := percentile(values, 0.50); got != 3*time.Millisecond {
		t.Fatalf("p50 = %v, want 3ms", got)
	}
	if got := percentile(values, 0.99); got < 7*time.Millisecond+879*time.Microsecond || got > 7*time.Millisecond+881*time.Microsecond {
		t.Fatalf("p99 = %v, want approximately 7.88ms", got)
	}
}

func TestCapacityOptionsRequireFiniteRun(t *testing.T) {
	if err := validateOptions(options{endpoint: "http://localhost", concurrency: 1}); err == nil {
		t.Fatal("empty capacity run was accepted")
	}
}

func TestCapacityOptionsRejectEndpointCredentials(t *testing.T) {
	for _, endpoint := range []string{
		"https://user:password@example.test/chat",
		"https://example.test/chat?token=secret",
		"https://example.test/chat#secret",
	} {
		err := validateOptions(options{
			endpoint:      endpoint,
			concurrency:   1,
			requests:      1,
			payload:       defaultCapacityPayload,
			timeout:       time.Second,
			sessionPrefix: "session",
		})
		if err == nil {
			t.Fatalf("endpoint %q was accepted", endpoint)
		}
	}
}

func TestMeasureDoesNotFollowRedirects(t *testing.T) {
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetCalls++
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer redirect.Close()
	result, err := measure(context.Background(), options{
		endpoint:      redirect.URL,
		concurrency:   1,
		requests:      1,
		payload:       defaultCapacityPayload,
		timeout:       time.Second,
		sessionPrefix: "session",
	}, redirect.Client())
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if result.Success != 0 || result.Failure != 1 {
		t.Fatalf("report = %#v", result)
	}
	if targetCalls != 0 {
		t.Fatalf("redirect target received %d requests", targetCalls)
	}
}

func TestMeasureRequiresHTTPClient(t *testing.T) {
	_, err := measure(context.Background(), options{
		endpoint:      "http://localhost",
		concurrency:   1,
		requests:      1,
		payload:       defaultCapacityPayload,
		timeout:       time.Second,
		sessionPrefix: "session",
	}, nil)
	if err == nil {
		t.Fatal("nil HTTP client was accepted")
	}
}

func TestMeasureSendsScopedHeadersAndReportsResults(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for _, name := range []string{"X-Request-ID", "Idempotency-Key", "X-Session-ID"} {
			if r.Header.Get(name) == "" {
				t.Errorf("missing %s", name)
			}
		}
		if r.Header.Get("Authorization") != "Bearer test-key" {
			t.Errorf("authorization = %q", r.Header.Get("Authorization"))
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	result, err := measure(context.Background(), options{
		endpoint:      server.URL,
		concurrency:   2,
		requests:      4,
		apiKey:        "test-key",
		payload:       defaultCapacityPayload,
		timeout:       time.Second,
		sessionPrefix: "session",
	}, server.Client())
	if err != nil {
		t.Fatalf("measure: %v", err)
	}
	if result.TotalRequests != 4 || result.Success != 4 || result.Failure != 0 {
		t.Fatalf("report = %#v", result)
	}
	if result.P50LatencyMS <= 0 || result.P99LatencyMS <= 0 {
		t.Fatalf("latency report = %#v", result)
	}
	if !json.Valid([]byte(defaultCapacityPayload)) {
		t.Fatal("default payload is not valid JSON")
	}
}
