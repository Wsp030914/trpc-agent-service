package telemetry_test

import (
	"context"
	"testing"

	platformtelemetry "github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
)

func TestW3CTraceContextSurvivesDurableBoundary(t *testing.T) {
	runtime := platformtelemetry.NewNoop(context.Background(), "test-service")
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })

	parentCtx, parentSpan := platformtelemetry.StartSpan(context.Background(), "parent")
	defer parentSpan.End()
	traceID := platformtelemetry.TraceID(parentCtx)
	if traceID == "" {
		t.Fatal("parent span did not create a trace id")
	}
	carrier := platformtelemetry.Inject(parentCtx)
	if carrier["traceparent"] == "" {
		t.Fatal("traceparent was not injected")
	}

	workerCtx := platformtelemetry.Extract(context.Background(), carrier)
	workerCtx, workerSpan := platformtelemetry.StartSpan(workerCtx, "worker")
	defer workerSpan.End()
	if got := platformtelemetry.TraceID(workerCtx); got != traceID {
		t.Fatalf("worker trace id = %q, want %q", got, traceID)
	}
	if platformtelemetry.TraceParent(workerCtx) == "" {
		t.Fatal("worker traceparent was not injectable")
	}
}

func TestStartWithoutOTLPEndpointSucceeds(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	runtime, err := platformtelemetry.Start(context.Background(), platformtelemetry.Config{
		ServiceName: "test-service",
		Protocol:    "grpc",
	})
	if err != nil {
		t.Fatalf("start telemetry without endpoint: %v", err)
	}
	if runtime == nil || runtime.TracerProvider == nil || runtime.MeterProvider == nil {
		t.Fatal("telemetry did not return SDK providers")
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatalf("close telemetry: %v", err)
	}
}

func TestStartRejectsUnknownProtocol(t *testing.T) {
	if _, err := platformtelemetry.Start(context.Background(), platformtelemetry.Config{Protocol: "udp"}); err == nil {
		t.Fatal("unknown telemetry protocol was accepted")
	}
}
