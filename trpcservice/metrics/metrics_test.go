package metrics_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
)

func TestNopCollectorValidatesTenantAwareEvent(t *testing.T) {
	event := metrics.Event{
		TenantID:  "tenant-a",
		AppID:     "support",
		TraceID:   "trace-1",
		Component: metrics.ComponentWorker,
		Operation: "prepare",
	}
	if err := (metrics.NopCollector{}).Record(context.Background(), event); err != nil {
		t.Fatalf("record event: %v", err)
	}
}

func TestNopCollectorRejectsIncompleteEvent(t *testing.T) {
	event := metrics.Event{
		TenantID:  "tenant-a",
		AppID:     "support",
		Component: metrics.ComponentWorker,
		Operation: "prepare",
	}
	if err := (metrics.NopCollector{}).Record(context.Background(), event); err == nil {
		t.Fatal("record event succeeded without trace_id")
	}
}
