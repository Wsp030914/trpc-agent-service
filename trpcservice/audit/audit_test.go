package audit_test

import (
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/audit"
)

func TestEventValidateRequiresScopedCorrelationMetadata(t *testing.T) {
	event := audit.Event{
		TenantID:      "tenant-a",
		AppID:         "app-a",
		Decision:      "completed",
		EventType:     audit.ExecutionCompleted,
		TraceID:       "trace-a",
		RequestID:     "request-a",
		ConfigVersion: "v1",
	}
	if err := event.Validate(); err != nil {
		t.Fatalf("validate event: %v", err)
	}

	event.ToolName = "delete"
	event.Latency = time.Second
	event.InputTokens = 10
	event.OutputTokens = 20
	event.TotalTokens = 30
	if err := event.Validate(); err != nil {
		t.Fatalf("validate metadata-only tool event: %v", err)
	}
}

func TestEventValidateRejectsInvalidMeasurements(t *testing.T) {
	base := audit.Event{
		TenantID:      "tenant-a",
		AppID:         "app-a",
		Decision:      "completed",
		EventType:     audit.ExecutionCompleted,
		TraceID:       "trace-a",
		RequestID:     "request-a",
		ConfigVersion: "v1",
	}
	tests := []audit.Event{
		func() audit.Event { event := base; event.Latency = -time.Millisecond; return event }(),
		func() audit.Event { event := base; event.TotalTokens = -1; return event }(),
	}
	for _, event := range tests {
		if err := event.Validate(); err == nil {
			t.Fatal("invalid audit measurement was accepted")
		}
	}
	cost := -0.1
	base.Cost = &cost
	if err := base.Validate(); err == nil {
		t.Fatal("negative audit cost was accepted")
	}
}
