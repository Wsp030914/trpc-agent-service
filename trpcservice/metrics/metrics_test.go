package metrics_test

import (
	"testing"

	platformmetrics "github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestPricingCatalogKeepsUnknownCostUnknown(t *testing.T) {
	catalog, err := platformmetrics.ParsePricingJSON(`{"openai/gpt-4.1":{"input_per_million":2,"output_per_million":8}}`)
	if err != nil {
		t.Fatalf("parse pricing: %v", err)
	}
	recorder, err := platformmetrics.New(noop.NewMeterProvider(), catalog)
	if err != nil {
		t.Fatalf("new metrics recorder: %v", err)
	}
	if cost := recorder.EstimateCost("openai", "unknown", 10, 20); cost != nil {
		t.Fatalf("unknown model cost = %v, want unknown", *cost)
	}
	cost := recorder.EstimateCost("openai", "gpt-4.1", 1_000_000, 500_000)
	if cost == nil || *cost != 6 {
		t.Fatalf("priced model cost = %v, want 6", cost)
	}
}

func TestPricingCatalogRejectsNegativePrices(t *testing.T) {
	if _, err := platformmetrics.NewPricingCatalog(map[string]platformmetrics.ModelPrice{
		"openai/gpt-4.1": {InputPerMillion: -1},
	}); err == nil {
		t.Fatal("negative model price was accepted")
	}
}
