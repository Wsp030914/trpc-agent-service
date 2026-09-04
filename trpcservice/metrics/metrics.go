// Package metrics records service measurements with the OpenTelemetry metric
// API. Labels are deliberately fixed to low-cardinality deployment fields.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// ModelPrice is operator-owned pricing metadata. Tenants cannot provide it.
type ModelPrice struct {
	InputPerMillion  float64 `json:"input_per_million"`
	OutputPerMillion float64 `json:"output_per_million"`
}

// PricingCatalog estimates cost only for explicitly configured models.
type PricingCatalog struct {
	prices map[string]ModelPrice
}

// NewPricingCatalog validates and copies operator pricing metadata.
func NewPricingCatalog(prices map[string]ModelPrice) (PricingCatalog, error) {
	copyOf := make(map[string]ModelPrice, len(prices))
	for key, price := range prices {
		key = strings.TrimSpace(key)
		if key == "" {
			return PricingCatalog{}, errors.New("pricing model key is required")
		}
		if price.InputPerMillion < 0 || price.OutputPerMillion < 0 ||
			math.IsNaN(price.InputPerMillion) || math.IsNaN(price.OutputPerMillion) ||
			math.IsInf(price.InputPerMillion, 0) || math.IsInf(price.OutputPerMillion, 0) {
			return PricingCatalog{}, fmt.Errorf("pricing for %q must be finite and non-negative", key)
		}
		copyOf[key] = price
	}
	return PricingCatalog{prices: copyOf}, nil
}

// ParsePricingJSON parses an operator-supplied JSON object keyed by
// "provider/model". Empty input means no pricing is configured.
func ParsePricingJSON(encoded string) (PricingCatalog, error) {
	if strings.TrimSpace(encoded) == "" {
		return NewPricingCatalog(nil)
	}
	var prices map[string]ModelPrice
	if err := json.Unmarshal([]byte(encoded), &prices); err != nil {
		return PricingCatalog{}, fmt.Errorf("parse model pricing: %w", err)
	}
	return NewPricingCatalog(prices)
}

// Estimate returns a cost in the operator's configured currency unit. The
// boolean is false when pricing is unknown.
func (p PricingCatalog) Estimate(provider, model string, inputTokens, outputTokens int) (float64, bool) {
	if inputTokens < 0 || outputTokens < 0 {
		return 0, false
	}
	price, ok := p.prices[strings.TrimSpace(provider)+"/"+strings.TrimSpace(model)]
	if !ok {
		return 0, false
	}
	return float64(inputTokens)*price.InputPerMillion/1_000_000 +
		float64(outputTokens)*price.OutputPerMillion/1_000_000, true
}

// Labels is the only information accepted as metric attributes.
type Labels struct {
	TenantID  string
	AppID     string
	Channel   string
	Provider  string
	Operation string
	Result    string
	ErrorType string
}

func (l Labels) attributes() []attribute.KeyValue {
	values := []struct {
		key   string
		value string
	}{
		{"tenant_id", l.TenantID},
		{"app_id", l.AppID},
		{"channel", l.Channel},
		{"provider", l.Provider},
		{"operation", l.Operation},
		{"result", l.Result},
		{"error_type", l.ErrorType},
	}
	attrs := make([]attribute.KeyValue, 0, len(values))
	for _, item := range values {
		if item.value != "" {
			attrs = append(attrs, attribute.String(item.key, item.value))
		}
	}
	return attrs
}

func setResultForError(labels *Labels, success, errType string) {
	if errType != "" {
		if labels.Result == "" || labels.Result == "success" || labels.Result == "accepted" {
			labels.Result = "failure"
		}
		return
	}
	if labels.Result == "" {
		labels.Result = success
	}
}

// Recorder owns the service's fixed metric instruments. It is safe for
// concurrent use and a nil recorder is a no-op.
type Recorder struct {
	requestCount        metric.Int64Counter
	errorCount          metric.Int64Counter
	modelLatency        metric.Float64Histogram
	modelInputTokens    metric.Int64Counter
	modelOutputTokens   metric.Int64Counter
	toolCalls           metric.Int64Counter
	toolLatency         metric.Float64Histogram
	toolErrors          metric.Int64Counter
	imCallbacks         metric.Int64Counter
	replySuccess        metric.Int64Counter
	replyFailure        metric.Int64Counter
	replyLatency        metric.Float64Histogram
	sessionLatency      metric.Float64Histogram
	sessionErrors       metric.Int64Counter
	memoryLatency       metric.Float64Histogram
	memoryErrors        metric.Int64Counter
	tenantTokenUsage    metric.Int64Counter
	tenantEstimatedCost metric.Float64Counter
	auditWriteFailures  metric.Int64Counter
	auditPurgeEvents    metric.Int64Counter
	governanceRejected  metric.Int64Counter
	pricing             PricingCatalog
}

// EstimateCost returns the operator-priced cost for one model usage sample.
// The boolean is false when the model is not priced.
func (r *Recorder) EstimateCost(provider, model string, inputTokens, outputTokens int) *float64 {
	if r == nil {
		return nil
	}
	cost, ok := r.pricing.Estimate(provider, model, inputTokens, outputTokens)
	if !ok {
		return nil
	}
	return &cost
}

// New creates fixed instruments from a standard OTel MeterProvider.
func New(provider metric.MeterProvider, pricing PricingCatalog) (*Recorder, error) {
	if provider == nil {
		return nil, errors.New("meter provider is required")
	}
	meter := provider.Meter("trpc-agent-service")
	newCounter := func(name string) (metric.Int64Counter, error) {
		return meter.Int64Counter(name)
	}
	newHistogram := func(name string) (metric.Float64Histogram, error) {
		return meter.Float64Histogram(name)
	}
	var err error
	r := &Recorder{pricing: pricing}
	if r.requestCount, err = newCounter("trpc_agent_service.request.count"); err != nil {
		return nil, err
	}
	if r.errorCount, err = newCounter("trpc_agent_service.error.count"); err != nil {
		return nil, err
	}
	if r.modelLatency, err = newHistogram("trpc_agent_service.model.latency"); err != nil {
		return nil, err
	}
	if r.modelInputTokens, err = newCounter("trpc_agent_service.model.input_tokens"); err != nil {
		return nil, err
	}
	if r.modelOutputTokens, err = newCounter("trpc_agent_service.model.output_tokens"); err != nil {
		return nil, err
	}
	if r.toolCalls, err = newCounter("trpc_agent_service.tool.calls"); err != nil {
		return nil, err
	}
	if r.toolLatency, err = newHistogram("trpc_agent_service.tool.latency"); err != nil {
		return nil, err
	}
	if r.toolErrors, err = newCounter("trpc_agent_service.tool.errors"); err != nil {
		return nil, err
	}
	if r.imCallbacks, err = newCounter("trpc_agent_service.im.callback.count"); err != nil {
		return nil, err
	}
	if r.replySuccess, err = newCounter("trpc_agent_service.im.reply.success"); err != nil {
		return nil, err
	}
	if r.replyFailure, err = newCounter("trpc_agent_service.im.reply.failure"); err != nil {
		return nil, err
	}
	if r.replyLatency, err = newHistogram("trpc_agent_service.im.reply.latency"); err != nil {
		return nil, err
	}
	if r.sessionLatency, err = newHistogram("trpc_agent_service.session.operation.latency"); err != nil {
		return nil, err
	}
	if r.sessionErrors, err = newCounter("trpc_agent_service.session.error.count"); err != nil {
		return nil, err
	}
	if r.memoryLatency, err = newHistogram("trpc_agent_service.memory.operation.latency"); err != nil {
		return nil, err
	}
	if r.memoryErrors, err = newCounter("trpc_agent_service.memory.error.count"); err != nil {
		return nil, err
	}
	if r.tenantTokenUsage, err = newCounter("trpc_agent_service.tenant.model.token_usage"); err != nil {
		return nil, err
	}
	if r.tenantEstimatedCost, err = meter.Float64Counter("trpc_agent_service.tenant.estimated_cost"); err != nil {
		return nil, err
	}
	if r.auditWriteFailures, err = newCounter("trpc_agent_service.audit.write.failure"); err != nil {
		return nil, err
	}
	if r.auditPurgeEvents, err = newCounter("trpc_agent_service.audit.purge.events"); err != nil {
		return nil, err
	}
	if r.governanceRejected, err = newCounter("trpc_agent_service.governance.rejected"); err != nil {
		return nil, err
	}
	return r, nil
}

// RecordRequest records ingress volume and optional classified errors.
func (r *Recorder) RecordRequest(ctx context.Context, labels Labels, errType string) {
	if r == nil {
		return
	}
	labels.Operation = "request"
	labels.ErrorType = errType
	setResultForError(&labels, "accepted", errType)
	attrs := labels.attributes()
	r.requestCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	if errType != "" {
		r.errorCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordModel records latency, token usage, and cost when operator pricing is
// known. Unknown pricing intentionally emits no fabricated cost.
func (r *Recorder) RecordModel(ctx context.Context, labels Labels, provider, model string, latency time.Duration, inputTokens, outputTokens int) {
	if r == nil {
		return
	}
	labels.Provider = provider
	labels.Operation = "model"
	if labels.Result == "" {
		labels.Result = "success"
	}
	attrs := labels.attributes()
	r.modelLatency.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
	if inputTokens > 0 {
		r.modelInputTokens.Add(ctx, int64(inputTokens), metric.WithAttributes(attrs...))
	}
	if outputTokens > 0 {
		r.modelOutputTokens.Add(ctx, int64(outputTokens), metric.WithAttributes(attrs...))
	}
	if total := inputTokens + outputTokens; total > 0 {
		r.tenantTokenUsage.Add(ctx, int64(total), metric.WithAttributes(attrs...))
	}
	if cost, ok := r.pricing.Estimate(provider, model, inputTokens, outputTokens); ok {
		r.tenantEstimatedCost.Add(ctx, cost, metric.WithAttributes(attrs...))
	}
}

// RecordTool records one tool call without accepting its name or arguments as
// metric labels.
func (r *Recorder) RecordTool(ctx context.Context, labels Labels, latency time.Duration, errType string) {
	if r == nil {
		return
	}
	labels.Operation = "tool"
	labels.ErrorType = errType
	setResultForError(&labels, "success", errType)
	attrs := labels.attributes()
	r.toolCalls.Add(ctx, 1, metric.WithAttributes(attrs...))
	r.toolLatency.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
	if errType != "" {
		r.toolErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordIMCallback records a channel callback result.
func (r *Recorder) RecordIMCallback(ctx context.Context, labels Labels, errType string) {
	if r == nil {
		return
	}
	labels.Operation = "im.callback"
	labels.ErrorType = errType
	setResultForError(&labels, "success", errType)
	attrs := labels.attributes()
	r.imCallbacks.Add(ctx, 1, metric.WithAttributes(attrs...))
	if errType != "" {
		r.errorCount.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordReply records reply send result and latency.
func (r *Recorder) RecordReply(ctx context.Context, labels Labels, latency time.Duration, errType string) {
	if r == nil {
		return
	}
	labels.Operation = "im.reply"
	labels.ErrorType = errType
	if errType == "" {
		labels.Result = "success"
	} else {
		labels.Result = "failure"
	}
	attrs := labels.attributes()
	r.replyLatency.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
	if errType == "" {
		r.replySuccess.Add(ctx, 1, metric.WithAttributes(attrs...))
	} else {
		r.replyFailure.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordSession records session backend latency and classified failures.
func (r *Recorder) RecordSession(ctx context.Context, labels Labels, latency time.Duration, errType string) {
	if r == nil {
		return
	}
	labels.Operation = "session"
	labels.ErrorType = errType
	setResultForError(&labels, "success", errType)
	attrs := labels.attributes()
	r.sessionLatency.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
	if errType != "" {
		r.sessionErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordMemory records an immutable-config-selected memory backend call.
func (r *Recorder) RecordMemory(ctx context.Context, labels Labels, latency time.Duration, errType string) {
	if r == nil {
		return
	}
	labels.Operation = "memory"
	labels.ErrorType = errType
	setResultForError(&labels, "success", errType)
	attrs := labels.attributes()
	r.memoryLatency.Record(ctx, latency.Seconds(), metric.WithAttributes(attrs...))
	if errType != "" {
		r.memoryErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
}

// RecordAuditFailure records a best-effort audit persistence failure.
func (r *Recorder) RecordAuditFailure(ctx context.Context, labels Labels) {
	if r == nil {
		return
	}
	labels.Operation = "audit"
	labels.Result = "failure"
	r.auditWriteFailures.Add(ctx, 1, metric.WithAttributes(labels.attributes()...))
}

// RecordAuditPurge records the number of rows removed by retention cleanup.
func (r *Recorder) RecordAuditPurge(ctx context.Context, labels Labels, count int64) {
	if r == nil || count <= 0 {
		return
	}
	labels.Operation = "audit_purge"
	labels.Result = "success"
	r.auditPurgeEvents.Add(ctx, count, metric.WithAttributes(labels.attributes()...))
}

// RecordGovernanceRejected records an admission or execution governance
// rejection. errorType is a stable class such as im_access_denied.
func (r *Recorder) RecordGovernanceRejected(ctx context.Context, labels Labels, errorType string) {
	if r == nil {
		return
	}
	labels.Operation = "governance"
	labels.Result = "rejected"
	labels.ErrorType = errorType
	r.governanceRejected.Add(ctx, 1, metric.WithAttributes(labels.attributes()...))
}
