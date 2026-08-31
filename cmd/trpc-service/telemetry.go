package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	telemetrymetric "trpc.group/trpc-go/trpc-agent-go/telemetry/metric"
	telemetrytrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"
)

const (
	envOTELCollectorEndpoint      = "OTEL_EXPORTER_OTLP_ENDPOINT"
	envOTELTracesEndpoint         = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	envOTELMetricsEndpoint        = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
	envOTELProtocol               = "OTEL_EXPORTER_OTLP_PROTOCOL"
	telemetryProtocolGRPC         = "grpc"
	telemetryProtocolHTTP         = "http"
	telemetryProtocolHTTPProtobuf = "http/protobuf"
	telemetryProtocolGRPCProtobuf = "grpc/protobuf"
	telemetryServiceName          = "trpc-agent-service"
)

// telemetryRuntime owns the framework telemetry providers initialized by the
// service process.
type telemetryRuntime struct {
	traceCleanup  func() error
	meterShutdown func(context.Context) error
}

// startTelemetry enables tRPC-Agent-Go OTLP exporters only when an endpoint is
// configured. The default remains the framework's no-op telemetry provider.
func startTelemetry(ctx context.Context) (*telemetryRuntime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !telemetryConfigured(os.Getenv) {
		return &telemetryRuntime{}, nil
	}

	setupCtx := context.WithoutCancel(ctx)
	runtime := &telemetryRuntime{}
	protocol := telemetryProtocol(os.Getenv)
	traceOptions := []telemetrytrace.Option{
		telemetrytrace.WithServiceName(telemetryServiceName),
		telemetrytrace.WithServiceVersion(trpcservice.Version),
	}
	metricOptions := []telemetrymetric.Option{
		telemetrymetric.WithServiceName(telemetryServiceName),
		telemetrymetric.WithServiceVersion(trpcservice.Version),
	}
	if protocol != "" {
		traceOptions = append(traceOptions, telemetrytrace.WithProtocol(protocol))
		metricOptions = append(metricOptions, telemetrymetric.WithProtocol(protocol))
	}

	if telemetryEndpointConfigured(os.Getenv, envOTELCollectorEndpoint) ||
		telemetryEndpointConfigured(os.Getenv, envOTELTracesEndpoint) {
		cleanup, err := telemetrytrace.Start(setupCtx, traceOptions...)
		if err != nil {
			return nil, fmt.Errorf("start telemetry traces: %w", err)
		}
		runtime.traceCleanup = cleanup
	}
	if telemetryEndpointConfigured(os.Getenv, envOTELCollectorEndpoint) ||
		telemetryEndpointConfigured(os.Getenv, envOTELMetricsEndpoint) {
		meterProvider, err := telemetrymetric.NewMeterProvider(setupCtx, metricOptions...)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("create telemetry metrics provider: %w", err), runtime.close())
		}
		runtime.meterShutdown = meterProvider.Shutdown
		if err := telemetrymetric.InitMeterProvider(meterProvider); err != nil {
			return nil, errors.Join(fmt.Errorf("initialize telemetry metrics: %w", err), runtime.close())
		}
	}
	return runtime, nil
}

func (r *telemetryRuntime) close() error {
	if r == nil {
		return nil
	}
	var err error
	if r.meterShutdown != nil {
		err = errors.Join(err, r.meterShutdown(context.Background()))
	}
	if r.traceCleanup != nil {
		err = errors.Join(err, r.traceCleanup())
	}
	return err
}

func telemetryConfigured(getenv func(string) string) bool {
	if getenv == nil {
		return false
	}
	return telemetryEndpointConfigured(getenv, envOTELCollectorEndpoint) ||
		telemetryEndpointConfigured(getenv, envOTELTracesEndpoint) ||
		telemetryEndpointConfigured(getenv, envOTELMetricsEndpoint)
}

func telemetryEndpointConfigured(getenv func(string) string, name string) bool {
	return getenv != nil && strings.TrimSpace(getenv(name)) != ""
}

func telemetryProtocol(getenv func(string) string) string {
	if getenv == nil {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(getenv(envOTELProtocol))) {
	case telemetryProtocolHTTP, telemetryProtocolHTTPProtobuf:
		return telemetryProtocolHTTP
	case telemetryProtocolGRPC, telemetryProtocolGRPCProtobuf:
		return telemetryProtocolGRPC
	default:
		return ""
	}
}
