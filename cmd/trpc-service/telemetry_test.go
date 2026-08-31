package main

import "testing"

func TestTelemetryConfigured(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want bool
	}{
		{name: "unset", env: map[string]string{}},
		{name: "generic endpoint", env: map[string]string{envOTELCollectorEndpoint: "collector:4317"}, want: true},
		{name: "trace endpoint", env: map[string]string{envOTELTracesEndpoint: "collector:4317"}, want: true},
		{name: "metrics endpoint", env: map[string]string{envOTELMetricsEndpoint: "collector:4317"}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			getenv := func(name string) string { return tt.env[name] }
			if got := telemetryConfigured(getenv); got != tt.want {
				t.Fatalf("telemetryConfigured() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestTelemetryProtocol(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  string
	}{
		{value: "grpc", want: telemetryProtocolGRPC},
		{value: "grpc/protobuf", want: telemetryProtocolGRPC},
		{value: "http", want: telemetryProtocolHTTP},
		{value: "http/protobuf", want: telemetryProtocolHTTP},
		{value: "unknown"},
	} {
		t.Run(tt.value, func(t *testing.T) {
			getenv := func(name string) string {
				if name == envOTELProtocol {
					return tt.value
				}
				return ""
			}
			if got := telemetryProtocol(getenv); got != tt.want {
				t.Fatalf("telemetryProtocol() = %q, want %q", got, tt.want)
			}
		})
	}
}
