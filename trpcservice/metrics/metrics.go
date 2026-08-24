// Package metrics exposes tenant-aware OpenTelemetry metrics.
package metrics

import (
	"context"
	"errors"
)

// Component identifies a node in the platform topology.
type Component string

const (
	// ComponentGateway records Agent Gateway observations.
	ComponentGateway Component = "gateway"
	// ComponentWorker records Agent Worker observations.
	ComponentWorker Component = "worker"
	// ComponentChannelAdapter records IM Channel Adapter observations.
	ComponentChannelAdapter Component = "channel_adapter"
	// ComponentStorageAdapter records Storage Adapter observations.
	ComponentStorageAdapter Component = "storage_adapter"
	// ComponentAdminAPI records Admin API observations.
	ComponentAdminAPI Component = "admin_api"
	// ComponentTelemetryCollector records collector-side observations.
	ComponentTelemetryCollector Component = "telemetry_collector"
)

// Event is the minimal tenant-aware telemetry envelope shared by topology nodes.
type Event struct {
	TenantID  string
	AppID     string
	TraceID   string
	Component Component
	Operation string
}

// Validate checks that an event carries the scope needed for tenant telemetry.
func (e Event) Validate() error {
	if e.TenantID == "" {
		return errors.New("tenant_id is required")
	}
	if e.AppID == "" {
		return errors.New("app_id is required")
	}
	if e.TraceID == "" {
		return errors.New("trace_id is required")
	}
	if !validComponent(e.Component) {
		return errors.New("component is invalid")
	}
	if e.Operation == "" {
		return errors.New("operation is required")
	}
	return nil
}

// Collector records tenant-aware telemetry from platform nodes.
type Collector interface {
	Record(ctx context.Context, event Event) error
}

// NopCollector validates events without exporting them.
type NopCollector struct{}

// Record validates the event and drops it.
func (NopCollector) Record(_ context.Context, event Event) error {
	return event.Validate()
}

func validComponent(component Component) bool {
	switch component {
	case ComponentGateway,
		ComponentWorker,
		ComponentChannelAdapter,
		ComponentStorageAdapter,
		ComponentAdminAPI,
		ComponentTelemetryCollector:
		return true
	default:
		return false
	}
}
