// Package log configures log levels and redaction for secrets.
package log

import "github.com/liuzengh/trpc-agent-service/trpcservice/tenant"

// RoutingFields returns the allowlisted tenant routing fields safe for logs.
// It excludes user principals, messages, credentials, and tool arguments.
func RoutingFields(tc tenant.RuntimeContext) map[string]string {
	fields := map[string]string{
		"tenant_id":      tc.TenantID,
		"app_id":         tc.AppID,
		"config_version": tc.ConfigVersion,
		"session_id":     tc.SessionID,
		"trace_id":       tc.TraceID,
	}
	if tc.Channel != "" {
		fields["channel"] = tc.Channel
	}
	if tc.BindingID != "" {
		fields["binding_id"] = tc.BindingID
	}
	return fields
}
