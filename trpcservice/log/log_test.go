package log_test

import (
	"reflect"
	"testing"

	platformlog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

func TestRoutingFieldsAllowlistsNonSensitiveValues(t *testing.T) {
	tc := tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		Channel:            "wecom",
		BindingID:          "binding-1",
		SessionID:          "session-1",
		SessionPrincipalID: "private-principal",
		UserID:             "private-user",
		TraceID:            "trace-1",
	}

	got := platformlog.RoutingFields(tc)
	want := map[string]string{
		"tenant_id":      "tenant-a",
		"app_id":         "support",
		"config_version": "v1",
		"channel":        "wecom",
		"binding_id":     "binding-1",
		"session_id":     "session-1",
		"trace_id":       "trace-1",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("routing fields = %#v, want %#v", got, want)
	}
}
