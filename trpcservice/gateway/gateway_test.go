package gateway_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type captureQueue struct {
	job gateway.Job
}

type captureRoutedQueue struct {
	job gateway.RoutedJob
}

type staticTenantResolver struct {
	tenant tenant.RuntimeContext
	source gateway.TenantSource
}

func (r staticTenantResolver) ResolveTenant(_ context.Context) (
	tenant.RuntimeContext,
	gateway.TenantSource,
	error,
) {
	return r.tenant, r.source, nil
}

func (q *captureQueue) Enqueue(_ context.Context, job gateway.Job) error {
	q.job = job
	return nil
}

func (q *captureRoutedQueue) EnqueueRouted(_ context.Context, job gateway.RoutedJob) error {
	q.job = job
	return nil
}

func TestGatewayCreatesTenantScopedJob(t *testing.T) {
	queue := &captureQueue{}
	gw := gateway.Gateway{Jobs: queue}
	artifactRefs := []string{"artifact-1"}

	req := gateway.Request{
		RequestID: "request-1",
		Tenant: staticTenantResolver{
			source: gateway.TenantSourceAuthenticatedClaims,
			tenant: tenant.RuntimeContext{
				TenantID:           "tenant-a",
				AppID:              "support",
				ConfigVersion:      "v1",
				SessionID:          "session-1",
				SessionPrincipalID: "principal-1",
				UserID:             "user-1",
			},
		},
		Message: gateway.Message{Text: "hello", ArtifactRefs: artifactRefs},
	}

	job, err := gw.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if queue.job.RequestID != req.RequestID {
		t.Fatalf("queued request ID = %q, want %q", queue.job.RequestID, req.RequestID)
	}
	if job.Tenant.TenantID != "tenant-a" {
		t.Fatalf("job tenant ID = %q, want tenant-a", job.Tenant.TenantID)
	}
	if job.TenantSource != gateway.TenantSourceAuthenticatedClaims {
		t.Fatalf("job tenant source = %q, want authenticated claims", job.TenantSource)
	}
	artifactRefs[0] = "mutated"
	if got := job.Message.ArtifactRefs[0]; got != "artifact-1" {
		t.Fatalf("job artifact ref = %q, want artifact-1", got)
	}
	job.Message.ArtifactRefs[0] = "returned-job-mutation"
	if got := queue.job.Message.ArtifactRefs[0]; got != "artifact-1" {
		t.Fatalf("queued artifact ref = %q, want artifact-1", got)
	}
	key, err := job.PartitionKey()
	if err != nil {
		t.Fatalf("partition key: %v", err)
	}
	const want = "tenant:tenant-a:app:support:session:session-1"
	if key != want {
		t.Fatalf("partition key = %q, want %q", key, want)
	}
}

func TestNewJobAcceptsOnlyTrustedTenantSources(t *testing.T) {
	if _, err := gateway.NewJob(context.Background(), gateway.Request{RequestID: "request-1"}); err == nil {
		t.Fatal("new job succeeded without tenant resolver")
	}

	for _, source := range []gateway.TenantSource{
		gateway.TenantSourceAuthenticatedClaims,
		gateway.TenantSourceVerifiedChannelBinding,
	} {
		req := testRequest("request-1", "tenant-a", "support", "session-1")
		resolver := req.Tenant.(staticTenantResolver)
		resolver.source = source
		if source == gateway.TenantSourceVerifiedChannelBinding {
			resolver.tenant.Channel = "wecom"
			resolver.tenant.BindingID = "binding-1"
		}
		req.Tenant = resolver
		if _, err := gateway.NewJob(context.Background(), req); err != nil {
			t.Fatalf("new job with tenant source %q: %v", source, err)
		}
	}

	for _, source := range []gateway.TenantSource{"", "external_payload"} {
		req := testRequest("request-1", "tenant-a", "support", "session-1")
		resolver := req.Tenant.(staticTenantResolver)
		resolver.source = source
		req.Tenant = resolver
		if _, err := gateway.NewJob(context.Background(), req); err == nil {
			t.Fatalf("new job succeeded with tenant source %q", source)
		}
	}

	req := testRequest("request-1", "tenant-a", "support", "session-1")
	resolver := req.Tenant.(staticTenantResolver)
	resolver.source = gateway.TenantSourceVerifiedChannelBinding
	req.Tenant = resolver
	if _, err := gateway.NewJob(context.Background(), req); err == nil {
		t.Fatal("new job succeeded with unverified channel binding metadata")
	}
}

func TestGatewayEnqueuesRoutedJobWithPartitionKey(t *testing.T) {
	queue := &captureRoutedQueue{}
	gw := gateway.Gateway{RoutedJobs: queue}
	req := testRequest("request-1", "tenant-a", "support", "session-1")

	job, err := gw.Handle(context.Background(), req)
	if err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if err := queue.job.Validate(); err != nil {
		t.Fatalf("validate routed job: %v", err)
	}
	const want = "tenant:tenant-a:app:support:session:session-1"
	if queue.job.PartitionKey != want {
		t.Fatalf("routed partition key = %q, want %q", queue.job.PartitionKey, want)
	}
	if queue.job.Job.RequestID != job.RequestID {
		t.Fatalf("routed request ID = %q, want %q", queue.job.Job.RequestID, job.RequestID)
	}
}

func TestRoutedJobRejectsMismatchedPartitionKey(t *testing.T) {
	job, err := gateway.NewJob(
		context.Background(),
		testRequest("request-1", "tenant-a", "support", "session-1"),
	)
	if err != nil {
		t.Fatalf("new job: %v", err)
	}
	routed := gateway.RoutedJob{
		Job:          job,
		PartitionKey: "tenant:tenant-b:app:support:session:session-1",
	}

	if err := routed.Validate(); err == nil {
		t.Fatal("validate routed job succeeded with mismatched partition key")
	}
}

func TestJobPartitionKeyIsolatesTenantAppAndSession(t *testing.T) {
	base, err := gateway.NewJob(
		context.Background(),
		testRequest("request-1", "tenant-a", "support", "session-1"),
	)
	if err != nil {
		t.Fatalf("new base job: %v", err)
	}
	baseKey, err := base.PartitionKey()
	if err != nil {
		t.Fatalf("base partition key: %v", err)
	}

	sameSession, err := gateway.NewJob(
		context.Background(),
		testRequest("request-2", "tenant-a", "support", "session-1"),
	)
	if err != nil {
		t.Fatalf("new same-session job: %v", err)
	}
	sameKey, err := sameSession.PartitionKey()
	if err != nil {
		t.Fatalf("same-session partition key: %v", err)
	}
	if sameKey != baseKey {
		t.Fatalf("same-session key = %q, want %q", sameKey, baseKey)
	}

	tests := []struct {
		name     string
		tenantID string
		appID    string
		session  string
	}{
		{name: "tenant", tenantID: "tenant-b", appID: "support", session: "session-1"},
		{name: "app", tenantID: "tenant-a", appID: "sales", session: "session-1"},
		{name: "session", tenantID: "tenant-a", appID: "support", session: "session-2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job, err := gateway.NewJob(
				context.Background(),
				testRequest("request-2", tt.tenantID, tt.appID, tt.session),
			)
			if err != nil {
				t.Fatalf("new job: %v", err)
			}
			key, err := job.PartitionKey()
			if err != nil {
				t.Fatalf("partition key: %v", err)
			}
			if key == baseKey {
				t.Fatalf("partition key = %q, want key isolated from %q", key, baseKey)
			}
		})
	}
}

func testRequest(requestID, tenantID, appID, sessionID string) gateway.Request {
	return gateway.Request{
		RequestID: requestID,
		Tenant: staticTenantResolver{
			source: gateway.TenantSourceAuthenticatedClaims,
			tenant: tenant.RuntimeContext{
				TenantID:           tenantID,
				AppID:              appID,
				ConfigVersion:      "v1",
				SessionID:          sessionID,
				SessionPrincipalID: "principal-1",
			},
		},
		Message: gateway.Message{Text: "hello"},
	}
}
