package gateway_test

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
)

type captureAdmitter struct {
	request gateway.AdmissionRequest
	result  gateway.AdmissionResult
	err     error
}

func (a *captureAdmitter) Admit(_ context.Context, request gateway.AdmissionRequest) (gateway.AdmissionResult, error) {
	a.request = request
	if a.err != nil {
		return gateway.AdmissionResult{}, a.err
	}
	return a.result, nil
}

type staticTenantResolver struct {
	tenant       tenant.RuntimeContext
	source       gateway.TenantSource
	identity     gateway.AdmissionIdentity
	withIdentity bool
}

type tenantOnlyResolver struct {
	tenant tenant.RuntimeContext
}

func (r tenantOnlyResolver) ResolveTenant(_ context.Context) (
	tenant.RuntimeContext,
	gateway.TenantSource,
	error,
) {
	return r.tenant, gateway.TenantSourceAuthenticatedClaims, nil
}

func (r staticTenantResolver) ResolveTenant(_ context.Context) (
	tenant.RuntimeContext,
	gateway.TenantSource,
	error,
) {
	return r.tenant, r.source, nil
}

func (r staticTenantResolver) ResolveAdmissionIdentity(
	_ context.Context,
) (gateway.AdmissionIdentity, error) {
	if !r.withIdentity {
		return gateway.AdmissionIdentity{}, errors.New("admission identity is unavailable")
	}
	return r.identity, nil
}

func TestGatewaySubmitsAtomicAdmission(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v2",
			TurnSeq:       7,
		},
	}
	gw := gateway.New(gateway.WithAdmitter(admitter))
	identity := validAdmissionIdentity()

	result, err := gw.Handle(context.Background(), gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
		Message: gateway.Message{Text: "hello"},
	})
	if err != nil {
		t.Fatalf("handle request: %v", err)
	}
	if result.RequestID != "request-1" || result.ConfigVersion != "v2" || result.TurnSeq != 7 {
		t.Fatalf("admission result = %#v", result)
	}
	if admitter.request.RequestID != "request-1" || admitter.request.IdempotencyKey != "client-key-1" {
		t.Fatalf("admission request = %#v", admitter.request)
	}
	if admitter.request.Identity.SourceID != identity.SourceID {
		t.Fatalf("admission source ID = %q, want %q", admitter.request.Identity.SourceID, identity.SourceID)
	}
}

func TestGatewayRequiresAdmitter(t *testing.T) {
	_, err := gateway.New().Handle(context.Background(), gateway.Request{})
	if !errors.Is(err, gateway.ErrAdmitterRequired) {
		t.Fatalf("handle error = %v, want admitter required", err)
	}
}

func TestGatewayRequiresAdmissionIdentity(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant:         tenantOnlyResolver{tenant: validRuntimeContext()},
	}
	_, err := gateway.New(gateway.WithAdmitter(admitter)).Handle(context.Background(), request)
	if !errors.Is(err, gateway.ErrAdmissionIdentityRequired) {
		t.Fatalf("handle error = %v, want admission identity required", err)
	}
	if admitter.request.RequestID != "" {
		t.Fatal("request reached admitter without admission identity")
	}
}

func TestGatewayRejectsInvalidAdmissionRequestBeforeBackend(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	identity := validAdmissionIdentity()
	identity.Tenant.UserID = ""
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
	}
	_, err := gateway.New(gateway.WithAdmitter(admitter)).Handle(context.Background(), request)
	if err == nil {
		t.Fatal("handle request succeeded with incomplete identity")
	}
	if admitter.request.RequestID != "" {
		t.Fatal("invalid request reached admitter")
	}
}

func TestGatewayRejectsUnsupportedMessageBeforeBackend(t *testing.T) {
	admitter := &captureAdmitter{
		result: gateway.AdmissionResult{
			RequestID:     "request-1",
			ConfigVersion: "v1",
			TurnSeq:       1,
		},
	}
	identity := validAdmissionIdentity()
	request := gateway.Request{
		RequestID:      "request-1",
		IdempotencyKey: "client-key-1",
		Tenant: staticTenantResolver{
			tenant:       identity.Tenant,
			source:       identity.Source,
			identity:     identity,
			withIdentity: true,
		},
		Message: gateway.Message{
			Text:         "hello",
			ArtifactRefs: []string{"artifact://file@1"},
		},
	}
	if _, err := gateway.New(gateway.WithAdmitter(admitter)).Handle(context.Background(), request); err == nil {
		t.Fatal("handle request succeeded with unsupported artifact refs")
	}
	if admitter.request.RequestID != "" {
		t.Fatal("unsupported message reached admitter")
	}
}

func validAdmissionIdentity() gateway.AdmissionIdentity {
	return gateway.AdmissionIdentity{
		Tenant:   validRuntimeContext(),
		Source:   gateway.TenantSourceAuthenticatedClaims,
		SourceID: "credential-1",
		CredentialDigest: gateway.CredentialDigest{
			1,
		},
	}
}

func validRuntimeContext() tenant.RuntimeContext {
	return tenant.RuntimeContext{
		TenantID:           "tenant-a",
		AppID:              "support",
		ConfigVersion:      "v1",
		SessionID:          "session-1",
		SessionPrincipalID: "principal-1",
		UserID:             "user-1",
		TraceID:            "trace-1",
	}
}
