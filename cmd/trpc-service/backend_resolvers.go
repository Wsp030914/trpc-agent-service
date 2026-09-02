package main

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	knowledgeqdrant "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge/qdrant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

type environmentSecretProvider struct {
	getenv func(string) string
}

type environmentCOSEndpointResolver struct {
	getenv func(string) string
}

type environmentQdrantEndpointResolver struct {
	getenv func(string) string
}

type environmentTencentDBGatewayResolver struct {
	getenv func(string) string
}

func (r environmentTencentDBGatewayResolver) ResolveTencentDBGateway(
	ctx context.Context,
	backendName string,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if backendName == "" {
		return "", errors.New("tencentdb memory backend name is required")
	}
	if r.getenv == nil {
		return "", errors.New("environment reader is required")
	}
	var gateways map[string]string
	if err := json.Unmarshal([]byte(r.getenv(envTencentDBGateways)), &gateways); err != nil {
		return "", fmt.Errorf("decode %s: %w", envTencentDBGateways, err)
	}
	gatewayURL := strings.TrimSpace(gateways[backendName])
	if gatewayURL == "" {
		return "", fmt.Errorf("tencentdb memory gateway is not configured for backend %q", backendName)
	}
	return gatewayURL, nil
}

func (r environmentQdrantEndpointResolver) ResolveQdrantEndpoint(
	ctx context.Context,
	backendName string,
) (knowledgeqdrant.Endpoint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return knowledgeqdrant.Endpoint{}, err
	}
	if backendName == "" {
		return knowledgeqdrant.Endpoint{}, errors.New("qdrant backend name is required")
	}
	if r.getenv == nil {
		return knowledgeqdrant.Endpoint{}, errors.New("environment reader is required")
	}
	var endpoints map[string]knowledgeqdrant.Endpoint
	if err := json.Unmarshal([]byte(r.getenv(envQdrantEndpoints)), &endpoints); err != nil {
		return knowledgeqdrant.Endpoint{}, fmt.Errorf("decode %s: %w", envQdrantEndpoints, err)
	}
	endpoint, ok := endpoints[backendName]
	if !ok {
		return knowledgeqdrant.Endpoint{}, fmt.Errorf("qdrant endpoint is not configured for backend %q", backendName)
	}
	return endpoint, nil
}

func (r environmentCOSEndpointResolver) ResolveCOSEndpoint(
	ctx context.Context,
	backendName string,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if backendName == "" {
		return "", errors.New("cos backend name is required")
	}
	if r.getenv == nil {
		return "", errors.New("environment reader is required")
	}
	var endpoints map[string]string
	if err := json.Unmarshal([]byte(r.getenv(envCOSEndpoints)), &endpoints); err != nil {
		return "", fmt.Errorf("decode %s: %w", envCOSEndpoints, err)
	}
	endpoint := endpoints[backendName]
	if endpoint == "" {
		return "", fmt.Errorf("cos endpoint is not configured for backend %q", backendName)
	}
	return endpoint, nil
}

func (p environmentSecretProvider) ResolveSecret(
	ctx context.Context,
	scope tenant.Scope,
	ref tenant.SecretRef,
) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := scope.Validate(); err != nil {
		return "", err
	}
	if err := ref.Validate(); err != nil {
		return "", err
	}
	if p.getenv == nil {
		return "", errors.New("environment reader is required")
	}
	value := p.getenv(scopedSecretEnvironmentKey(scope, ref))
	if value == "" {
		return "", errors.New("scoped secret is not configured")
	}
	return value, nil
}

func scopedSecretEnvironmentKey(scope tenant.Scope, ref tenant.SecretRef) string {
	parts := []string{
		"TRPC_AGENT_SERVICE_SECRET",
		hex.EncodeToString([]byte(scope.TenantID)),
		hex.EncodeToString([]byte(scope.AppID)),
		hex.EncodeToString([]byte(ref.Name)),
		hex.EncodeToString([]byte(ref.Version)),
	}
	return strings.Join(parts, "_")
}

// defaultEndpointPolicy is the production model endpoint policy: it allows
// operator-configured https endpoints and blocks addresses that enable
// server-side request forgery (loopback, link-local, multicast, unspecified).
type defaultEndpointPolicy struct{}

func (defaultEndpointPolicy) ResolveModelBaseURL(
	_ context.Context,
	_ worker.Execution,
	configuredURL string,
) (string, error) {
	parsed, err := url.Parse(configuredURL)
	if err != nil {
		return "", fmt.Errorf("parse model base url: %w", err)
	}
	host := parsed.Hostname()
	if parsed.Scheme != "https" || host == "" {
		return "", errors.New("model base url must use https")
	}
	if ip := net.ParseIP(host); ip != nil {
		if blockedModelEndpointIP(ip) {
			return "", errors.New("model base url resolves to a blocked address")
		}
		return configuredURL, nil
	}
	addresses, err := net.LookupIP(host)
	if err != nil {
		return "", fmt.Errorf("resolve model base url host: %w", err)
	}
	for _, address := range addresses {
		if blockedModelEndpointIP(address) {
			return "", errors.New("model base url resolves to a blocked address")
		}
	}
	return configuredURL, nil
}

func blockedModelEndpointIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}
