// Package cos stores Knowledge source objects in the COS backend configured
// for the application's artifacts.
package cos

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"

	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
	cosclient "github.com/tencentyun/cos-go-sdk-v5"
)

// SecretProvider resolves scoped COS credentials. Values must not be persisted
// or logged by callers.
type SecretProvider interface {
	ResolveSecret(context.Context, tenant.Scope, tenant.SecretRef) (string, error)
}

// EndpointResolver resolves one operator-controlled COS endpoint by its
// logical backend name. It must not consume tenant-provided network addresses.
type EndpointResolver interface {
	ResolveCOSEndpoint(context.Context, string) (string, error)
}

// Resolver owns COS clients for trusted Knowledge source access.
type Resolver struct {
	secrets   SecretProvider
	endpoints EndpointResolver

	mu      sync.Mutex
	closed  bool
	clients map[string]*cosclient.Client
}

// NewResolver creates a Knowledge source resolver backed by the configured
// Artifact COS backend.
func NewResolver(secrets SecretProvider, endpoints EndpointResolver) (*Resolver, error) {
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if endpoints == nil {
		return nil, errors.New("cos endpoint resolver is required")
	}
	return &Resolver{
		secrets:   secrets,
		endpoints: endpoints,
		clients:   make(map[string]*cosclient.Client),
	}, nil
}

// NewDocument creates immutable metadata for one source payload. Its object
// key is generated from the trusted tenant and application scope.
func NewDocument(
	scope tenant.Scope,
	knowledgeBaseID string,
	documentID string,
	version int,
	content []byte,
	mimeType string,
	indexGeneration string,
) (platformknowledge.Document, error) {
	if err := scope.Validate(); err != nil {
		return platformknowledge.Document{}, err
	}
	if knowledgeBaseID == "" || documentID == "" || version < 0 || indexGeneration == "" {
		return platformknowledge.Document{}, errors.New("knowledge source identity is invalid")
	}
	digest := sha256.Sum256(content)
	document := platformknowledge.Document{
		Scope:           scope,
		KnowledgeBaseID: knowledgeBaseID,
		ID:              documentID,
		Version:         version,
		ObjectKey:       objectKey(scope, knowledgeBaseID, documentID, version, digest),
		ContentSHA256:   digest[:],
		MIMEType:        mimeType,
		Status:          platformknowledge.DocumentStatusAvailable,
		IndexGeneration: indexGeneration,
	}
	if err := document.Validate(); err != nil {
		return platformknowledge.Document{}, err
	}
	return document, nil
}

// PutSource writes a source object only at its generated object key. Callers
// must persist the returned Document metadata before making it indexable.
func (r *Resolver) PutSource(
	ctx context.Context,
	exec worker.Execution,
	document platformknowledge.Document,
	content []byte,
) error {
	if err := validateDocumentExecution(exec, document); err != nil {
		return err
	}
	if expected := objectKey(
		document.Scope,
		document.KnowledgeBaseID,
		document.ID,
		document.Version,
		sha256.Sum256(content),
	); document.ObjectKey != expected {
		return errors.New("knowledge source object key does not match content")
	}
	if !bytes.Equal(document.ContentSHA256, sha256Sum(content)) {
		return errors.New("knowledge source content hash does not match metadata")
	}
	client, err := r.resolveClient(ctx, exec)
	if err != nil {
		return err
	}
	_, err = client.Object.Put(ctx, document.ObjectKey, bytes.NewReader(content), &cosclient.ObjectPutOptions{
		ObjectPutHeaderOptions: &cosclient.ObjectPutHeaderOptions{ContentType: document.MIMEType},
	})
	if err != nil {
		return fmt.Errorf("put knowledge source: %w", err)
	}
	return nil
}

// GetSource verifies a document's immutable object identity before reading its
// content. It never accepts a caller-supplied object key.
func (r *Resolver) GetSource(
	ctx context.Context,
	exec worker.Execution,
	document platformknowledge.Document,
) (content []byte, err error) {
	if err := validateDocumentExecution(exec, document); err != nil {
		return nil, err
	}
	if expected := objectKey(
		document.Scope,
		document.KnowledgeBaseID,
		document.ID,
		document.Version,
		byteArray(document.ContentSHA256),
	); document.ObjectKey != expected {
		return nil, errors.New("knowledge source object key is invalid")
	}
	client, err := r.resolveClient(ctx, exec)
	if err != nil {
		return nil, err
	}
	response, err := client.Object.Get(ctx, document.ObjectKey, nil)
	if err != nil {
		return nil, fmt.Errorf("get knowledge source: %w", err)
	}
	defer func() {
		if closeErr := response.Body.Close(); err == nil && closeErr != nil {
			content = nil
			err = fmt.Errorf("close knowledge source: %w", closeErr)
		}
	}()
	content, err = io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("read knowledge source: %w", err)
	}
	if !bytes.Equal(document.ContentSHA256, sha256Sum(content)) {
		return nil, errors.New("knowledge source content hash mismatch")
	}
	return content, nil
}

// DeleteSource removes one generated source object. It is reserved for
// compensating a failed SQL metadata transaction; callers must never use it as
// an authorization decision.
func (r *Resolver) DeleteSource(
	ctx context.Context,
	exec worker.Execution,
	document platformknowledge.Document,
) error {
	if err := validateDocumentExecution(exec, document); err != nil {
		return err
	}
	if expected := objectKey(
		document.Scope,
		document.KnowledgeBaseID,
		document.ID,
		document.Version,
		byteArray(document.ContentSHA256),
	); document.ObjectKey != expected {
		return errors.New("knowledge source object key is invalid")
	}
	client, err := r.resolveClient(ctx, exec)
	if err != nil {
		return err
	}
	_, err = client.Object.Delete(ctx, document.ObjectKey)
	if err != nil {
		return fmt.Errorf("delete knowledge source: %w", err)
	}
	return nil
}

// Close releases cached client references. COS clients do not own a closeable
// connection, so callers may safely call Close multiple times.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	r.clients = nil
	return nil
}

func (r *Resolver) resolveClient(ctx context.Context, exec worker.Execution) (*cosclient.Client, error) {
	if r == nil || r.secrets == nil || r.endpoints == nil {
		return nil, errors.New("knowledge cos resolver is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return nil, err
	}
	ref := exec.Config.BackendConfig.Artifact
	if err := artifactcos.ValidateBackend(ref); err != nil {
		return nil, fmt.Errorf("knowledge source artifact backend: %w", err)
	}
	if err := exec.Storage.Artifact.Validate(exec.Tenant.Scope(), storage.CapabilityArtifact, ref); err != nil {
		return nil, fmt.Errorf("knowledge source artifact storage handle: %w", err)
	}
	endpoint, err := r.endpoints.ResolveCOSEndpoint(ctx, ref.Name)
	if err != nil {
		return nil, fmt.Errorf("resolve knowledge cos endpoint: %w", err)
	}
	parsed, err := parseEndpoint(endpoint)
	if err != nil {
		return nil, err
	}
	key, err := sourceClientKey(exec.Tenant.Scope(), exec.Tenant.ConfigVersion, ref, endpoint)
	if err != nil {
		return nil, err
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.New("knowledge cos resolver is closed")
	}
	if client := r.clients[key]; client != nil {
		r.mu.Unlock()
		return client, nil
	}
	r.mu.Unlock()

	credential, err := r.secrets.ResolveSecret(ctx, exec.Tenant.Scope(), ref.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("resolve knowledge cos credentials: %w", err)
	}
	credentials, err := parseCredentials(credential)
	if err != nil {
		return nil, err
	}
	client := cosclient.NewClient(&cosclient.BaseURL{BucketURL: parsed}, &http.Client{
		Transport: &cosclient.AuthorizationTransport{
			SecretID:  credentials.SecretID,
			SecretKey: credentials.SecretKey,
		},
	})
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, errors.New("knowledge cos resolver is closed")
	}
	if existing := r.clients[key]; existing != nil {
		return existing, nil
	}
	r.clients[key] = client
	return client, nil
}

func validateDocumentExecution(exec worker.Execution, document platformknowledge.Document) error {
	if err := document.Validate(); err != nil {
		return err
	}
	if err := exec.Tenant.Validate(); err != nil {
		return err
	}
	if document.Scope != exec.Tenant.Scope() {
		return errors.New("knowledge document scope does not match execution")
	}
	return nil
}

func parseEndpoint(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return nil, errors.New("operator cos endpoint must be an absolute https url")
	}
	return parsed, nil
}

type credentials struct {
	SecretID  string `json:"secret_id"`
	SecretKey string `json:"secret_key"`
}

func parseCredentials(value string) (credentials, error) {
	var result credentials
	decoder := json.NewDecoder(strings.NewReader(value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return credentials{}, fmt.Errorf("decode cos credentials: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return credentials{}, errors.New("decode cos credentials: multiple json values")
		}
		return credentials{}, fmt.Errorf("decode cos credentials: %w", err)
	}
	if result.SecretID == "" || result.SecretKey == "" {
		return credentials{}, errors.New("cos credentials require secret_id and secret_key")
	}
	return result, nil
}

func objectKey(scope tenant.Scope, baseID, documentID string, version int, digest [sha256.Size]byte) string {
	return strings.Join([]string{
		"knowledge",
		"v1",
		"tenant", storageSegment(scope.TenantID),
		"app", storageSegment(scope.AppID),
		"base", storageSegment(baseID),
		"document", storageSegment(documentID),
		"version", fmt.Sprintf("%d", version),
		hex.EncodeToString(digest[:]),
	}, "/")
}

func sourceClientKey(scope tenant.Scope, version string, ref tenant.BackendRef, endpoint string) (string, error) {
	return scope.Key("knowledge-source", version, ref.Provider, ref.Name, ref.SecretRef.Name, ref.SecretRef.Version, endpoint)
}

func storageSegment(value string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(value))
}

func sha256Sum(content []byte) []byte {
	digest := sha256.Sum256(content)
	return digest[:]
}

func byteArray(value []byte) [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], value)
	return result
}
