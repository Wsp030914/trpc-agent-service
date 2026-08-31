package qdrant

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	qdrantclient "github.com/qdrant/go-client/qdrant"
)

func TestValidateBackend(t *testing.T) {
	valid := tenant.BackendRef{
		Kind:     tenant.BackendVector,
		Provider: providerName,
		Name:     "shared-qdrant",
		Options: map[string]string{
			optionEmbeddingModel:      "text-embedding-3-small",
			optionEmbeddingDimensions: "1536",
			optionEmbeddingProfile:    "text-embedding-3-small",
			optionIndexGeneration:     "g1",
		},
	}
	if err := ValidateBackend(valid); err != nil {
		t.Fatalf("validate backend: %v", err)
	}

	for name, mutate := range map[string]func(*tenant.BackendRef){
		"kind":     func(ref *tenant.BackendRef) { ref.Kind = tenant.BackendSQL },
		"provider": func(ref *tenant.BackendRef) { ref.Provider = "other" },
		"dimensions": func(ref *tenant.BackendRef) {
			ref.Options[optionEmbeddingDimensions] = "invalid"
		},
		"profile": func(ref *tenant.BackendRef) {
			ref.Options[optionEmbeddingProfile] = "invalid profile"
		},
		"generation": func(ref *tenant.BackendRef) {
			ref.Options[optionIndexGeneration] = "-g1"
		},
	} {
		t.Run(name, func(t *testing.T) {
			ref := valid
			ref.Options = make(map[string]string, len(valid.Options))
			for key, value := range valid.Options {
				ref.Options[key] = value
			}
			mutate(&ref)
			if err := ValidateBackend(ref); err == nil {
				t.Fatal("ValidateBackend() error = nil")
			}
		})
	}
}

func TestEnsureScopedPayloadIndexesOnce(t *testing.T) {
	t.Parallel()
	resolver := &Resolver{indexBootstraps: make(map[string]*indexBootstrap)}
	client := &payloadIndexClientStub{schema: make(map[string]*qdrantclient.PayloadSchemaInfo)}
	endpoint := Endpoint{Host: "qdrant", Port: 6334}

	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := resolver.ensureScopedPayloadIndexes(context.Background(), client, endpoint, "knowledge-profile-g1"); err != nil {
				t.Errorf("ensure indexes: %v", err)
			}
		}()
	}
	wait.Wait()
	if client.createCalls != len(scopedPayloadFields) {
		t.Fatalf("create calls = %d, want %d", client.createCalls, len(scopedPayloadFields))
	}
}

func TestEnsureScopedPayloadIndexesLeaderCancellationDoesNotPoisonWaiter(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver := &Resolver{
		lifecycle:       lifecycle,
		indexBootstraps: make(map[string]*indexBootstrap),
	}
	client := newBlockingPayloadIndexClient()
	leaderCtx, leaderCancel := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)
	go func() {
		leaderResult <- resolver.ensureScopedPayloadIndexes(leaderCtx, client, Endpoint{Host: "qdrant", Port: 6334}, "knowledge-profile-g1")
	}()
	<-client.started
	leaderCancel()
	waiterResult := make(chan error, 1)
	go func() {
		waiterResult <- resolver.ensureScopedPayloadIndexes(context.Background(), client, Endpoint{Host: "qdrant", Port: 6334}, "knowledge-profile-g1")
	}()
	close(client.release)
	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader error = %v, want context canceled", err)
	}
	select {
	case err := <-waiterResult:
		if err != nil {
			t.Fatalf("waiter error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not finish")
	}
}

func TestEnsureScopedPayloadIndexesResolverCloseCancelsBootstrap(t *testing.T) {
	lifecycle, cancel := context.WithCancel(context.Background())
	resolver := &Resolver{
		lifecycle:       lifecycle,
		cancel:          cancel,
		indexBootstraps: make(map[string]*indexBootstrap),
	}
	client := newBlockingPayloadIndexClient()
	result := make(chan error, 1)
	go func() {
		result <- resolver.ensureScopedPayloadIndexes(context.Background(), client, Endpoint{Host: "qdrant", Port: 6334}, "knowledge-profile-g1")
	}()
	<-client.started
	if err := resolver.Close(); err != nil {
		t.Fatalf("close resolver: %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("bootstrap error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("bootstrap did not stop after resolver close")
	}
}

func TestEnsureScopedPayloadIndexesAcceptsConvergedCreateRace(t *testing.T) {
	t.Parallel()
	client := &payloadIndexClientStub{
		schema:    make(map[string]*qdrantclient.PayloadSchemaInfo),
		createErr: errors.New("already exists"),
	}
	if err := ensureScopedPayloadIndexes(context.Background(), client, "knowledge-profile-g1"); err != nil {
		t.Fatalf("ensure indexes after race: %v", err)
	}
}

func TestEnsureScopedPayloadIndexesRejectsWrongSchema(t *testing.T) {
	t.Parallel()
	client := &payloadIndexClientStub{schema: map[string]*qdrantclient.PayloadSchemaInfo{
		scopedPayloadFields[0]: {DataType: qdrantclient.PayloadSchemaType_Integer},
	}}
	if err := ensureScopedPayloadIndexes(context.Background(), client, "knowledge-profile-g1"); err == nil {
		t.Fatal("ensure indexes accepted non-keyword schema")
	}
}

type payloadIndexClientStub struct {
	mu          sync.Mutex
	schema      map[string]*qdrantclient.PayloadSchemaInfo
	createCalls int
	createErr   error
}

type blockingPayloadIndexClient struct {
	base    *payloadIndexClientStub
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingPayloadIndexClient() *blockingPayloadIndexClient {
	return &blockingPayloadIndexClient{
		base:    &payloadIndexClientStub{schema: make(map[string]*qdrantclient.PayloadSchemaInfo)},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *blockingPayloadIndexClient) GetCollectionInfo(
	ctx context.Context,
	collectionName string,
) (*qdrantclient.CollectionInfo, error) {
	c.once.Do(func() { close(c.started) })
	select {
	case <-c.release:
		return c.base.GetCollectionInfo(ctx, collectionName)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *blockingPayloadIndexClient) CreateFieldIndex(
	ctx context.Context,
	request *qdrantclient.CreateFieldIndexCollection,
) (*qdrantclient.UpdateResult, error) {
	return c.base.CreateFieldIndex(ctx, request)
}

func (c *payloadIndexClientStub) GetCollectionInfo(
	_ context.Context,
	_ string,
) (*qdrantclient.CollectionInfo, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	schema := make(map[string]*qdrantclient.PayloadSchemaInfo, len(c.schema))
	for key, value := range c.schema {
		schema[key] = value
	}
	return &qdrantclient.CollectionInfo{PayloadSchema: schema}, nil
}

func (c *payloadIndexClientStub) CreateFieldIndex(
	_ context.Context,
	request *qdrantclient.CreateFieldIndexCollection,
) (*qdrantclient.UpdateResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.createCalls++
	c.schema[request.FieldName] = &qdrantclient.PayloadSchemaInfo{DataType: qdrantclient.PayloadSchemaType_Keyword}
	return nil, c.createErr
}

func TestEndpointValidate(t *testing.T) {
	if err := (Endpoint{Host: "qdrant", Port: 6334}).Validate(); err != nil {
		t.Fatalf("validate endpoint: %v", err)
	}
	for _, endpoint := range []Endpoint{
		{Port: 6334},
		{Host: "qdrant"},
		{Host: "qdrant", Port: 65536},
	} {
		if err := endpoint.Validate(); err == nil {
			t.Fatalf("Validate(%#v) error = nil", endpoint)
		}
	}
}
