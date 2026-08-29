package importer

import (
	"context"
	"errors"
	"reflect"
	"testing"

	platformknowledge "github.com/liuzengh/trpc-agent-service/trpcservice/knowledge"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func TestImportWritesSourceThenMetadataAndJob(t *testing.T) {
	sources := &recordingSources{}
	repository := &recordingRepository{}
	importer, err := New(sources, repository)
	if err != nil {
		t.Fatalf("new importer: %v", err)
	}
	document, err := importer.Import(context.Background(), importExecution(), Input{
		KnowledgeBaseID: "handbook",
		DocumentID:      "employee-handbook",
		Version:         1,
		Content:         []byte("employee policy"),
		MIMEType:        "text/plain",
	})
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if sources.puts != 1 || sources.deletes != 0 {
		t.Fatalf("source calls = puts %d deletes %d", sources.puts, sources.deletes)
	}
	if !reflect.DeepEqual(repository.document, document) || !reflect.DeepEqual(repository.job.Document, document) || repository.job.ConfigVersion != "v2" {
		t.Fatalf("repository values = %#v, %#v", repository.document, repository.job)
	}
}

func TestImportCompensatesSourceWhenMetadataFails(t *testing.T) {
	sources := &recordingSources{}
	importer, err := New(sources, &recordingRepository{err: errors.New("database unavailable")})
	if err != nil {
		t.Fatalf("new importer: %v", err)
	}
	_, err = importer.Import(context.Background(), importExecution(), Input{
		KnowledgeBaseID: "handbook",
		DocumentID:      "employee-handbook",
		Version:         1,
		Content:         []byte("employee policy"),
		MIMEType:        "text/plain",
	})
	if err == nil {
		t.Fatal("Import() error = nil")
	}
	if sources.puts != 1 || sources.deletes != 1 {
		t.Fatalf("source calls = puts %d deletes %d", sources.puts, sources.deletes)
	}
}

type recordingSources struct {
	puts    int
	deletes int
}

func (s *recordingSources) PutSource(context.Context, worker.Execution, platformknowledge.Document, []byte) error {
	s.puts++
	return nil
}

func (s *recordingSources) DeleteSource(context.Context, worker.Execution, platformknowledge.Document) error {
	s.deletes++
	return nil
}

type recordingRepository struct {
	document platformknowledge.Document
	job      platformknowledge.IndexJob
	err      error
}

func (r *recordingRepository) CreateKnowledgeDocumentAndEnqueue(
	_ context.Context,
	document platformknowledge.Document,
	job platformknowledge.IndexJob,
) error {
	r.document = document
	r.job = job
	return r.err
}

func importExecution() worker.Execution {
	return worker.Execution{
		Tenant: tenant.RuntimeContext{
			TenantID:           "tenant-a",
			AppID:              "knowledge",
			ConfigVersion:      "v2",
			SessionPrincipalID: "user-a",
			SessionID:          "session-a",
			UserID:             "user-a",
			TraceID:            "trace-a",
		},
		Config: tenant.AppConfig{
			TenantID:         "tenant-a",
			AppID:            "knowledge",
			Version:          "v2",
			KnowledgeBaseIDs: []string{"handbook"},
			BackendConfig: tenant.BackendConfig{
				Knowledge: tenant.BackendRef{Options: map[string]string{indexGenerationOption: "g1"}},
				Artifact:  tenant.BackendRef{Kind: tenant.BackendObject, Name: "cos"},
			},
		},
	}
}
