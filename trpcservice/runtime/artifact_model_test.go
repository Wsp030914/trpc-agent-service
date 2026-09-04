package runtime

import (
	"context"
	"testing"

	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	artifactmemory "trpc.group/trpc-go/trpc-agent-go/artifact/inmemory"
	frameworkmodel "trpc.group/trpc-go/trpc-agent-go/model"
)

func TestArtifactHydratingModelLoadsReferenceBeforeProviderCall(t *testing.T) {
	storage := artifactmemory.NewService()
	info := frameworkartifact.SessionInfo{
		AppName:   "tenant-a/app-a/runner/v1",
		UserID:    "principal-a",
		SessionID: "session-a",
	}
	data := []byte("attachment contents")
	if _, err := storage.SaveArtifact(context.Background(), info, "inbound/file", &frameworkartifact.Artifact{
		Data:     data,
		Name:     "report.docx",
		MimeType: "text/plain",
	}); err != nil {
		t.Fatalf("save artifact: %v", err)
	}

	model := &capturingModel{}
	hydrating := &artifactHydratingModel{
		Model:     model,
		artifacts: storage,
		info:      info,
	}
	request := &frameworkmodel.Request{Messages: []frameworkmodel.Message{{
		Role: frameworkmodel.RoleUser,
		ContentParts: []frameworkmodel.ContentPart{{
			Type: frameworkmodel.ContentTypeFile,
			ContentRef: &frameworkmodel.ContentRef{
				ArtifactRef: "artifact://inbound/file@0",
				SizeBytes:   int64(len(data)),
				MimeType:    "text/plain",
			},
		}},
	}}}

	responses, err := hydrating.GenerateContent(context.Background(), request)
	if err != nil {
		t.Fatalf("generate content: %v", err)
	}
	if responses == nil {
		t.Fatal("generate content returned nil response channel")
	}
	if model.request == nil || len(model.request.Messages) != 1 || len(model.request.Messages[0].ContentParts) != 1 {
		t.Fatalf("provider request = %#v", model.request)
	}
	part := model.request.Messages[0].ContentParts[0]
	if part.File == nil || string(part.File.Data) != string(data) || part.File.Name != "report.docx" || part.File.MimeType != "text/plain" {
		t.Fatalf("hydrated file = %#v", part.File)
	}
	originalPart := request.Messages[0].ContentParts[0]
	if originalPart.File != nil || originalPart.ContentRef == nil {
		t.Fatalf("durable request was mutated with artifact bytes: %#v", originalPart)
	}
}

type capturingModel struct {
	request *frameworkmodel.Request
}

func (m *capturingModel) GenerateContent(_ context.Context, request *frameworkmodel.Request) (<-chan *frameworkmodel.Response, error) {
	m.request = request
	responses := make(chan *frameworkmodel.Response)
	close(responses)
	return responses, nil
}

func (*capturingModel) Info() frameworkmodel.Info {
	return frameworkmodel.Info{}
}

var _ frameworkmodel.Model = (*capturingModel)(nil)
