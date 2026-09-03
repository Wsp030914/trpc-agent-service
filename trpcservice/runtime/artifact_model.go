package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
	"trpc.group/trpc-go/trpc-agent-go/model"
)

// artifactHydratingModel keeps the durable model request reference-only while
// restoring bytes immediately before the provider call. This keeps provider
// media out of session events, execution commands, and logs.
type artifactHydratingModel struct {
	model.Model
	artifacts frameworkartifact.Service
	info      frameworkartifact.SessionInfo
}

func (m *artifactHydratingModel) GenerateContent(
	ctx context.Context,
	request *model.Request,
) (<-chan *model.Response, error) {
	hydratedRequest, err := cloneRequest(request)
	if err != nil {
		return nil, err
	}
	if err := m.hydrateRequest(ctx, hydratedRequest); err != nil {
		return nil, err
	}
	return m.Model.GenerateContent(ctx, hydratedRequest)
}

func cloneRequest(request *model.Request) (*model.Request, error) {
	if request == nil {
		return nil, errors.New("request cannot be nil")
	}
	clone := *request
	clone.Messages = append([]model.Message(nil), request.Messages...)
	for messageIndex := range clone.Messages {
		message := &clone.Messages[messageIndex]
		message.ContentParts = append([]model.ContentPart(nil), message.ContentParts...)
		for partIndex := range message.ContentParts {
			part := &message.ContentParts[partIndex]
			if part.ContentRef != nil {
				contentRef := *part.ContentRef
				part.ContentRef = &contentRef
			}
			if part.File != nil {
				file := *part.File
				file.Data = bytes.Clone(part.File.Data)
				part.File = &file
			}
			if part.Image != nil {
				image := *part.Image
				image.Data = bytes.Clone(part.Image.Data)
				part.Image = &image
			}
			if part.Audio != nil {
				audio := *part.Audio
				audio.Data = bytes.Clone(part.Audio.Data)
				part.Audio = &audio
			}
		}
	}
	return &clone, nil
}

func (m *artifactHydratingModel) hydrateRequest(ctx context.Context, request *model.Request) error {
	if request == nil {
		return errors.New("request cannot be nil")
	}
	if m == nil || m.Model == nil || m.artifacts == nil {
		return errors.New("artifact hydrating model is not initialized")
	}
	for messageIndex := range request.Messages {
		for partIndex := range request.Messages[messageIndex].ContentParts {
			part := &request.Messages[messageIndex].ContentParts[partIndex]
			if part.ContentRef == nil {
				continue
			}
			if err := m.hydratePart(ctx, part); err != nil {
				return fmt.Errorf("hydrate message %d content part %d: %w", messageIndex, partIndex, err)
			}
		}
	}
	return nil
}

func (m *artifactHydratingModel) hydratePart(ctx context.Context, part *model.ContentPart) error {
	name, version, err := parseArtifactRef(part.ContentRef)
	if err != nil {
		return err
	}
	artifactValue, err := m.artifacts.LoadArtifact(ctx, m.info, name, &version)
	if err != nil {
		return fmt.Errorf("load %s@%d: %w", name, version, err)
	}
	if artifactValue == nil {
		return fmt.Errorf("artifact not found: %s@%d", name, version)
	}
	if err := validateLoadedArtifact(part.ContentRef, artifactValue.Data); err != nil {
		return err
	}
	switch part.Type {
	case model.ContentTypeFile:
		file := part.File
		if file == nil {
			file = &model.File{}
		}
		file.Data = append(file.Data[:0], artifactValue.Data...)
		if file.Name == "" {
			file.Name = chooseArtifactName(part.ContentRef, name)
		}
		if file.MimeType == "" {
			file.MimeType = chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType)
		}
		part.File = file
	case model.ContentTypeImage:
		image := part.Image
		if image == nil {
			image = &model.Image{}
		}
		image.Data = append(image.Data[:0], artifactValue.Data...)
		if image.Format == "" {
			image.Format = chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType)
		}
		part.Image = image
	case model.ContentTypeAudio:
		audio := part.Audio
		if audio == nil {
			audio = &model.Audio{}
		}
		audio.Data = append(audio.Data[:0], artifactValue.Data...)
		if audio.Format == "" {
			audio.Format = chooseArtifactMimeType(part.ContentRef, artifactValue.MimeType)
		}
		part.Audio = audio
	default:
		return fmt.Errorf("unsupported content reference type %q", part.Type)
	}
	return nil
}

func parseArtifactRef(ref *model.ContentRef) (string, int, error) {
	if ref == nil {
		return "", 0, errors.New("content ref is required")
	}
	if ref.ArtifactName != "" {
		return gateway.ParseArtifactRef(
			"artifact://" + ref.ArtifactName + "@" + strconv.Itoa(ref.ArtifactVersion),
		)
	}
	return gateway.ParseArtifactRef(ref.ArtifactRef)
}

func validateLoadedArtifact(ref *model.ContentRef, data []byte) error {
	if ref.SizeBytes > 0 && int64(len(data)) != ref.SizeBytes {
		return errors.New("artifact size does not match content reference")
	}
	return nil
}

func chooseArtifactName(ref *model.ContentRef, fallback string) string {
	if ref.OriginalName != "" {
		return ref.OriginalName
	}
	if ref.ArtifactName != "" {
		return ref.ArtifactName
	}
	return fallback
}

func chooseArtifactMimeType(ref *model.ContentRef, fallback string) string {
	if ref.MimeType != "" {
		return ref.MimeType
	}
	return fallback
}
