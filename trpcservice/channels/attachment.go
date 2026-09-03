package channels

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

const defaultMaxInboundArtifactBytes int64 = 32 << 20

// DownloadedMedia is the short-lived result of a provider media download.
// Its bytes must not be copied into ChannelInput, Gateway, or execution
// events.
type DownloadedMedia struct {
	Filename string
	MIMEType string
	Data     []byte
}

// InboundArtifact identifies one deterministic, tenant-scoped artifact write.
// The writer owns persistence and must make the external message/item tuple
// idempotent.
type InboundArtifact struct {
	TenantID          string
	AppID             string
	BindingID         string
	ExternalMessageID string
	ItemNo            int
	Kind              MessageType
	Filename          string
	MIMEType          string
	Data              []byte
}

// Validate checks the controlled artifact write request.
func (a InboundArtifact) Validate() error {
	if a.TenantID == "" || a.AppID == "" || a.BindingID == "" || a.ExternalMessageID == "" {
		return errors.New("inbound artifact scope and message are required")
	}
	if a.ItemNo < 0 {
		return errors.New("inbound artifact item number must not be negative")
	}
	if err := a.Kind.Validate(); err != nil {
		return err
	}
	if a.Kind == MessageTypeText || a.Kind == MessageTypeUnsupported {
		return errors.New("inbound artifact kind is invalid")
	}
	if len(a.Data) == 0 {
		return errors.New("inbound artifact data is required")
	}
	if strings.ContainsAny(a.Filename, "\r\n\x00") || strings.ContainsAny(a.MIMEType, "\r\n\x00") {
		return errors.New("inbound artifact metadata is invalid")
	}
	return nil
}

// MediaDownloader resolves a provider media reference within the already
// verified Binding scope.
type MediaDownloader interface {
	Download(context.Context, ChannelInput, ProviderMediaRef) (DownloadedMedia, error)
}

// ArtifactWriter stores one inbound artifact and returns a tenant-scoped
// ArtifactRef. Implementations should use the external message and ItemNo as
// an idempotency key.
type ArtifactWriter interface {
	WriteInboundArtifact(context.Context, InboundArtifact) (string, error)
}

// ArtifactIngestor materializes provider media before PostgreSQL admission.
// It is independent of a concrete object store; the supplied ArtifactWriter
// is the only persistence boundary.
type ArtifactIngestor struct {
	downloader MediaDownloader
	writer     ArtifactWriter
	maxBytes   int64
}

// NewArtifactIngestor creates the IM-07 pre-admission media pipeline.
func NewArtifactIngestor(downloader MediaDownloader, writer ArtifactWriter) (*ArtifactIngestor, error) {
	if downloader == nil || writer == nil {
		return nil, errors.New("media downloader and artifact writer are required")
	}
	return &ArtifactIngestor{
		downloader: downloader,
		writer:     writer,
		maxBytes:   defaultMaxInboundArtifactBytes,
	}, nil
}

// WithMaxBytes limits one inbound provider media item in memory.
func (i *ArtifactIngestor) WithMaxBytes(maxBytes int64) error {
	if i == nil {
		return errors.New("artifact ingestor is not initialized")
	}
	if maxBytes <= 0 {
		return errors.New("max inbound artifact bytes must be positive")
	}
	i.maxBytes = maxBytes
	return nil
}

// Prepare downloads and writes media, then returns ChannelInput containing
// only the resulting ArtifactRefs.
func (i *ArtifactIngestor) Prepare(
	ctx context.Context,
	input ChannelInput,
	media []ProviderMediaRef,
) (ChannelInput, error) {
	if i == nil || i.downloader == nil || i.writer == nil {
		return ChannelInput{}, errors.New("artifact ingestor is not initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return ChannelInput{}, err
	}
	if err := input.Validate(); err != nil {
		return ChannelInput{}, err
	}
	if len(media) == 0 {
		return input.Clone(), nil
	}
	prepared := input.Clone()
	for itemNo, reference := range media {
		if err := reference.Validate(); err != nil {
			return ChannelInput{}, fmt.Errorf("provider media %d: %w", itemNo, err)
		}
		downloaded, err := i.downloader.Download(ctx, input, reference)
		if err != nil {
			return ChannelInput{}, fmt.Errorf("download provider media %d: %w", itemNo, err)
		}
		if int64(len(downloaded.Data)) > i.maxBytes {
			return ChannelInput{}, fmt.Errorf("provider media %d exceeds size limit", itemNo)
		}
		artifactRef, err := i.writer.WriteInboundArtifact(ctx, InboundArtifact{
			TenantID:          input.TenantID,
			AppID:             input.AppID,
			BindingID:         input.BindingID,
			ExternalMessageID: input.ExternalMessageID,
			ItemNo:            itemNo,
			Kind:              reference.Kind,
			Filename:          downloaded.Filename,
			MIMEType:          downloaded.MIMEType,
			Data:              downloaded.Data,
		})
		if err != nil {
			return ChannelInput{}, fmt.Errorf("write inbound artifact %d: %w", itemNo, err)
		}
		if !strings.HasPrefix(artifactRef, "artifact://") || strings.TrimSpace(strings.TrimPrefix(artifactRef, "artifact://")) == "" {
			return ChannelInput{}, fmt.Errorf("inbound artifact %d returned invalid artifact ref", itemNo)
		}
		prepared.ArtifactRefs = append(prepared.ArtifactRefs, artifactRef)
	}
	return prepared, nil
}

var _ AttachmentIngestor = (*ArtifactIngestor)(nil)
