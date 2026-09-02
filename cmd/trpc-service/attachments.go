package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	"github.com/google/uuid"
	artifactcos "github.com/liuzengh/trpc-agent-service/trpcservice/artifact/cos"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/feishu"
	"github.com/liuzengh/trpc-agent-service/trpcservice/postgres"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
	"github.com/liuzengh/trpc-agent-service/trpcservice/tenant"
	frameworkartifact "trpc.group/trpc-go/trpc-agent-go/artifact"
)

const productionInboundMediaLimit int64 = 32 << 20

// productionMediaDownloader resolves media only through the verified binding
// selected by ChannelInput. WeCom uses its provider URL; Feishu uses the
// official message-resource API and the binding's app credentials.
type productionMediaDownloader struct {
	store   *postgres.Store
	secrets platformsecret.SecretProvider
	http    *http.Client
}

func (d productionMediaDownloader) Download(
	ctx context.Context,
	input channels.ChannelInput,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	if d.store == nil || d.secrets == nil {
		return channels.DownloadedMedia{}, errors.New("production media downloader is not initialized")
	}
	binding, err := d.store.ResolveBinding(ctx, input.TenantID, input.AppID, input.BindingID)
	if err != nil {
		return channels.DownloadedMedia{}, err
	}
	if binding.Status != channels.BindingActive || binding.BindingRevision != input.BindingRevision ||
		binding.Channel != input.Channel {
		return channels.DownloadedMedia{}, errors.New("media binding authorization changed")
	}
	switch input.Channel {
	case channels.ChannelWeCom:
		return d.downloadWeCom(ctx, media)
	case channels.ChannelFeishu:
		client, err := feishu.NewOutboundClient(ctx, d.secrets, binding.Snapshot())
		if err != nil {
			return channels.DownloadedMedia{}, err
		}
		return client.DownloadMediaForMessage(ctx, input.ExternalMessageID, media)
	default:
		return channels.DownloadedMedia{}, fmt.Errorf("unsupported media channel %q", input.Channel)
	}
}

func (d productionMediaDownloader) downloadWeCom(
	ctx context.Context,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	if err := media.Validate(); err != nil {
		return channels.DownloadedMedia{}, err
	}
	parsed, err := url.ParseRequestURI(media.Reference)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
		return channels.DownloadedMedia{}, errors.New("wecom media url is invalid")
	}
	client := d.http
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
	if err != nil {
		return channels.DownloadedMedia{}, errors.New("create wecom media request")
	}
	response, err := client.Do(request)
	if err != nil {
		return channels.DownloadedMedia{}, fmt.Errorf("download wecom media: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return channels.DownloadedMedia{}, fmt.Errorf("wecom media returned status %d", response.StatusCode)
	}
	if response.ContentLength > productionInboundMediaLimit {
		return channels.DownloadedMedia{}, errors.New("wecom media exceeds size limit")
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, productionInboundMediaLimit+1))
	if err != nil {
		return channels.DownloadedMedia{}, fmt.Errorf("read wecom media: %w", err)
	}
	if int64(len(data)) > productionInboundMediaLimit {
		return channels.DownloadedMedia{}, errors.New("wecom media exceeds size limit")
	}
	filename := path.Base(parsed.Path)
	if filename == "." || filename == "/" || filename == "" {
		filename = "attachment"
	}
	mimeType := strings.TrimSpace(strings.Split(response.Header.Get("Content-Type"), ";")[0])
	if mimeType == "" || mimeType == "application/octet-stream" {
		mimeType = http.DetectContentType(data)
	}
	if _, _, err := mime.ParseMediaType(mimeType); err != nil {
		mimeType = "application/octet-stream"
	}
	return channels.DownloadedMedia{Filename: filename, MIMEType: mimeType, Data: data}, nil
}

type productionInboundArtifactWriter struct {
	store    *postgres.Store
	resolver *artifactcos.Resolver
}

func (w productionInboundArtifactWriter) WriteInboundArtifact(
	ctx context.Context,
	input channels.InboundArtifact,
) (string, error) {
	if w.store == nil || w.resolver == nil {
		return "", errors.New("production artifact writer is not initialized")
	}
	if err := input.Validate(); err != nil {
		return "", err
	}
	app, err := w.store.ResolveAgentApp(ctx, input.TenantID, input.AppID)
	if err != nil {
		return "", err
	}
	config, err := w.store.ResolveAppConfig(ctx, input.TenantID, input.AppID, app.ActiveConfigVersion)
	if err != nil {
		return "", err
	}
	ref := config.BackendConfig.Artifact
	if ref.IsZero() {
		return "", errors.New("artifact backend is required for inbound media")
	}
	objectStore, err := w.resolver.ResolveInboundStore(
		ctx,
		tenant.Scope{TenantID: input.TenantID, AppID: input.AppID},
		app.ActiveConfigVersion,
		ref,
	)
	if err != nil {
		return "", err
	}
	objectID := uuid.NewString()
	artifactName := "inbound/" + objectID
	artifactRef := "artifact://" + artifactName + "@0"
	objectKey, err := objectStore.Put(ctx, objectID, &frameworkartifact.Artifact{
		Data:     input.Data,
		MimeType: input.MIMEType,
		Name:     input.Filename,
	})
	if err != nil {
		return "", err
	}
	staged, err := w.store.StageInboundArtifact(ctx, postgres.StagedInboundArtifact{
		TenantID:          input.TenantID,
		AppID:             input.AppID,
		BindingID:         input.BindingID,
		ExternalMessageID: input.ExternalMessageID,
		ItemNo:            input.ItemNo,
		ArtifactRef:       artifactRef,
		ConfigVersion:     app.ActiveConfigVersion,
		Filename:          artifactName,
		ObjectKey:         objectKey,
		MIMEType:          input.MIMEType,
		Size:              int64(len(input.Data)),
	})
	if err != nil {
		return "", errors.Join(err, objectStore.Delete(context.WithoutCancel(ctx), objectKey))
	}
	if staged.ObjectKey != objectKey {
		if deleteErr := objectStore.Delete(context.WithoutCancel(ctx), objectKey); deleteErr != nil {
			return "", errors.Join(errors.New("inbound artifact idempotency object mismatch"), deleteErr)
		}
	}
	return staged.ArtifactRef, nil
}

func newProductionAttachmentIngestor(
	store *postgres.Store,
	secrets platformsecret.SecretProvider,
) (channels.AttachmentIngestor, error) {
	if store == nil || secrets == nil {
		return nil, errors.New("attachment ingestor dependencies are required")
	}
	resolver, err := artifactcos.NewResolver(
		secrets,
		environmentCOSEndpointResolver{getenv: os.Getenv},
	)
	if err != nil {
		return nil, err
	}
	ingestor, err := channels.NewArtifactIngestor(
		productionMediaDownloader{store: store, secrets: secrets},
		productionInboundArtifactWriter{store: store, resolver: resolver},
	)
	if err != nil {
		return nil, err
	}
	return ingestor, nil
}
