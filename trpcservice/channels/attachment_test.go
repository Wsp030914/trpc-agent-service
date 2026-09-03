package channels_test

import (
	"context"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

func TestArtifactIngestorMaterializesOnlyArtifactRefs(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID:          "tenant-a",
		AppID:             "support",
		Channel:           channels.ChannelWeCom,
		BindingID:         "binding-1",
		BindingRevision:   1,
		ExternalMessageID: "message-1",
		Conversation:      channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:       channels.MessageTypeImage,
	}, channels.ChannelMappingInput{
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "user-1",
	})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}

	writer := &recordingArtifactWriter{ref: "artifact://inbound/one"}
	ingestor, err := channels.NewArtifactIngestor(
		recordingMediaDownloader{download: func(_ context.Context, got channels.ChannelInput, media channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
			if got.ArtifactRefs != nil {
				t.Fatalf("downloader received pre-existing artifact refs: %#v", got.ArtifactRefs)
			}
			if media.Reference != "provider-media-1" {
				t.Fatalf("media reference = %q", media.Reference)
			}
			return channels.DownloadedMedia{Filename: "image.png", MIMEType: "image/png", Data: []byte("image")}, nil
		}},
		writer,
	)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}

	prepared, err := ingestor.Prepare(context.Background(), input, []channels.ProviderMediaRef{{
		Kind:      channels.MessageTypeImage,
		Reference: "provider-media-1",
	}})
	if err != nil {
		t.Fatalf("prepare artifact: %v", err)
	}
	if len(prepared.ArtifactRefs) != 1 || prepared.ArtifactRefs[0] != writer.ref {
		t.Fatalf("artifact refs = %#v", prepared.ArtifactRefs)
	}
	if string(writer.artifact.Data) != "image" || writer.artifact.Filename != "image.png" {
		t.Fatalf("written artifact = %#v", writer.artifact)
	}
}

func TestArtifactIngestorRejectsOversizedMediaBeforeWrite(t *testing.T) {
	input, err := channels.NewChannelInput(channels.ChannelInput{
		TenantID:          "tenant-a",
		AppID:             "support",
		Channel:           channels.ChannelFeishu,
		BindingID:         "binding-1",
		BindingRevision:   1,
		ExternalMessageID: "message-1",
		Conversation:      channels.ChannelConversation{Kind: channels.ConversationDirect},
		MessageType:       channels.MessageTypeFile,
	}, channels.ChannelMappingInput{
		ExternalSenderID:     "user-1",
		ProviderSenderTarget: "user-1",
	})
	if err != nil {
		t.Fatalf("new channel input: %v", err)
	}

	writer := &recordingArtifactWriter{ref: "artifact://inbound/one"}
	ingestor, err := channels.NewArtifactIngestor(
		recordingMediaDownloader{download: func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error) {
			return channels.DownloadedMedia{Data: []byte("12345")}, nil
		}},
		writer,
	)
	if err != nil {
		t.Fatalf("new artifact ingestor: %v", err)
	}
	if err := ingestor.WithMaxBytes(4); err != nil {
		t.Fatalf("set artifact limit: %v", err)
	}
	if _, err := ingestor.Prepare(context.Background(), input, []channels.ProviderMediaRef{{
		Kind:      channels.MessageTypeFile,
		Reference: "provider-media-1",
	}}); err == nil {
		t.Fatal("oversized media was accepted")
	}
	if writer.called {
		t.Fatal("writer was called for oversized media")
	}
}

type recordingArtifactWriter struct {
	ref      string
	artifact channels.InboundArtifact
	called   bool
}

type recordingMediaDownloader struct {
	download func(context.Context, channels.ChannelInput, channels.ProviderMediaRef) (channels.DownloadedMedia, error)
}

func (d recordingMediaDownloader) Download(
	ctx context.Context,
	input channels.ChannelInput,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	return d.download(ctx, input, media)
}

func (w *recordingArtifactWriter) WriteInboundArtifact(_ context.Context, artifact channels.InboundArtifact) (string, error) {
	w.called = true
	w.artifact = artifact
	return w.ref, nil
}

var _ channels.ArtifactWriter = (*recordingArtifactWriter)(nil)
