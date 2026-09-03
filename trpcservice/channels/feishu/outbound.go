package feishu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	platformsecret "github.com/liuzengh/trpc-agent-service/trpcservice/secret"
)

const (
	maxFeishuReplyTextBytes    = 150 << 10
	maxFeishuInboundMediaBytes = 32 << 20
)

var (
	errFeishuReplyTooLarge          = errors.New("feishu reply is too large")
	errFeishuReplyUnsupported       = errors.New("feishu reply is unsupported")
	errFeishuOutboundNotInitialized = errors.New("feishu outbound client is not initialized")
	errFeishuProviderMessageID      = errors.New("feishu provider message id is missing")
)

// ProviderSendError is the stable, body-redacted error returned by one
// Feishu provider call. Reply Outbox retry policy remains owned by IM-06.
type ProviderSendError struct {
	StatusCode int
	Code       int
	Retryable  bool
	cause      error
}

// Error returns a provider-safe description without response bodies, targets,
// credentials, or other provider payload data.
func (e *ProviderSendError) Error() string {
	if e == nil {
		return "feishu provider send failed"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("feishu provider returned status %d", e.StatusCode)
	}
	if e.Code != 0 {
		return fmt.Sprintf("feishu provider returned code %d", e.Code)
	}
	return "feishu provider send failed"
}

// Unwrap returns the non-sensitive transport or decoding cause, if present.
func (e *ProviderSendError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.cause
}

// IsRetryable reports whether the provider or transport indicated a temporary
// failure. ReplySender owns the actual retry decision and attempt limit.
func (e *ProviderSendError) IsRetryable() bool {
	return e != nil && e.Retryable
}

// OutboundOption configures a Feishu one-call provider client.
type OutboundOption func(*outboundConfig) error

type outboundConfig struct {
	httpClient *http.Client
	baseURL    string
}

// WithHTTPClient supplies the HTTP client used by the official Feishu SDK.
// It is useful for transport policy and fake-provider tests.
func WithHTTPClient(client *http.Client) OutboundOption {
	return func(config *outboundConfig) error {
		if client == nil {
			return errors.New("http client is required")
		}
		config.httpClient = client
		return nil
	}
}

// WithBaseURL overrides both Feishu API base URLs. Production callers should
// leave it unset; tests use it to point the official SDK at a fake provider.
func WithBaseURL(baseURL string) OutboundOption {
	return func(config *outboundConfig) error {
		parsed, err := url.ParseRequestURI(baseURL)
		if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" {
			return errors.New("feishu base url is invalid")
		}
		config.baseURL = strings.TrimRight(baseURL, "/")
		return nil
	}
}

// OutboundClient performs exactly one Feishu provider operation. It does not
// claim, persist, retry, or otherwise manage Reply Outbox state.
type OutboundClient struct {
	client *lark.Client
}

// NewOutboundClient creates a Feishu client backed by the binding's scoped
// App Secret. Binding.Secret is never returned or logged.
func NewOutboundClient(
	ctx context.Context,
	secrets platformsecret.SecretProvider,
	binding channels.BindingSnapshot,
	opts ...OutboundOption,
) (*OutboundClient, error) {
	if ctx == nil {
		return nil, errors.New("context is required")
	}
	if secrets == nil {
		return nil, errors.New("secret provider is required")
	}
	if err := binding.Validate(); err != nil {
		return nil, fmt.Errorf("feishu binding: %w", err)
	}
	if binding.Channel != channels.ChannelFeishu {
		return nil, errors.New("feishu outbound client received another channel")
	}
	if binding.ExternalAccountScope == "" {
		return nil, errors.New("feishu external account scope is required")
	}
	if binding.Secret.Name == "" {
		return nil, errors.New("feishu app secret reference is required")
	}
	appSecret, err := secrets.ResolveSecret(ctx, binding.Scope(), binding.Secret)
	if err != nil {
		return nil, fmt.Errorf("resolve feishu app secret: %w", err)
	}
	if appSecret == "" {
		return nil, errors.New("feishu app secret is required")
	}
	config := outboundConfig{}
	for _, opt := range opts {
		if opt == nil {
			continue
		}
		if err := opt(&config); err != nil {
			return nil, err
		}
	}
	clientOptions := []lark.ClientOptionFunc{
		lark.WithLogReqAtDebug(false),
	}
	if config.httpClient != nil {
		clientOptions = append(clientOptions, lark.WithHttpClient(config.httpClient))
	}
	if config.baseURL != "" {
		clientOptions = append(clientOptions,
			lark.WithOpenBaseUrl(config.baseURL),
			lark.WithOAuthBaseUrl(config.baseURL),
		)
	}
	return &OutboundClient{
		client: lark.NewClient(binding.ExternalAccount, appSecret, clientOptions...),
	}, nil
}

// DownloadMediaForMessage is the production media path. Feishu requires both
// the message ID and resource key; the former comes from ChannelInput's
// normalized external message ID and the latter from the opaque media ref.
func (c *OutboundClient) DownloadMediaForMessage(
	ctx context.Context,
	messageID string,
	media channels.ProviderMediaRef,
) (channels.DownloadedMedia, error) {
	if c == nil || c.client == nil {
		return channels.DownloadedMedia{}, errFeishuOutboundNotInitialized
	}
	if err := media.Validate(); err != nil {
		return channels.DownloadedMedia{}, err
	}
	if messageID == "" {
		return channels.DownloadedMedia{}, errors.New("feishu media message id is required")
	}
	resource := media.Reference
	resourceType := "image"
	if media.Kind == channels.MessageTypeFile {
		resourceType = "file"
	}
	request := larkim.NewGetMessageResourceReqBuilder().
		MessageId(messageID).
		FileKey(resource).
		Type(resourceType).
		Build()
	response, err := c.client.Im.MessageResource.Get(ctx, request)
	if err != nil {
		return channels.DownloadedMedia{}, transportError(err)
	}
	if response == nil {
		return channels.DownloadedMedia{}, &ProviderSendError{Retryable: true, cause: errors.New("empty feishu media response")}
	}
	if !response.Success() {
		return channels.DownloadedMedia{}, responseError(response.ApiResp, response.Code)
	}
	if response.File == nil {
		return channels.DownloadedMedia{}, errors.New("feishu media response has no file")
	}
	data, err := io.ReadAll(io.LimitReader(response.File, maxFeishuInboundMediaBytes+1))
	if err != nil {
		return channels.DownloadedMedia{}, fmt.Errorf("read feishu media: %w", err)
	}
	if len(data) > maxFeishuInboundMediaBytes {
		return channels.DownloadedMedia{}, errors.New("feishu media exceeds size limit")
	}
	filename := response.FileName
	if filename == "" {
		filename = resource
	}
	return channels.DownloadedMedia{
		Filename: filename,
		MIMEType: http.DetectContentType(data),
		Data:     data,
	}, nil
}

// SendOnce encodes and sends one Feishu Reply through the official Open API.
// It performs no queue claim, retry, or lease operation.
func (c *OutboundClient) SendOnce(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.client == nil {
		return channels.ProviderReceipt{}, errFeishuOutboundNotInitialized
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("feishu reply: %w", err)
	}
	if reply.Channel != channels.ChannelFeishu {
		return channels.ProviderReceipt{}, errors.New("feishu outbound client received another channel")
	}
	content, err := encodeReply(reply)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	kind, id, err := parseProviderTarget(providerTarget)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	switch kind {
	case feishuTargetMessage:
		return c.replyMessage(ctx, id, content, reply.ReplyID)
	case feishuTargetUser, feishuTargetConversation:
		return c.createMessage(ctx, kind, id, content, reply.ReplyID)
	default:
		return channels.ProviderReceipt{}, errFeishuReplyUnsupported
	}
}

func encodeReply(reply channels.Reply) ([]byte, error) {
	if !utf8.ValidString(reply.Text) || len([]byte(reply.Text)) > maxFeishuReplyTextBytes {
		return nil, errFeishuReplyTooLarge
	}
	content, err := json.Marshal(struct {
		Text string `json:"text"`
	}{Text: reply.Text})
	if err != nil {
		return nil, fmt.Errorf("encode feishu text: %w", err)
	}
	return content, nil
}

func (c *OutboundClient) createMessage(
	ctx context.Context,
	targetKind, targetID string,
	content []byte,
	uuid string,
) (channels.ProviderReceipt, error) {
	req := larkim.NewCreateMessageReqBuilder().
		ReceiveIdType(targetKind).
		Body(larkim.NewCreateMessageReqBodyBuilder().
			ReceiveId(targetID).
			MsgType(larkim.MsgTypeText).
			Content(string(content)).
			Uuid(uuid).
			Build()).
		Build()
	resp, err := c.client.Im.Message.Create(ctx, req)
	if err != nil {
		return channels.ProviderReceipt{}, transportError(err)
	}
	if resp == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{Retryable: true, cause: errors.New("empty feishu response")}
	}
	if !resp.Success() {
		return channels.ProviderReceipt{}, responseError(resp.ApiResp, resp.Code)
	}
	if resp.Data != nil {
		return receiptFromID(resp.Data.MessageId)
	}
	return receiptFromID(nil)
}

func (c *OutboundClient) replyMessage(
	ctx context.Context,
	messageID string,
	content []byte,
	uuid string,
) (channels.ProviderReceipt, error) {
	req := larkim.NewReplyMessageReqBuilder().
		MessageId(messageID).
		Body(larkim.NewReplyMessageReqBodyBuilder().
			MsgType(larkim.MsgTypeText).
			Content(string(content)).
			Uuid(uuid).
			Build()).
		Build()
	resp, err := c.client.Im.Message.Reply(ctx, req)
	if err != nil {
		return channels.ProviderReceipt{}, transportError(err)
	}
	if resp == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{Retryable: true, cause: errors.New("empty feishu response")}
	}
	if !resp.Success() {
		return channels.ProviderReceipt{}, responseError(resp.ApiResp, resp.Code)
	}
	if resp.Data != nil {
		return receiptFromID(resp.Data.MessageId)
	}
	return receiptFromID(nil)
}

func receiptFromID(providerMessageID *string) (channels.ProviderReceipt, error) {
	if providerMessageID != nil && *providerMessageID != "" {
		return channels.ProviderReceipt{ProviderMessageID: *providerMessageID}, nil
	}
	return channels.ProviderReceipt{}, &ProviderSendError{cause: errFeishuProviderMessageID}
}

func transportError(err error) error {
	return &ProviderSendError{
		Retryable: !errors.Is(err, context.Canceled),
		cause:     err,
	}
}

func responseError(response *larkcore.ApiResp, code int) error {
	statusCode := 0
	if response != nil {
		statusCode = response.StatusCode
	}
	return &ProviderSendError{
		StatusCode: statusCode,
		Code:       code,
		Retryable:  retryableFeishuStatus(statusCode),
	}
}

func retryableFeishuStatus(statusCode int) bool {
	return statusCode == http.StatusRequestTimeout || statusCode == http.StatusTooEarly ||
		statusCode == http.StatusTooManyRequests || statusCode >= http.StatusInternalServerError
}

var _ channels.ProviderOutboundClient = (*OutboundClient)(nil)
