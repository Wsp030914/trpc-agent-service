package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
)

const (
	maxWeComReplyBytes            = 20480
	maxWeComProviderResponseBytes = 64 << 10
)

var (
	errWeComReplyTooLarge = errors.New("wecom reply is too large")
)

// ProviderSendError is the stable, body-redacted error returned by one
// WeCom provider call. Retry policy and Reply Outbox state transitions remain
// owned by IM-06.
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
		return "wecom provider send failed"
	}
	if e.StatusCode != 0 {
		return fmt.Sprintf("wecom provider returned status %d", e.StatusCode)
	}
	if e.Code != 0 {
		return fmt.Sprintf("wecom provider returned code %d", e.Code)
	}
	return "wecom provider send failed"
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

// OutboundClient performs exactly one WeCom AI Bot active response_url call.
// It does not claim, persist, retry, or otherwise manage Reply Outbox state.
type OutboundClient struct {
	httpClient      *http.Client
	targetValidator func(string) error
}

// NewOutboundClient creates a one-call WeCom provider client. A nil HTTP
// client uses http.DefaultClient; callers control deadlines with context.
func NewOutboundClient(httpClient *http.Client) *OutboundClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	client := *httpClient
	// A response_url is an external capability, not a redirect permission.
	// Refuse redirects even when a caller supplied a client with a permissive
	// redirect policy.
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &OutboundClient{
		httpClient:      &client,
		targetValidator: validateResponseURL,
	}
}

// SendOnce encodes and sends one platform Reply to a resolved WeCom target.
// WeCom response_url calls do not return a provider message ID, so the local
// reply ID is used as the shared receipt fallback.
func (c *OutboundClient) SendOnce(
	ctx context.Context,
	reply channels.Reply,
	providerTarget string,
) (channels.ProviderReceipt, error) {
	if c == nil || c.httpClient == nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: errors.New("wecom outbound client is not initialized")}
	}
	if err := reply.Validate(); err != nil {
		return channels.ProviderReceipt{}, fmt.Errorf("wecom reply: %w", err)
	}
	if reply.Channel != channels.ChannelWeCom {
		return channels.ProviderReceipt{}, errors.New("wecom outbound client received another channel")
	}
	validator := c.targetValidator
	if validator == nil {
		validator = validateResponseURL
	}
	if err := validator(providerTarget); err != nil {
		return channels.ProviderReceipt{}, &ProviderSendError{cause: err}
	}
	payload, err := encodeReply(reply)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	result, err := sendProviderPayload(ctx, c.httpClient, providerTarget, payload)
	if err != nil {
		return channels.ProviderReceipt{}, err
	}
	return providerReceipt(reply, result), nil
}

func sendProviderPayload(
	ctx context.Context,
	httpClient *http.Client,
	target string,
	payload []byte,
) (providerResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(payload))
	if err != nil {
		return providerResponse{}, &ProviderSendError{cause: errors.New("create wecom provider request")}
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := httpClient.Do(request)
	if err != nil {
		return providerResponse{}, &ProviderSendError{Retryable: true, cause: err}
	}
	defer func() {
		_ = response.Body.Close()
	}()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxWeComProviderResponseBytes+1))
	if err != nil {
		return providerResponse{}, &ProviderSendError{
			StatusCode: response.StatusCode,
			Retryable:  retryableHTTPStatus(response.StatusCode),
			cause:      err,
		}
	}
	if len(body) > maxWeComProviderResponseBytes {
		return providerResponse{}, &ProviderSendError{
			StatusCode: response.StatusCode,
			Retryable:  retryableHTTPStatus(response.StatusCode),
			cause:      errWeComReplyTooLarge,
		}
	}
	var result providerResponse
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &result); err != nil {
			return providerResponse{}, &ProviderSendError{
				StatusCode: response.StatusCode,
				Retryable:  retryableHTTPStatus(response.StatusCode),
				cause:      errors.New("decode wecom provider response"),
			}
		}
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return providerResponse{}, &ProviderSendError{
			StatusCode: response.StatusCode,
			Code:       result.Code,
			Retryable:  retryableHTTPStatus(response.StatusCode),
		}
	}
	if result.Code != 0 {
		return providerResponse{}, &ProviderSendError{Code: result.Code}
	}
	return result, nil
}

func providerReceipt(
	reply channels.Reply,
	result providerResponse,
) channels.ProviderReceipt {
	providerMessageID := result.MessageID
	if providerMessageID == "" {
		providerMessageID = reply.ReplyID
	}
	return channels.ProviderReceipt{ProviderMessageID: providerMessageID}
}

type providerResponse struct {
	Code      int    `json:"errcode"`
	MessageID string `json:"msgid"`
}

func encodeReply(reply channels.Reply) ([]byte, error) {
	if len([]byte(reply.Text)) > maxWeComReplyBytes {
		return nil, errWeComReplyTooLarge
	}
	return json.Marshal(struct {
		MessageType string `json:"msgtype"`
		Markdown    struct {
			Content string `json:"content"`
		} `json:"markdown"`
	}{
		MessageType: "markdown",
		Markdown: struct {
			Content string `json:"content"`
		}{Content: reply.Text},
	})
}

func retryableHTTPStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly ||
		status == http.StatusTooManyRequests || status >= http.StatusInternalServerError
}

var _ channels.ProviderOutboundClient = (*OutboundClient)(nil)
