package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

const maxConfirmationResponseBytes = 64 << 10

type options struct {
	provider          string
	baseURL           string
	confirmationURL   string
	apiKey            string
	allowInsecureHTTP bool
	correlationID     string
	message           string
	timeout           time.Duration
}

type confirmationRequest struct {
	Provider       string `json:"provider"`
	ServiceBaseURL string `json:"service_base_url"`
	CorrelationID  string `json:"correlation_id"`
	Message        string `json:"message"`
}

type confirmationResponse struct {
	Confirmed       bool   `json:"confirmed"`
	Provider        string `json:"provider"`
	CorrelationID   string `json:"correlation_id"`
	EventAccepted   bool   `json:"event_accepted"`
	EventStatus     int    `json:"event_status"`
	ExecutionStatus string `json:"execution_status"`
	ReplyConfirmed  bool   `json:"reply_confirmed"`
}

func main() {
	opts, err := parseFlags(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, "im-e2e:", err)
		os.Exit(2)
	}
	if err := run(context.Background(), opts, nil); err != nil {
		fmt.Fprintln(os.Stderr, "im-e2e:", err)
		os.Exit(1)
	}
	fmt.Printf("IM_E2E=PASS provider=%s correlation_id=%s\n", opts.provider, opts.correlationID)
}

func parseFlags(args []string) (options, error) {
	flags := flag.NewFlagSet("im-e2e", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	var opts options
	flags.StringVar(&opts.provider, "provider", "", "provider: feishu or wecom")
	flags.StringVar(&opts.baseURL, "base-url", "", "service base URL")
	flags.StringVar(&opts.confirmationURL, "confirmation-url", "", "provider confirmation runner URL")
	flags.StringVar(&opts.apiKey, "api-key", "", "optional bearer token for the confirmation runner")
	flags.BoolVar(&opts.allowInsecureHTTP, "allow-insecure-http", false, "allow HTTP only for loopback test endpoints")
	flags.StringVar(&opts.correlationID, "correlation-id", "", "correlation ID")
	flags.StringVar(&opts.message, "message", "", "message submitted to the confirmation runner")
	flags.DurationVar(&opts.timeout, "timeout", 2*time.Minute, "overall timeout")
	if err := flags.Parse(args); err != nil {
		return options{}, errors.New("invalid command-line arguments")
	}
	if opts.correlationID == "" {
		opts.correlationID = fmt.Sprintf("im-e2e-%d", time.Now().UnixNano())
	}
	if opts.message == "" {
		opts.message = "trpc-agent-service IM E2E confirmation"
	}
	if err := validateOptions(opts); err != nil {
		return options{}, err
	}
	return opts, nil
}

func validateOptions(opts options) error {
	if opts.provider != "feishu" && opts.provider != "wecom" {
		return errors.New("provider must be feishu or wecom")
	}
	if opts.timeout <= 0 {
		return errors.New("timeout must be positive")
	}
	if strings.TrimSpace(opts.correlationID) == "" {
		return errors.New("correlation-id is required")
	}
	if strings.TrimSpace(opts.message) == "" {
		return errors.New("message is required")
	}
	baseURL, err := parseEndpoint(opts.baseURL, "base-url", false)
	if err != nil {
		return err
	}
	if err := validateEndpointTransport(baseURL, "base-url", opts.allowInsecureHTTP); err != nil {
		return err
	}
	confirmationURL, err := parseEndpoint(opts.confirmationURL, "confirmation-url", true)
	if err != nil {
		return err
	}
	if err := validateEndpointTransport(confirmationURL, "confirmation-url", opts.allowInsecureHTTP); err != nil {
		return err
	}
	return nil
}

func parseEndpoint(raw, name string, allowQuery bool) (*url.URL, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || (!allowQuery && parsed.RawQuery != "") {
		return nil, fmt.Errorf("%s is invalid", name)
	}
	return parsed, nil
}

func validateEndpointTransport(parsed *url.URL, name string, allowInsecureHTTP bool) error {
	if parsed.Scheme == "https" {
		return nil
	}
	if allowInsecureHTTP && isLoopbackHost(parsed.Hostname()) {
		return nil
	}
	return fmt.Errorf("%s must use https; HTTP is allowed only for loopback tests with --allow-insecure-http", name)
}

func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func run(ctx context.Context, opts options, client *http.Client) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if err := validateOptions(opts); err != nil {
		return err
	}
	runCtx, cancel := context.WithTimeout(ctx, opts.timeout)
	defer cancel()
	if client == nil {
		client = &http.Client{}
	}
	safeClient := *client
	safeClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	if err := checkReadiness(runCtx, &safeClient, opts.baseURL); err != nil {
		return err
	}
	return confirmProviderReply(runCtx, &safeClient, opts)
}

func checkReadiness(ctx context.Context, client *http.Client, baseURL string) error {
	base, err := parseEndpoint(baseURL, "base-url", false)
	if err != nil {
		return err
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/readyz"
	base.RawPath = ""
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, base.String(), nil)
	if err != nil {
		return errors.New("create readiness request failed")
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("service readiness request failed")
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxConfirmationResponseBytes))
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("service readiness returned status %d", response.StatusCode)
	}
	return nil
}

func confirmProviderReply(ctx context.Context, client *http.Client, opts options) error {
	payload, err := json.Marshal(confirmationRequest{
		Provider:       opts.provider,
		ServiceBaseURL: strings.TrimRight(opts.baseURL, "/"),
		CorrelationID:  opts.correlationID,
		Message:        opts.message,
	})
	if err != nil {
		return errors.New("encode confirmation request failed")
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, opts.confirmationURL, bytes.NewReader(payload))
	if err != nil {
		return errors.New("create confirmation request failed")
	}
	request.Header.Set("Content-Type", "application/json")
	if strings.TrimSpace(opts.apiKey) != "" {
		request.Header.Set("Authorization", "Bearer "+opts.apiKey)
	}
	response, err := client.Do(request)
	if err != nil {
		return errors.New("confirmation runner request failed")
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxConfirmationResponseBytes+1))
	if err != nil {
		return errors.New("read confirmation runner response failed")
	}
	if len(body) > maxConfirmationResponseBytes {
		return errors.New("confirmation runner response is too large")
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("confirmation runner returned status %d", response.StatusCode)
	}
	var result confirmationResponse
	if err := json.Unmarshal(body, &result); err != nil {
		return errors.New("decode confirmation runner response failed")
	}
	if !result.Confirmed || result.Provider != opts.provider || result.CorrelationID != opts.correlationID ||
		!result.EventAccepted || result.EventStatus != http.StatusOK ||
		result.ExecutionStatus != "SUCCEEDED" || !result.ReplyConfirmed {
		return errors.New("provider event and confirmed reply were not both verified")
	}
	return nil
}
