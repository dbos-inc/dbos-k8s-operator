// Package conductor is a minimal bearer-JWT REST client for the Conductor API.
package conductor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// ErrNoPolicy reports a 404 — a normal state, not a failure.
var ErrNoPolicy = errors.New("no autoscaling policy")

const defaultDomain = "cloud.dbos.dev"

const defaultCloudPathPrefix = "/conductor/v1alpha1"

type VersionRecommendation struct {
	ApplicationVersion string
	DesiredExecutors   int
	ObservedAt         int64 // epoch ms
}

// Body is the latest version's entry verbatim, re-served as-is to KEDA;
// OldVersions holds the remaining entries, most recently registered first.
type AutoscaleResult struct {
	Body               []byte
	ApplicationVersion string
	DesiredExecutors   int
	ObservedAt         int64
	OldVersions        []VersionRecommendation
}

type Options struct {
	// Base URL up to (not including) /v2; empty derives it from DBOS_DOMAIN.
	Endpoint string

	OrgName string // required; org name, not UUID
	Token   string // required; bearer JWT

	InsecureSkipVerify bool          // dev only
	Timeout            time.Duration // per-request cap, default 10s
}

type Client struct {
	baseURL string
	orgName string
	token   string
	http    *http.Client
}

func New(opts Options) (*Client, error) {
	if opts.OrgName == "" {
		return nil, fmt.Errorf("orgName is required")
	}
	if opts.Token == "" {
		return nil, fmt.Errorf("token is required")
	}
	base, err := resolveBaseURL(opts.Endpoint)
	if err != nil {
		return nil, err
	}
	timeout := opts.Timeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if opts.InsecureSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &Client{
		baseURL: base,
		orgName: opts.OrgName,
		token:   opts.Token,
		http:    &http.Client{Timeout: timeout, Transport: transport},
	}, nil
}

func resolveBaseURL(endpoint string) (string, error) {
	if endpoint != "" {
		if _, err := url.Parse(endpoint); err != nil {
			return "", fmt.Errorf("invalid endpoint %q: %w", endpoint, err)
		}
		return strings.TrimRight(endpoint, "/"), nil
	}
	domain := os.Getenv("DBOS_DOMAIN")
	if domain == "" {
		domain = defaultDomain
	}
	domain = strings.TrimRight(domain, "/")
	if !strings.Contains(domain, "://") {
		domain = "https://" + domain
	}
	return domain + defaultCloudPathPrefix, nil
}

// One entry per version still holding work, latest first; ErrNoPolicy on 404.
func (c *Client) QueueAutoscale(ctx context.Context, app string) (*AutoscaleResult, error) {
	path := fmt.Sprintf("/v2/orgs/%s/apps/%s/autoscale", url.PathEscape(c.orgName), url.PathEscape(app))
	var entries []json.RawMessage
	if _, err := c.do(ctx, http.MethodGet, path, nil, &entries); err != nil {
		var he *HTTPError
		if errors.As(err, &he) && he.StatusCode == http.StatusNotFound {
			return nil, fmt.Errorf("QueueAutoscale %s: %w", app, ErrNoPolicy)
		}
		return nil, fmt.Errorf("QueueAutoscale %s: %w", app, err)
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("QueueAutoscale %s: no recommendation returned", app)
	}
	type entry struct {
		ApplicationVersion string `json:"applicationVersion"`
		IsLatest           bool   `json:"isLatest"`
		DesiredExecutors   int    `json:"desiredExecutors"`
		ObservedAt         int64  `json:"observedAt"`
	}
	result := &AutoscaleResult{}
	for _, raw := range entries {
		var e entry
		if err := json.Unmarshal(raw, &e); err != nil {
			return nil, fmt.Errorf("QueueAutoscale %s: decode entry: %w", app, err)
		}
		if e.IsLatest {
			if result.Body == nil {
				result.Body = raw
				result.ApplicationVersion = e.ApplicationVersion
				result.DesiredExecutors = e.DesiredExecutors
				result.ObservedAt = e.ObservedAt
			}
			continue
		}
		result.OldVersions = append(result.OldVersions, VersionRecommendation{
			ApplicationVersion: e.ApplicationVersion,
			DesiredExecutors:   e.DesiredExecutors,
			ObservedAt:         e.ObservedAt,
		})
	}
	if result.Body == nil {
		return nil, fmt.Errorf("QueueAutoscale %s: no isLatest entry in %d-entry response", app, len(entries))
	}
	return result, nil
}

type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *HTTPError) Error() string {
	return e.Status + ": " + e.Body
}

func (c *Client) do(ctx context.Context, method, path string, in any, out any) ([]byte, error) {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		body = strings.NewReader(string(buf))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("%s %s: %w", method, path,
			&HTTPError{StatusCode: resp.StatusCode, Status: resp.Status, Body: strings.TrimSpace(string(raw))})
	}
	if out == nil {
		return raw, nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}
	return raw, nil
}
