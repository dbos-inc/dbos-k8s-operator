// Package conductor is a minimal bearer-JWT REST client for the Conductor API.
// Only the one read path the operator needs is implemented: QueueAutoscale,
// the queue-based-autoscaling recommendation Conductor computes from the
// application's stored autoscaling policy (404 when none is installed).
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

// ErrNoPolicy reports that Conductor answered the autoscale request with 404:
// the application has no stored autoscaling policy (or does not exist), so
// there is no recommendation to act on. A normal state, not a failure.
var ErrNoPolicy = errors.New("no autoscaling policy")

const defaultDomain = "cloud.dbos.dev"

// defaultCloudPathPrefix is the path prefix DBOS Cloud uses to route to
// Conductor's REST API. Self-hosted deployments typically expose the API at
// just /api, so users override Endpoint with the appropriate base URL.
const defaultCloudPathPrefix = "/conductor/v1alpha1"

// VersionRecommendation is one application version's entry in a
// queue-based-autoscaling response.
type VersionRecommendation struct {
	ApplicationVersion string
	IsLatest           bool
	DesiredExecutors   int
	ObservedAt         int64 // epoch ms the aggregate was computed, from the response
}

// AutoscaleResult is a decoded queue-based-autoscaling response. Body is the
// latest version's entry, raw JSON exactly as Conductor served it (camelCase
// v2 form — KEDA's valueLocation must read "desiredExecutors"), so it can be
// re-served verbatim to KEDA's metrics-api scaler, whose valueLocation reads
// a single object; the parsed fields mirror that entry for logging and CR
// status updates. OldVersions carries the remaining entries — older versions
// that still hold work and need executors of their own — in Conductor's order
// (most recently registered first).
type AutoscaleResult struct {
	Body               []byte
	ApplicationVersion string
	DesiredExecutors   int
	ObservedAt         int64
	OldVersions        []VersionRecommendation
}

// Options configures a Client.
type Options struct {
	// Endpoint is the full base URL up to (but not including) /v2. Optional;
	// if empty, defaults to https://${DBOS_DOMAIN:-cloud.dbos.dev}/conductor/v1alpha1.
	Endpoint string

	// OrgName is required and is passed as the :orgName URL segment. The v2
	// API addresses orgs by name (e.g. "local"), not by UUID.
	OrgName string

	// Token is the bearer JWT, required.
	Token string

	// InsecureSkipVerify disables TLS verification (dev only).
	InsecureSkipVerify bool

	// Timeout caps each HTTP request. Defaults to 10s.
	Timeout time.Duration
}

// Client is safe for concurrent use.
type Client struct {
	baseURL string
	orgName string
	token   string
	http    *http.Client
}

// New constructs a client. Validates required fields and resolves the base URL.
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

// resolveBaseURL turns the user-supplied Endpoint (which may be empty) into
// the full base URL we prepend to every request. The result has no trailing
// slash and contains everything up to but not including /v2.
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

// QueueAutoscale asks Conductor how many executors the app needs right now.
// The computation is driven entirely by the application's stored autoscaling
// policy; returns ErrNoPolicy on 404 (no policy installed).
//
// Conductor answers with one entry per application version — the latest first,
// then every older version still holding work; a version with no remaining
// work at all is absent. The latest entry is re-served to KEDA, which scales
// the app's main Deployment; the older entries are returned so the manager can
// maintain (for now: report) per-version Deployments for them.
//
//	GET <base>/v2/orgs/<orgName>/apps/<app>/autoscale
//	  → [{"applicationVersion": "...", "isLatest": true, "desiredExecutors": N, "observedAt": ms}, ...]
func (c *Client) QueueAutoscale(ctx context.Context, app string) (*AutoscaleResult, error) {
	path := fmt.Sprintf("/v2/orgs/%s/apps/%s/autoscale", url.PathEscape(c.orgName), url.PathEscape(app))
	// Decoded twice: once to parse the entries, once to keep the latest
	// entry's bytes intact for KEDA.
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
		// The latest version leads the array; keying on the flag keeps this
		// working if that ever stops holding.
		if e.IsLatest && result.Body == nil {
			result.Body = raw
			result.ApplicationVersion = e.ApplicationVersion
			result.DesiredExecutors = e.DesiredExecutors
			result.ObservedAt = e.ObservedAt
			continue
		}
		result.OldVersions = append(result.OldVersions, VersionRecommendation{
			ApplicationVersion: e.ApplicationVersion,
			IsLatest:           e.IsLatest,
			DesiredExecutors:   e.DesiredExecutors,
			ObservedAt:         e.ObservedAt,
		})
	}
	if result.Body == nil {
		return nil, fmt.Errorf("QueueAutoscale %s: no isLatest entry in %d-entry response", app, len(entries))
	}
	return result, nil
}

// HTTPError is a non-2xx Conductor response.
type HTTPError struct {
	StatusCode int
	Status     string
	Body       string
}

func (e *HTTPError) Error() string {
	return e.Status + ": " + e.Body
}

// do performs one request and returns the raw response body (also decoding it
// into out when out is non-nil).
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
