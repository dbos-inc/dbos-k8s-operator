// Package conductor is a minimal bearer-JWT REST client for the Conductor API.
// Only the read paths the operator needs for queue load are implemented:
// ListQueues (for worker_concurrency discovery) and QueueDepth (count of
// ENQUEUED+PENDING workflows on a queue).
package conductor

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// defaultDomain is the fallback when both Options.Endpoint and the DBOS_DOMAIN
// env var are unset.
const defaultDomain = "cloud.dbos.dev"

// defaultCloudPathPrefix is the path prefix DBOS Cloud uses to route to
// Conductor's REST API. Self-hosted deployments typically expose the API at
// just /api, so users override Endpoint with the appropriate base URL.
const defaultCloudPathPrefix = "/conductor/v1alpha1"

// Queue mirrors Conductor's QueueOutput (only the fields the operator uses).
type Queue struct {
	Name              string `json:"name"`
	Concurrency       *int   `json:"concurrency"`
	WorkerConcurrency *int   `json:"worker_concurrency"`
}

// workflow is what we decode QueueDepth's array elements into; we only care
// about the length, so an empty struct would do, but a named type keeps the
// JSON decoder honest about the shape it accepts.
type workflow struct {
	WorkflowUUID string `json:"WorkflowUUID"`
}

// Options configures a Client.
type Options struct {
	// Endpoint is the full base URL up to (but not including) /api. Optional;
	// if empty, defaults to https://${DBOS_DOMAIN:-cloud.dbos.dev}/conductor/v1alpha1.
	Endpoint string

	// OrgName is required and is passed as the :org_id URL segment.
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
// slash and contains everything up to but not including /api.
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
	return "https://" + domain + defaultCloudPathPrefix, nil
}

// ListQueues returns every queue Conductor has registered for app, with each
// queue's static config (worker_concurrency, etc.) populated. Used by the
// poller to discover queues without requiring the user to enumerate them in
// the operator's ConfigMap.
//
//	GET <base>/api/<orgName>/applications/<app>/queues
func (c *Client) ListQueues(ctx context.Context, app string) ([]Queue, error) {
	path := fmt.Sprintf("/api/%s/applications/%s/queues",
		url.PathEscape(c.orgName), url.PathEscape(app))
	var out []Queue
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("ListQueues %s: %w", app, err)
	}
	return out, nil
}

// queueDepthBody is the POST body Conductor's ListQueuedWorkflows expects.
// We filter on the queue name and statuses ENQUEUED + PENDING. Limit is set
// to a soft cap; if a queue is deeper than this we under-report (acceptable
// for HPA's purposes — well into "scale to max" territory anyway).
type queueDepthBody struct {
	QueueName []string `json:"queue_name"`
	Status    []string `json:"status"`
	Limit     int      `json:"limit"`
}

// QueueDepth returns the number of workflows in ENQUEUED or PENDING state for
// the named queue.
//
//	POST <base>/api/<orgName>/applications/<app>/queues/
//	  body: {queue_name: [<queue>], status: ["ENQUEUED","PENDING"], limit: 10000}
func (c *Client) QueueDepth(ctx context.Context, app, queue string) (int64, error) {
	path := fmt.Sprintf("/api/%s/applications/%s/queues/",
		url.PathEscape(c.orgName), url.PathEscape(app))
	body := queueDepthBody{
		QueueName: []string{queue},
		Status:    []string{"ENQUEUED", "PENDING"},
		Limit:     10000,
	}
	var out []workflow
	if err := c.do(ctx, http.MethodPost, path, body, &out); err != nil {
		return 0, fmt.Errorf("QueueDepth %s/%s: %w", app, queue, err)
	}
	return int64(len(out)), nil
}

func (c *Client) do(ctx context.Context, method, path string, in any, out any) error {
	var body io.Reader
	if in != nil {
		buf, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("marshal request: %w", err)
		}
		body = strings.NewReader(string(buf))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}
