// Package conductor is a minimal bearer-JWT REST client for the Conductor API.
// Only the read paths the operator needs for queue load are implemented:
// GetQueue (for worker_concurrency) and QueueDepth (count of ENQUEUED+PENDING
// workflows on a queue).
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

// Workflow mirrors WorkflowsOutput (only the fields the operator reads).
type Workflow struct {
	WorkflowUUID       string  `json:"WorkflowUUID"`
	Status             *string `json:"Status"`
	QueueName          *string `json:"QueueName"`
	ApplicationVersion *string `json:"ApplicationVersion"`
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

// GetQueue fetches static queue config (worker_concurrency, etc.).
//
//	GET <base>/api/<orgName>/applications/<app>/queues/<queue>
func (c *Client) GetQueue(ctx context.Context, app, queue string) (*Queue, error) {
	path := fmt.Sprintf("/api/%s/applications/%s/queues/%s",
		url.PathEscape(c.orgName), url.PathEscape(app), url.PathEscape(queue))
	var out Queue
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("GetQueue %s/%s: %w", app, queue, err)
	}
	return &out, nil
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

// listWorkflowsBody is the POST body for Conductor's ListWorkflows endpoint.
// Only the filters the operator uses are exposed; the full request shape has
// many more fields.
type listWorkflowsBody struct {
	Status []string `json:"status"`
	Limit  int      `json:"limit"`
}

// PendingWorkflowsByVersion returns a map of application_version → count for
// every non-terminal (ENQUEUED or PENDING) workflow Conductor has for app.
// Used by the archiver to decide which old versions need a sibling Deployment
// kept alive.
//
// Implementation note: the underlying ListWorkflows endpoint doesn't aggregate
// server-side, so we list and group client-side. Capped at 10000 workflows —
// well past the point where the right answer is "everything's pending, scale
// to max" anyway, but worth knowing about if you're debugging strange counts.
//
//	POST <base>/api/<orgName>/applications/<app>/workflows/
//	  body: {status: ["ENQUEUED","PENDING"], limit: 10000}
func (c *Client) PendingWorkflowsByVersion(ctx context.Context, app string) (map[string]int, error) {
	path := fmt.Sprintf("/api/%s/applications/%s/workflows/",
		url.PathEscape(c.orgName), url.PathEscape(app))
	body := listWorkflowsBody{
		Status: []string{"ENQUEUED", "PENDING"},
		Limit:  10000,
	}
	var out []Workflow
	if err := c.do(ctx, http.MethodPost, path, body, &out); err != nil {
		return nil, fmt.Errorf("PendingWorkflowsByVersion %s: %w", app, err)
	}
	counts := make(map[string]int, len(out))
	for _, wf := range out {
		v := ""
		if wf.ApplicationVersion != nil {
			v = *wf.ApplicationVersion
		}
		counts[v]++
	}
	return counts, nil
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
	var out []Workflow
	if err := c.do(ctx, http.MethodPost, path, body, &out); err != nil {
		return 0, fmt.Errorf("QueueDepth %s/%s: %w", app, queue, err)
	}
	return int64(len(out)), nil
}

// VersionMetadata is what the operator writes to and reads from Conductor's
// version_metadata column. Conductor stores it as opaque JSONB, but the
// operator owns the shape — currently just the K8s pod-template-hash of the
// ReplicaSet that ran this version's executors.
type VersionMetadata struct {
	PodTemplateHash string `json:"pod_template_hash,omitempty"`
}

// ApplicationVersion mirrors Conductor's ApplicationVersionOutput. The
// VersionMetadata field is populated server-side from Conductor's own
// application_versions table (the executor proxy doesn't carry it); nil if
// the row has no metadata yet.
type ApplicationVersion struct {
	VersionID        string           `json:"version_id"`
	VersionName      string           `json:"version_name"`
	VersionTimestamp int64            `json:"version_timestamp"`
	CreatedAt        int64            `json:"created_at"`
	VersionMetadata  *VersionMetadata `json:"version_metadata,omitempty"`
}

// ListApplicationVersions returns every version Conductor has on record for
// app. Sorted by version_timestamp DESC per Conductor's contract (so index 0
// is the latest), but callers should not rely on sort order — the
// LatestApplicationVersion helper below picks by timestamp explicitly.
//
//	GET <base>/api/<orgName>/applications/<app>/versions
func (c *Client) ListApplicationVersions(ctx context.Context, app string) ([]ApplicationVersion, error) {
	path := fmt.Sprintf("/api/%s/applications/%s/versions",
		url.PathEscape(c.orgName), url.PathEscape(app))
	var out []ApplicationVersion
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("ListApplicationVersions %s: %w", app, err)
	}
	return out, nil
}

// LatestApplicationVersion returns the version with the highest
// version_timestamp. Returns a zero-value ApplicationVersion and an error if
// the app has no registered versions yet.
func (c *Client) LatestApplicationVersion(ctx context.Context, app string) (ApplicationVersion, error) {
	versions, err := c.ListApplicationVersions(ctx, app)
	if err != nil {
		return ApplicationVersion{}, err
	}
	if len(versions) == 0 {
		return ApplicationVersion{}, fmt.Errorf("no versions registered for app %q", app)
	}
	latest := versions[0]
	for _, v := range versions[1:] {
		if v.VersionTimestamp > latest.VersionTimestamp {
			latest = v
		}
	}
	return latest, nil
}

// updateVersionMetadataBody mirrors Conductor's UpdateApplicationVersionMetadataBody.
// version_metadata is opaque JSON — the operator currently stuffs a
// {"pod_template_hash": "..."} object in here; Conductor stores it as JSONB
// without interpreting it.
type updateVersionMetadataBody struct {
	VersionName     string            `json:"version_name"`
	VersionMetadata map[string]string `json:"version_metadata"`
}

// UpdateVersionMetadata writes opaque metadata onto a specific application
// version (identified by name) for app. The version is explicit — callers
// resolve which version they want before calling.
//
//	PATCH <base>/api/<orgName>/applications/<app>/versions/metadata
//	  body: {"version_name": "<v>", "version_metadata": {...}}
func (c *Client) UpdateVersionMetadata(ctx context.Context, app, versionName string, metadata map[string]string) error {
	path := fmt.Sprintf("/api/%s/applications/%s/versions/metadata",
		url.PathEscape(c.orgName), url.PathEscape(app))
	body := updateVersionMetadataBody{
		VersionName:     versionName,
		VersionMetadata: metadata,
	}
	if err := c.do(ctx, http.MethodPatch, path, body, nil); err != nil {
		return fmt.Errorf("UpdateVersionMetadata %s (version=%s): %w", app, versionName, err)
	}
	return nil
}

// Executor mirrors Conductor's GetExecutorResponse (only the fields the
// operator uses).
type Executor struct {
	ID         string  `json:"executor_id"`
	AppID      string  `json:"application_id"`
	AppVersion string  `json:"application_version"`
	Status     string  `json:"status"`
	Hostname   *string `json:"hostname,omitempty"`
}

// ListExecutors returns every executor record Conductor has for app. The
// operator joins on hostname (= K8s pod name) to map a K8s rollout's pods to
// the application_version their Transact process registered under.
//
//	GET <base>/api/<orgName>/applications/<app>/executors
func (c *Client) ListExecutors(ctx context.Context, app string) ([]Executor, error) {
	path := fmt.Sprintf("/api/%s/applications/%s/executors",
		url.PathEscape(c.orgName), url.PathEscape(app))
	var out []Executor
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
		return nil, fmt.Errorf("ListExecutors %s: %w", app, err)
	}
	return out, nil
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
