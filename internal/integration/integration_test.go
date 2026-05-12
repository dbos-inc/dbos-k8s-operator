// Package integration exercises the full operator pipeline in-process:
// poller.Run → conductor REST client → store → metricsadapter.Provider.
// Conductor is faked with an httptest.Server returning canned ListQueues
// and QueueDepth responses. No Kubernetes is involved.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/custom-metrics-apiserver/pkg/provider"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/metricsadapter"
	"github.com/dbos-inc/dbos-k8s-operator/internal/poller"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// fakeConductor serves the two endpoints the operator uses, keyed by app.
type fakeConductor struct {
	mu     sync.Mutex
	org    string
	state  map[string]appState // app name → state
	listed int
	polled int
}

type appState struct {
	queues map[string]queueState
}

type queueState struct {
	workerConcurrency *int
	depth             int
}

func newFakeConductor(org string) *fakeConductor {
	return &fakeConductor{org: org, state: map[string]appState{}}
}

func (f *fakeConductor) setQueue(app, queue string, workerConcurrency *int, depth int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	a, ok := f.state[app]
	if !ok {
		a = appState{queues: map[string]queueState{}}
	}
	a.queues[queue] = queueState{workerConcurrency: workerConcurrency, depth: depth}
	f.state[app] = a
}

func (f *fakeConductor) removeQueue(app, queue string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if a, ok := f.state[app]; ok {
		delete(a.queues, queue)
	}
}

func (f *fakeConductor) counts() (listed, polled int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listed, f.polled
}

func (f *fakeConductor) handler(t *testing.T) http.Handler {
	mux := http.NewServeMux()
	// Wildcards: pin org segment so we catch path-template regressions.
	listPath := fmt.Sprintf("GET /api/%s/applications/{app}/queues", f.org)
	depthPath := fmt.Sprintf("POST /api/%s/applications/{app}/queues/", f.org)

	mux.HandleFunc(listPath, func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(t, r) {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		app := r.PathValue("app")
		f.mu.Lock()
		f.listed++
		st := f.state[app]
		out := make([]conductor.Queue, 0, len(st.queues))
		for name, q := range st.queues {
			out = append(out, conductor.Queue{
				Name:              name,
				WorkerConcurrency: q.workerConcurrency,
			})
		}
		f.mu.Unlock()
		writeJSON(t, w, out)
	})

	mux.HandleFunc(depthPath, func(w http.ResponseWriter, r *http.Request) {
		if !checkAuth(t, r) {
			http.Error(w, "missing auth", http.StatusUnauthorized)
			return
		}
		var body struct {
			QueueName []string `json:"queue_name"`
			Status    []string `json:"status"`
			Limit     int      `json:"limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode QueueDepth body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(body.QueueName) != 1 {
			t.Errorf("QueueDepth: expected exactly one queue name, got %v", body.QueueName)
		}
		app := r.PathValue("app")
		f.mu.Lock()
		f.polled++
		depth := f.state[app].queues[body.QueueName[0]].depth
		f.mu.Unlock()
		// Conductor returns an array of {WorkflowUUID:...}; we just need len().
		out := make([]map[string]string, depth)
		for i := range out {
			out[i] = map[string]string{"WorkflowUUID": fmt.Sprintf("wf-%d", i)}
		}
		writeJSON(t, w, out)
	})

	return mux
}

func checkAuth(t *testing.T, r *http.Request) bool {
	t.Helper()
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") == "" {
		return false
	}
	return true
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("write json: %v", err)
	}
}

// runPoller starts poller.Run in a goroutine and returns a stop func.
func runPoller(ctx context.Context, t *testing.T, endpoint, app string, s store.Store, interval time.Duration) {
	t.Helper()
	cfg := poller.Config{
		AppName:    app,
		OrgName:    "test-org",
		Endpoint:   endpoint,
		Token:      "test-token",
		Interval:   interval,
		MaxBackoff: 5 * interval,
	}
	go poller.Run(ctx, cfg, s)
}

// waitFor polls cond up to timeout. Fails the test if cond never returns true.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

func intPtr(i int) *int { return &i }

// TestPipelineEndToEnd is the headline case: two queues, max wins, HPA selector matches.
func TestPipelineEndToEnd(t *testing.T) {
	fake := newFakeConductor("test-org")
	fake.setQueue("app1", "fast", intPtr(4), 4)   // load = 1.0
	fake.setQueue("app1", "slow", intPtr(2), 10)  // load = 5.0 ← winner
	fake.setQueue("other", "q", intPtr(2), 2)     // separate app — must not bleed into app1

	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	s := store.NewInMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runPoller(ctx, t, srv.URL, "app1", s, 25*time.Millisecond)
	runPoller(ctx, t, srv.URL, "other", s, 25*time.Millisecond)

	waitFor(t, 2*time.Second, func() bool {
		return len(s.ByApp("app1")) == 2 && len(s.ByApp("other")) == 1
	}, "store to fill")

	prov := metricsadapter.New(s)
	values, err := prov.GetExternalMetric(ctx, "default",
		labels.SelectorFromSet(labels.Set{"app": "app1"}),
		provider.ExternalMetricInfo{Metric: metricsadapter.MetricName})
	if err != nil {
		t.Fatalf("GetExternalMetric: %v", err)
	}
	if len(values.Items) != 1 {
		t.Fatalf("Items len = %d, want 1 (selector should match app1 only); items=%v", len(values.Items), values.Items)
	}
	v := values.Items[0]
	if got := v.MetricLabels["app"]; got != "app1" {
		t.Errorf("label app = %q, want app1", got)
	}
	// load = 5.0 → 5000m
	if got := v.Value.MilliValue(); got != 5000 {
		t.Errorf("value = %d milli, want 5000 (max load 5.0 across queues)", got)
	}
}

// TestEvictsRemovedQueue verifies the poller drops a sample for a queue that
// disappears from ListQueues, so the metric doesn't get stuck on a stale max.
func TestEvictsRemovedQueue(t *testing.T) {
	fake := newFakeConductor("test-org")
	fake.setQueue("app1", "fast", intPtr(4), 4)  // load 1.0
	fake.setQueue("app1", "slow", intPtr(2), 10) // load 5.0

	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	s := store.NewInMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runPoller(ctx, t, srv.URL, "app1", s, 20*time.Millisecond)

	waitFor(t, 2*time.Second, func() bool { return len(s.ByApp("app1")) == 2 }, "both queues observed")

	// The slow queue disappears (e.g. user deleted it).
	fake.removeQueue("app1", "slow")

	waitFor(t, 2*time.Second, func() bool {
		got := s.ByApp("app1")
		if len(got) != 1 {
			return false
		}
		return got[0].Queue == "fast"
	}, "slow to be evicted")

	prov := metricsadapter.New(s)
	values, err := prov.GetExternalMetric(ctx, "default",
		labels.SelectorFromSet(labels.Set{"app": "app1"}),
		provider.ExternalMetricInfo{Metric: metricsadapter.MetricName})
	if err != nil {
		t.Fatalf("GetExternalMetric: %v", err)
	}
	if len(values.Items) != 1 {
		t.Fatalf("Items len = %d, want 1", len(values.Items))
	}
	// Only `fast` remains: load 1.0 → 1000m.
	if got := values.Items[0].Value.MilliValue(); got != 1000 {
		t.Errorf("value = %d milli, want 1000 (max load 1.0 after eviction)", got)
	}
}

// TestSkipsQueueWithoutWorkerConcurrency: a queue with no worker_concurrency
// is undefined for load purposes and must be skipped.
func TestSkipsQueueWithoutWorkerConcurrency(t *testing.T) {
	fake := newFakeConductor("test-org")
	fake.setQueue("app1", "unbounded", nil, 100) // skipped
	fake.setQueue("app1", "bounded", intPtr(5), 5)

	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	s := store.NewInMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runPoller(ctx, t, srv.URL, "app1", s, 20*time.Millisecond)

	waitFor(t, 2*time.Second, func() bool { return len(s.ByApp("app1")) >= 1 }, "bounded queue observed")
	// Give the poller a couple more ticks; "unbounded" must never appear.
	time.Sleep(100 * time.Millisecond)
	got := s.ByApp("app1")
	if len(got) != 1 {
		t.Fatalf("expected only 1 queue, got %d: %v", len(got), got)
	}
	if got[0].Queue != "bounded" {
		t.Fatalf("expected only bounded queue, got %q", got[0].Queue)
	}
}

// TestSelectorNoMatchReturnsEmpty: when no app matches the HPA label selector,
// the provider returns an empty list (HPA treats this as "metric unknown" — see
// metricsadapter.GetExternalMetric).
func TestSelectorNoMatchReturnsEmpty(t *testing.T) {
	fake := newFakeConductor("test-org")
	fake.setQueue("app1", "q", intPtr(1), 2)
	srv := httptest.NewServer(fake.handler(t))
	defer srv.Close()

	s := store.NewInMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runPoller(ctx, t, srv.URL, "app1", s, 20*time.Millisecond)
	waitFor(t, 2*time.Second, func() bool { return len(s.ByApp("app1")) == 1 }, "queue observed")

	prov := metricsadapter.New(s)
	values, err := prov.GetExternalMetric(ctx, "default",
		labels.SelectorFromSet(labels.Set{"app": "ghost"}),
		provider.ExternalMetricInfo{Metric: metricsadapter.MetricName})
	if err != nil {
		t.Fatalf("GetExternalMetric: %v", err)
	}
	if len(values.Items) != 0 {
		t.Fatalf("Items len = %d, want 0 (no app matches)", len(values.Items))
	}
}

// TestConductorErrorDoesNotPoison: if ListQueues starts failing, the existing
// (last-good) samples should remain in the store — the metric stays available
// rather than vanishing.
func TestConductorErrorDoesNotPoison(t *testing.T) {
	fake := newFakeConductor("test-org")
	fake.setQueue("app1", "q", intPtr(2), 4) // load 2.0

	// Wrap handler so we can flip to error mode mid-test.
	var failing sync.Once
	var fail bool
	var failMu sync.Mutex
	wrapped := func(w http.ResponseWriter, r *http.Request) {
		failMu.Lock()
		f := fail
		failMu.Unlock()
		if f {
			http.Error(w, "boom", http.StatusInternalServerError)
			return
		}
		fake.handler(t).ServeHTTP(w, r)
	}
	srv := httptest.NewServer(http.HandlerFunc(wrapped))
	defer srv.Close()

	s := store.NewInMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runPoller(ctx, t, srv.URL, "app1", s, 20*time.Millisecond)
	waitFor(t, 2*time.Second, func() bool { return len(s.ByApp("app1")) == 1 }, "initial sample")

	failing.Do(func() {
		failMu.Lock()
		fail = true
		failMu.Unlock()
	})
	// Several ticks of errors must not wipe the store.
	time.Sleep(200 * time.Millisecond)

	got := s.ByApp("app1")
	if len(got) != 1 || got[0].Sample.Load != 2.0 {
		t.Fatalf("after errors, store = %v; want last-good sample (load 2.0)", got)
	}
}
