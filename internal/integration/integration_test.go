// Package integration exercises the poll pipeline in-process:
// poller.Run → conductor REST client → store → metricshttp handler.
// Conductor is faked with an httptest.Server serving the parameterless
// autoscale endpoint. No Kubernetes is involved (the kube manager is covered
// by its own unit tests).
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/metricshttp"
	"github.com/dbos-inc/dbos-k8s-operator/internal/poller"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// fakeConductor serves the single endpoint the operator uses.
type fakeConductor struct {
	mu      sync.Mutex
	desired int
}

func (f *fakeConductor) handler(t *testing.T, org, app string) http.Handler {
	mux := http.NewServeMux()
	base := fmt.Sprintf("/v2/orgs/%s/apps/%s", org, app)
	mux.HandleFunc("GET "+base+"/autoscale", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		if r.URL.RawQuery != "" {
			t.Errorf("autoscale GET carried query parameters: %s", r.URL.RawQuery)
		}
		// One entry per application version, latest first (v2 camelCase form).
		_ = json.NewEncoder(w).Encode([]map[string]any{
			{
				"applicationVersion": "v2",
				"isLatest":           true,
				"desiredExecutors":   f.desired,
				"observedAt":         time.Now().UnixMilli(),
			},
			{
				"applicationVersion": "v1",
				"isLatest":           false,
				// An older version's demand must not leak into the latest
				// entry (which KEDA reads); it surfaces only in OldVersions.
				"desiredExecutors": f.desired + 100,
				"observedAt":       time.Now().UnixMilli(),
			},
		})
	})
	return mux
}

func TestPollPipeline(t *testing.T) {
	fake := &fakeConductor{desired: 4}
	srv := httptest.NewServer(fake.handler(t, "org", "myapp"))
	defer srv.Close()

	client, err := conductor.New(conductor.Options{Endpoint: srv.URL, OrgName: "org", Token: "jwt"})
	if err != nil {
		t.Fatal(err)
	}

	s := store.NewInMemory()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var results []store.Result
	var mu sync.Mutex
	go poller.Run(ctx, poller.Config{
		AppName:    "myapp",
		Interval:   10 * time.Millisecond,
		MaxBackoff: 100 * time.Millisecond,
		OnResult: func(r store.Result) {
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		},
	}, client, s)

	waitFor(t, func() bool { _, ok := s.Get("myapp"); return ok })

	r, _ := s.Get("myapp")
	if r.DesiredExecutors != 4 {
		t.Fatalf("desired = %d, want 4", r.DesiredExecutors)
	}
	if len(r.OldVersions) != 1 || r.OldVersions[0].ApplicationVersion != "v1" ||
		r.OldVersions[0].IsLatest || r.OldVersions[0].DesiredExecutors != 104 {
		t.Fatalf("oldVersions = %+v, want [v1 desired 104]", r.OldVersions)
	}

	// A changed recommendation (e.g. after a policy edit in Conductor) lands
	// within one tick — no restart.
	fake.mu.Lock()
	fake.desired = 9
	fake.mu.Unlock()
	waitFor(t, func() bool { r, ok := s.Get("myapp"); return ok && r.DesiredExecutors == 9 })

	// The HTTP endpoint serves the conductor body verbatim (v2 camelCase —
	// KEDA's valueLocation reads desiredExecutors).
	h := metricshttp.NewServer(s).Handler()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/apps/myapp/autoscale", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics endpoint = %d: %s", rec.Code, rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("metrics endpoint body: %v", err)
	}
	if body["desiredExecutors"].(float64) != 9 {
		t.Errorf("served desiredExecutors = %v", body["desiredExecutors"])
	}

	// An app with no reading at all 503s, so KEDA never scales on garbage. An
	// old reading is not garbage: it is served as-is.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/apps/ghost/autoscale", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("unknown app = %d, want 503", rec.Code)
	}

	mu.Lock()
	if len(results) == 0 {
		t.Error("OnResult never fired")
	}
	mu.Unlock()

	// Cancelling the poller evicts the app so a deleted CR stops being served.
	cancel()
	waitFor(t, func() bool { _, ok := s.Get("myapp"); return !ok })
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met within deadline")
}
