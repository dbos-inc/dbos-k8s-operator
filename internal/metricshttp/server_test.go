package metricshttp

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

func get(t *testing.T, s *Server, app string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/apps/"+app+"/autoscale", nil))
	return rec
}

func TestServeAppNamespaced(t *testing.T) {
	st := store.NewInMemory()
	st.Set("dbos/myapp", store.Result{Body: []byte(`{"desiredExecutors":4}`), DesiredExecutors: 4, PolledAt: time.Now()})
	srv := NewServer(st)
	if rec := get(t, srv, "dbos/myapp"); rec.Code != http.StatusOK {
		t.Errorf("namespaced lookup = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if rec := get(t, srv, "other/myapp"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("wrong namespace = %d, want 503", rec.Code)
	}
}

func TestServeAppStaleness(t *testing.T) {
	st := store.NewInMemory()
	srv := NewServer(st)

	if rec := get(t, srv, "dbos/myapp"); rec.Code != http.StatusServiceUnavailable {
		t.Errorf("no result = %d, want 503", rec.Code)
	}

	st.Set("dbos/myapp", store.Result{Body: []byte(`{"desiredExecutors":4}`), DesiredExecutors: 4, PolledAt: time.Now()})
	if rec := get(t, srv, "dbos/myapp"); rec.Code != http.StatusOK {
		t.Errorf("fresh result = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	st.MarkStale("dbos/myapp")
	rec := get(t, srv, "dbos/myapp")
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("stale result = %d, want 503: %s", rec.Code, rec.Body.String())
	}
	if rec.Header().Get("X-DBOS-Polled-At") != "" {
		t.Error("stale response carries data headers")
	}

	st.Set("dbos/myapp", store.Result{Body: []byte(`{"desiredExecutors":5}`), DesiredExecutors: 5, PolledAt: time.Now()})
	if rec := get(t, srv, "dbos/myapp"); rec.Code != http.StatusOK {
		t.Errorf("recovered result = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	st.Set("dbos/myapp", store.Result{NoPolicy: true, PolledAt: time.Now()})
	if rec := get(t, srv, "dbos/myapp"); rec.Code != http.StatusNotFound {
		t.Errorf("noPolicy = %d, want 404", rec.Code)
	}
}
