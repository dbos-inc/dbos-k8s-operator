// Package metricshttp serves the poll results over plain HTTP for KEDA's
// metrics-api scaler.
package metricshttp

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// A result the poller marked stale is answered with 503 instead of served:
// KEDA propagates no metric on scaler error, so the HPA holds the current
// replica count rather than acting on stale data.
type Server struct {
	store *store.InMemory
}

func NewServer(s *store.InMemory) *Server {
	return &Server{store: s}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /apps/{namespace}/{name}/autoscale", s.serveApp)
	return mux
}

func (s *Server) serveApp(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("namespace") + "/" + r.PathValue("name")
	result, ok := s.store.Get(app)
	if !ok {
		errJSON(w, http.StatusServiceUnavailable, "no successful poll for app "+app)
		return
	}
	if result.Stale {
		errJSON(w, http.StatusServiceUnavailable, fmt.Sprintf(
			"conductor unreachable for app %s; last successful poll at %s",
			app, result.PolledAt.UTC().Format(time.RFC3339)))
		return
	}
	if result.NoPolicy {
		errJSON(w, http.StatusNotFound, "no autoscaling policy installed for app "+app)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-DBOS-Polled-At", result.PolledAt.UTC().Format(time.RFC3339))
	w.Header().Set("X-DBOS-Desired-Executors", strconv.Itoa(result.DesiredExecutors))
	if _, err := w.Write(result.Body); err != nil {
		klog.V(2).ErrorS(err, "write response", "app", app)
	}
}

func errJSON(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
