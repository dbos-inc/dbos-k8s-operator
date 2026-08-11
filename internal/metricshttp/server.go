// Package metricshttp serves the operator's poll results over plain HTTP for
// KEDA's metrics-api scaler. The response is Conductor's queue-based
// autoscaling JSON verbatim (snake_case), so a ScaledObject that used to
// point at Conductor keeps its valueLocation (desiredExecutors) and only
// changes its url. Authorization headers are accepted and ignored — the
// endpoint is in-cluster and read-only.
package metricshttp

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// Server serves GET /apps/{app}/autoscale and GET /healthz.
//
// The latest in-memory reading is served however old it is: the endpoint has
// no staleness cutoff. A failing poller therefore holds the last known
// recommendation rather than answering 503; X-DBOS-Polled-At carries the age
// so a consumer that cares can decide for itself.
type Server struct {
	store store.Store
}

func NewServer(s store.Store) *Server {
	return &Server{store: s}
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /apps/{app}/autoscale", s.serveApp)
	return mux
}

func (s *Server) serveApp(w http.ResponseWriter, r *http.Request) {
	app := r.PathValue("app")
	result, ok := s.store.Get(app)
	if !ok {
		errJSON(w, http.StatusServiceUnavailable, "no successful poll for app "+app)
		return
	}
	if result.NoPolicy {
		// Mirror Conductor's 404: no policy, no recommendation. KEDA's scaler
		// errors out and the HPA holds the current replica count.
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
