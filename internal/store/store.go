// Package store is the shared in-process result store. Each app's poller
// writes the latest Conductor autoscale response; the HTTP metrics endpoint
// (polled by KEDA) and the CR status updater read from it. The Store is an
// interface so in-memory can be swapped without touching its consumers.
package store

import (
	"sync"
	"time"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
)

// Result is the latest successful queue-based-autoscaling poll for one app.
type Result struct {
	// Body is the latest version's entry of Conductor's response, verbatim
	// (snake_case v1 JSON), re-served as-is to KEDA so its valueLocation
	// (desiredExecutors) keeps working.
	Body             []byte
	DesiredExecutors int
	ObservedAt       int64     // epoch ms the aggregate was computed (from the response)
	PolledAt         time.Time // when this poll completed
	// OldVersions are the response's non-latest entries: older application
	// versions still holding work, each needing executors of its own version.
	OldVersions []conductor.VersionRecommendation
	// NoPolicy records that Conductor answered 404 — the app has no
	// autoscaling policy installed. Body is empty; the metrics endpoint 404s
	// so the HPA holds the current replica count.
	NoPolicy bool
}

// Store is the abstraction over the in-process result store.
type Store interface {
	// Set replaces the app's latest result.
	Set(app string, r Result)

	// Get returns the app's latest result, or false if it has none.
	Get(app string) (Result, bool)

	// Delete removes an app's result; no-op if absent.
	Delete(app string)

	// Apps returns the app names that currently have a result.
	Apps() []string
}

type InMemory struct {
	mu    sync.RWMutex
	byApp map[string]Result
}

func NewInMemory() *InMemory {
	return &InMemory{byApp: make(map[string]Result)}
}

func (s *InMemory) Set(app string, r Result) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byApp[app] = r
}

func (s *InMemory) Get(app string) (Result, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.byApp[app]
	return r, ok
}

func (s *InMemory) Delete(app string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byApp, app)
}

func (s *InMemory) Apps() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.byApp))
	for app := range s.byApp {
		out = append(out, app)
	}
	return out
}
