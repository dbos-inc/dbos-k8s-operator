// Package store is the shared in-process metric store. The poller writes
// observations; the External Metrics adapter, the Prometheus exporter, and
// (later) the KEDA scaler read from it. The Store is an interface so that
// in-memory can be swapped for a TTL-evicting variant or a Redis-backed
// implementation without touching its consumers.
package store

import (
	"sync"
	"time"
)

// Sample is a point-in-time observation of one queue's load.
type Sample struct {
	Depth             int64
	WorkerConcurrency int32
	Load              float64 // (ENQUEUED + PENDING) / WorkerConcurrency
	ObservedAt        time.Time
}

// Key identifies one queue observation.
type Key struct {
	App   string
	Queue string
}

// KeyedSample pairs a Key with its Sample. Returned by readers so callers
// can iterate without exposing the underlying map.
type KeyedSample struct {
	Key
	Sample
}

// Store is the abstraction over our in-process metrics store.
type Store interface {
	// Set replaces the sample under k.
	Set(k Key, s Sample)

	// Get returns the sample under k, or false if absent.
	Get(k Key) (Sample, bool)

	// Delete removes a single entry; no-op if absent.
	Delete(k Key)

	// List returns every entry. Callers must not mutate the returned slice.
	List() []KeyedSample

	// Apps returns the distinct app names that currently have at least one
	// sample. Used by the External Metrics adapter to evaluate HPA label
	// selectors against the set of observed apps.
	Apps() []string

	// ByApp returns every sample for app. Nil if app is unknown.
	ByApp(app string) []KeyedSample
}

type InMemory struct {
	mu    sync.RWMutex
	byApp map[string]map[string]Sample
}

func NewInMemory() *InMemory {
	return &InMemory{byApp: make(map[string]map[string]Sample)}
}

func (s *InMemory) Set(k Key, sample Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queues, ok := s.byApp[k.App]
	if !ok {
		queues = make(map[string]Sample)
		s.byApp[k.App] = queues
	}
	queues[k.Queue] = sample
}

func (s *InMemory) Get(k Key) (Sample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	queues, ok := s.byApp[k.App]
	if !ok {
		return Sample{}, false
	}
	v, ok := queues[k.Queue]
	return v, ok
}

func (s *InMemory) Delete(k Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	queues, ok := s.byApp[k.App]
	if !ok {
		return
	}
	delete(queues, k.Queue)
	if len(queues) == 0 {
		delete(s.byApp, k.App)
	}
}

func (s *InMemory) List() []KeyedSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []KeyedSample
	for app, queues := range s.byApp {
		for q, v := range queues {
			out = append(out, KeyedSample{Key: Key{App: app, Queue: q}, Sample: v})
		}
	}
	return out
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

func (s *InMemory) ByApp(app string) []KeyedSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	queues, ok := s.byApp[app]
	if !ok {
		return nil
	}
	out := make([]KeyedSample, 0, len(queues))
	for q, v := range queues {
		out = append(out, KeyedSample{Key: Key{App: app, Queue: q}, Sample: v})
	}
	return out
}
