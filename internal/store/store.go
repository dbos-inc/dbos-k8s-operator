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

// Key identifies one queue observation. Namespace stays empty in today's
// ConfigMap-driven mode (single-tenant, no CRDs); a future CRD-driven mode
// would populate it from the CR's metadata.namespace.
type Key struct {
	Namespace string
	App       string
	Queue     string
}

// KeyedSample pairs a Key with its Sample. Returned by List and matchers so
// callers can iterate without exposing the underlying map.
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

	// MatchByNamespace returns every entry whose Namespace equals ns OR is
	// empty (today's wildcard for ConfigMap-driven entries), and whose
	// (app, queue) satisfies predicate. Used by the External Metrics adapter
	// to evaluate HPA label selectors within a namespace scope.
	MatchByNamespace(ns string, predicate func(app, queue string) bool) []KeyedSample
}

// InMemory is the default Store implementation: a sync.RWMutex over a map.
type InMemory struct {
	mu      sync.RWMutex
	entries map[Key]Sample
}

// NewInMemory returns an empty in-memory store.
func NewInMemory() *InMemory {
	return &InMemory{entries: make(map[Key]Sample)}
}

func (s *InMemory) Set(k Key, sample Sample) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries[k] = sample
}

func (s *InMemory) Get(k Key) (Sample, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.entries[k]
	return v, ok
}

func (s *InMemory) Delete(k Key) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, k)
}

func (s *InMemory) List() []KeyedSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]KeyedSample, 0, len(s.entries))
	for k, v := range s.entries {
		out = append(out, KeyedSample{Key: k, Sample: v})
	}
	return out
}

func (s *InMemory) MatchByNamespace(ns string, predicate func(app, queue string) bool) []KeyedSample {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []KeyedSample
	for k, v := range s.entries {
		if k.Namespace != "" && k.Namespace != ns {
			continue
		}
		if !predicate(k.App, k.Queue) {
			continue
		}
		out = append(out, KeyedSample{Key: k, Sample: v})
	}
	return out
}
