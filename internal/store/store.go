// Package store is the shared in-process store of per-app poll results.
package store

import (
	"sync"
	"time"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
)

type Result struct {
	Body             []byte // latest version's entry verbatim, re-served to KEDA
	DesiredExecutors int
	ObservedAt       int64     // epoch ms
	PolledAt         time.Time // when this poll completed
	OldVersions      []conductor.VersionRecommendation
	NoPolicy         bool // Conductor answered 404: no autoscaling policy
	Stale            bool // the poll after this result failed; cleared by the next Set
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

// MarkStale flags the app's result as stale, keeping the rest of the entry
// (PolledAt included) intact. No-op if the app has no result.
func (s *InMemory) MarkStale(app string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r, ok := s.byApp[app]; ok {
		r.Stale = true
		s.byApp[app] = r
	}
}

func (s *InMemory) Delete(app string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.byApp, app)
}
