package store

import (
	"testing"
	"time"
)

func TestInMemory(t *testing.T) {
	s := NewInMemory()

	if _, ok := s.Get("a"); ok {
		t.Fatal("empty store returned a result")
	}

	r := Result{Body: []byte(`{"desiredExecutors":3}`), DesiredExecutors: 3, PolledAt: time.Now()}
	s.Set("a", r)
	got, ok := s.Get("a")
	if !ok || got.DesiredExecutors != 3 || string(got.Body) != string(r.Body) {
		t.Fatalf("Get = (%+v, %v)", got, ok)
	}

	s.Set("a", Result{DesiredExecutors: 7})
	if got, _ := s.Get("a"); got.DesiredExecutors != 7 {
		t.Fatalf("Set did not replace: %+v", got)
	}

	s.MarkStale("a")
	if got, _ := s.Get("a"); !got.Stale || got.DesiredExecutors != 7 {
		t.Fatalf("MarkStale = %+v, want Stale with the entry intact", got)
	}
	s.Set("a", Result{DesiredExecutors: 8})
	if got, _ := s.Get("a"); got.Stale {
		t.Fatal("Set did not clear Stale")
	}
	s.MarkStale("ghost") // no-op

	s.Delete("a")
	if _, ok := s.Get("a"); ok {
		t.Fatal("Delete left the entry")
	}
	s.Delete("a") // idempotent
}
