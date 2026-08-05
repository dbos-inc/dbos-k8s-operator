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

	r := Result{Body: []byte(`{"desired_executors":3}`), DesiredExecutors: 3, PolledAt: time.Now()}
	s.Set("a", r)
	got, ok := s.Get("a")
	if !ok || got.DesiredExecutors != 3 || string(got.Body) != string(r.Body) {
		t.Fatalf("Get = (%+v, %v)", got, ok)
	}

	s.Set("a", Result{DesiredExecutors: 7})
	if got, _ := s.Get("a"); got.DesiredExecutors != 7 {
		t.Fatalf("Set did not replace: %+v", got)
	}

	s.Set("b", Result{DesiredExecutors: 1})
	if apps := s.Apps(); len(apps) != 2 {
		t.Fatalf("Apps = %v", apps)
	}

	s.Delete("a")
	if _, ok := s.Get("a"); ok {
		t.Fatal("Delete left the entry")
	}
	s.Delete("a") // idempotent
}
