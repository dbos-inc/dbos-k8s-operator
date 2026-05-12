package store

import (
	"sort"
	"sync"
	"testing"
	"time"
)

func sample(load float64) Sample {
	return Sample{Depth: 5, WorkerConcurrency: 2, Load: load, ObservedAt: time.Now()}
}

func TestSetAndGet(t *testing.T) {
	s := NewInMemory()
	k := Key{App: "a", Queue: "q1"}

	if _, ok := s.Get(k); ok {
		t.Fatalf("expected miss on empty store")
	}

	s.Set(k, sample(1.5))
	got, ok := s.Get(k)
	if !ok {
		t.Fatalf("expected hit after Set")
	}
	if got.Load != 1.5 {
		t.Fatalf("load = %v, want 1.5", got.Load)
	}

	// Set replaces.
	s.Set(k, sample(2.0))
	got, _ = s.Get(k)
	if got.Load != 2.0 {
		t.Fatalf("load = %v, want 2.0 after replace", got.Load)
	}
}

func TestDeleteRemovesEmptyAppBucket(t *testing.T) {
	s := NewInMemory()
	s.Set(Key{App: "a", Queue: "q1"}, sample(1))
	s.Set(Key{App: "a", Queue: "q2"}, sample(1))

	s.Delete(Key{App: "a", Queue: "q1"})
	if got := s.ByApp("a"); len(got) != 1 {
		t.Fatalf("ByApp after one delete = %v, want len 1", got)
	}

	s.Delete(Key{App: "a", Queue: "q2"})
	if got := s.ByApp("a"); got != nil {
		t.Fatalf("ByApp after all deletes = %v, want nil", got)
	}
	if apps := s.Apps(); len(apps) != 0 {
		t.Fatalf("Apps after all deletes = %v, want empty", apps)
	}

	// Delete of unknown is a no-op.
	s.Delete(Key{App: "unknown", Queue: "q"})
	s.Delete(Key{App: "a", Queue: "gone"})
}

func TestAppsOnlyListsAppsWithSamples(t *testing.T) {
	s := NewInMemory()
	s.Set(Key{App: "a", Queue: "q"}, sample(1))
	s.Set(Key{App: "b", Queue: "q"}, sample(1))
	s.Set(Key{App: "c", Queue: "q1"}, sample(1))
	s.Delete(Key{App: "a", Queue: "q"})

	apps := s.Apps()
	sort.Strings(apps)
	if want := []string{"b", "c"}; !equalStrings(apps, want) {
		t.Fatalf("Apps = %v, want %v", apps, want)
	}
}

func TestByAppUnknown(t *testing.T) {
	s := NewInMemory()
	if got := s.ByApp("nope"); got != nil {
		t.Fatalf("ByApp(unknown) = %v, want nil", got)
	}
}

func TestByAppReturnsAllQueues(t *testing.T) {
	s := NewInMemory()
	s.Set(Key{App: "a", Queue: "q1"}, sample(1))
	s.Set(Key{App: "a", Queue: "q2"}, sample(2))
	s.Set(Key{App: "b", Queue: "q1"}, sample(9))

	got := s.ByApp("a")
	if len(got) != 2 {
		t.Fatalf("ByApp(a) returned %d entries, want 2", len(got))
	}
	for _, e := range got {
		if e.App != "a" {
			t.Fatalf("ByApp(a) returned entry for app %q", e.App)
		}
	}
}

func TestListIsFlat(t *testing.T) {
	s := NewInMemory()
	s.Set(Key{App: "a", Queue: "q1"}, sample(1))
	s.Set(Key{App: "a", Queue: "q2"}, sample(2))
	s.Set(Key{App: "b", Queue: "q1"}, sample(3))

	if got := s.List(); len(got) != 3 {
		t.Fatalf("List len = %d, want 3", len(got))
	}
}

// TestConcurrentSetList exercises the RWMutex under -race.
func TestConcurrentSetList(t *testing.T) {
	s := NewInMemory()
	var wg sync.WaitGroup
	const writers, reads = 8, 200

	stop := make(chan struct{})
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				s.Set(Key{App: "a", Queue: "q"}, sample(float64(w*1000+i)))
			}
		}(w)
	}

	for range reads {
		_ = s.List()
		_ = s.Apps()
		_ = s.ByApp("a")
	}
	close(stop)
	wg.Wait()
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
