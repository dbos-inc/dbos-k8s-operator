// Package poller runs one goroutine per DBOSApplication. Each tick it asks
// Conductor for the desired executor count — Conductor computes it from the
// app's stored autoscaling policy, so policy edits take effect within one
// interval, no restart — and writes the response to the shared store for the
// KEDA-facing HTTP endpoint. Exponential backoff (capped at MaxBackoff) on
// failures.
package poller

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// Config is the per-app configuration the poller runs against.
type Config struct {
	AppName    string
	Interval   time.Duration
	MaxBackoff time.Duration

	// OnResult, when set, is invoked after every successful tick (used by the
	// kube manager to update the DBOSApplication's status).
	OnResult func(r store.Result)
}

// Run polls until ctx is cancelled. The store entry is deleted on exit so a
// removed app stops being served rather than going stale.
func Run(ctx context.Context, cfg Config, client *conductor.Client, s store.Store) {
	logger := klog.FromContext(ctx).WithValues("app", cfg.AppName)

	backoff := cfg.Interval
	timer := time.NewTimer(0) // first tick immediate
	defer timer.Stop()
	defer s.Delete(cfg.AppName)

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := tick(ctx, cfg, client, s, logger); err != nil {
				logger.V(2).Error(err, "poll tick failed")
				backoff *= 2
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
			} else {
				backoff = cfg.Interval
			}
			timer.Reset(jitter(backoff))
		}
	}
}

// tick fetches the autoscale recommendation and stores the result. A failed
// tick leaves the previous result in place — the HTTP endpoint applies its
// own staleness cutoff.
func tick(ctx context.Context, cfg Config, client *conductor.Client, s store.Store, logger klog.Logger) error {
	tickCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := client.QueueAutoscale(tickCtx, cfg.AppName)
	if errors.Is(err, conductor.ErrNoPolicy) {
		// A normal state, not a failure: keep polling at the regular interval
		// so an installed policy takes effect within one tick.
		r := store.Result{NoPolicy: true, PolledAt: time.Now()}
		logger.V(2).Info("polled", "noPolicy", true)
		s.Set(cfg.AppName, r)
		if cfg.OnResult != nil {
			cfg.OnResult(r)
		}
		return nil
	}
	if err != nil {
		return err
	}

	r := store.Result{
		Body:             res.Body,
		DesiredExecutors: res.DesiredExecutors, // Latest versions
		ObservedAt:       res.ObservedAt,
		OldVersions:      res.OldVersions,
		PolledAt:         time.Now(),
	}
	logger.V(2).Info("polled", "desiredExecutors", r.DesiredExecutors, "oldVersions", len(r.OldVersions))
	s.Set(cfg.AppName, r)
	if cfg.OnResult != nil {
		cfg.OnResult(r)
	}
	return nil
}

// jitter returns d ± up to 10%. Used to spread tick alignment across pollers.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := time.Duration(rand.Int63n(int64(d)/5+1)) - d/10
	return d + delta
}
