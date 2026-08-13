// Package poller polls Conductor's autoscale recommendation per app.
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

// maxBackoff caps the failure backoff between ticks.
const maxBackoff = 60 * time.Second

type Config struct {
	AppName  string
	StoreKey string
	Interval time.Duration
}

// Run polls until ctx is cancelled; the manager owns store entry cleanup.
func Run(ctx context.Context, cfg Config, client *conductor.Client, s *store.InMemory) {
	logger := klog.FromContext(ctx).WithValues("app", cfg.AppName)

	backoff := cfg.Interval
	timer := time.NewTimer(0) // first tick immediate
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if err := tick(ctx, cfg, client, s, logger); err != nil {
				logger.V(2).Info("poll tick failed", "err", err)
				s.MarkStale(cfg.StoreKey)
				backoff *= 2
				if backoff > maxBackoff {
					backoff = maxBackoff
				}
			} else {
				backoff = cfg.Interval
			}
			timer.Reset(jitter(backoff))
		}
	}
}

// A failed tick leaves the previous result in place.
func tick(ctx context.Context, cfg Config, client *conductor.Client, s *store.InMemory, logger klog.Logger) error {
	tickCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := client.QueueAutoscale(tickCtx, cfg.AppName)
	if errors.Is(err, conductor.ErrNoPolicy) {
		logger.V(2).Info("polled", "noPolicy", true)
		s.Set(cfg.StoreKey, store.Result{NoPolicy: true, PolledAt: time.Now()})
		return nil
	}
	if err != nil {
		return err
	}

	r := store.Result{
		Body:             res.Body,
		DesiredExecutors: res.DesiredExecutors,
		ObservedAt:       res.ObservedAt,
		OldVersions:      res.OldVersions,
		PolledAt:         time.Now(),
	}
	logger.V(2).Info("polled", "desiredExecutors", r.DesiredExecutors, "oldVersions", len(r.OldVersions))
	s.Set(cfg.StoreKey, r)
	return nil
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := time.Duration(rand.Int63n(int64(d)/5+1)) - d/10
	return d + delta
}
