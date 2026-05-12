// Package poller runs one goroutine per configured DBOS app. The goroutine
// drives a single ticker against a Conductor client: each tick queries every
// queue's depth + worker_concurrency and writes a Sample to the shared store
// for HPA. Exponential backoff (capped at MaxBackoff) on failures.
package poller

import (
	"context"
	"math/rand"
	"time"

	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// Config is the per-app configuration the poller runs against.
type Config struct {
	AppName     string
	OrgName     string
	Endpoint    string // forwarded to conductor.New; may be empty
	Token       string
	InsecureTLS bool
	Interval    time.Duration
	MaxBackoff  time.Duration
}

// Run blocks until ctx is cancelled. Spawn one of these per configured app.
func Run(ctx context.Context, cfg Config, s store.Store) {
	logger := klog.FromContext(ctx).WithValues("app", cfg.AppName)

	client, err := conductor.New(conductor.Options{
		Endpoint:           cfg.Endpoint,
		OrgName:            cfg.OrgName,
		Token:              cfg.Token,
		InsecureSkipVerify: cfg.InsecureTLS,
	})
	if err != nil {
		logger.Error(err, "build conductor client; poller for app will not run")
		return
	}

	backoff := cfg.Interval
	metricTimer := time.NewTimer(0) // first metric tick immediate
	defer metricTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-metricTimer.C:
			anyErr := tickApp(ctx, client, cfg, s, logger)
			if anyErr {
				backoff *= 2
				if backoff > cfg.MaxBackoff {
					backoff = cfg.MaxBackoff
				}
			} else {
				backoff = cfg.Interval
			}
			metricTimer.Reset(jitter(backoff))
		}
	}
}

// tickApp discovers the app's queues from Conductor, then polls each one's
// depth. Queues without worker_concurrency are skipped (load is undefined).
// Returns true if discovery or any individual depth poll failed.
//
// Queue discovery on every tick (rather than once at startup) means adding a
// queue to the app shows up in HPA's input within one poll interval, with no
// operator restart needed.
func tickApp(ctx context.Context, client *conductor.Client, cfg Config, s store.Store, logger klog.Logger) bool {
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	queues, err := client.ListQueues(pollCtx, cfg.AppName)
	if err != nil {
		logger.V(2).Info("ListQueues failed", "err", err)
		return true
	}

	anyErr := false
	live := make(map[string]struct{}, len(queues))
	for _, q := range queues {
		if q.WorkerConcurrency == nil || *q.WorkerConcurrency <= 0 {
			logger.V(2).Info("queue has no worker_concurrency; skipping",
				"queue", q.Name,
				"workerConcurrency", q.WorkerConcurrency,
				"concurrency", q.Concurrency)
			continue
		}
		if err := pollQueueDepth(pollCtx, client, cfg.AppName, q, s, logger); err != nil {
			anyErr = true
			logger.V(2).Info("queue depth poll failed", "queue", q.Name, "err", err)
			continue
		}
		live[q.Name] = struct{}{}
	}

	// Evict store entries for queues that no longer exist (or that lost their
	// worker_concurrency between ticks). Without this, the max-aggregation
	// would keep returning a stale value indefinitely.
	for _, e := range s.List() {
		if e.App != cfg.AppName {
			continue
		}
		if _, ok := live[e.Queue]; !ok {
			logger.V(1).Info("evicting stale queue sample",
				"queue", e.Queue, "lastObservedAt", e.ObservedAt)
			s.Delete(e.Key)
		}
	}
	return anyErr
}

// pollQueueDepth fetches one queue's depth and writes a Sample to the store.
// Caller has already verified worker_concurrency is set.
func pollQueueDepth(ctx context.Context, client *conductor.Client, app string, q conductor.Queue, s store.Store, logger klog.Logger) error {
	depth, err := client.QueueDepth(ctx, app, q.Name)
	if err != nil {
		return err
	}
	wc := int32(*q.WorkerConcurrency)
	load := float64(depth) / float64(wc)
	logger.V(2).Info("queue polled",
		"queue", q.Name,
		"depth", depth,
		"workerConcurrency", wc,
		"load", load)
	s.Set(store.Key{App: app, Queue: q.Name},
		store.Sample{
			Depth:             depth,
			WorkerConcurrency: wc,
			Load:              load,
			ObservedAt:        time.Now(),
		})
	return nil
}

// jitter returns d ± up to 10%. Used to spread tick alignment across pollers.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := time.Duration(rand.Int63n(int64(d)/5 + 1)) - d/10
	return d + delta
}
