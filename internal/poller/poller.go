// Package poller runs one goroutine per configured DBOS app. Each goroutine
// ticks on a configurable cadence, queries Conductor for every queue's
// depth + worker_concurrency, computes load, and writes to the shared store.
//
// Failure handling: ticks that produce errors trigger exponential backoff
// (capped at MaxBackoff) on the entire app's tick interval. A successful
// tick resets the interval to the configured Interval. Jitter (±10%) is
// applied to every reset to avoid thundering herds across apps.
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
	Queues      []string
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
	timer := time.NewTimer(0) // first tick immediate
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			anyErr := tickApp(ctx, client, cfg, s, logger)
			if anyErr {
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

// tickApp polls every queue once and writes successful observations to the
// store. Returns true if any queue's tick failed.
func tickApp(ctx context.Context, client *conductor.Client, cfg Config, s store.Store, logger klog.Logger) bool {
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	anyErr := false
	for _, q := range cfg.Queues {
		if err := pollQueue(pollCtx, client, cfg.AppName, q, s, logger); err != nil {
			anyErr = true
			logger.V(2).Info("queue poll failed", "queue", q, "err", err)
		}
	}
	return anyErr
}

// pollQueue does one queue's two HTTP calls and writes to the store. Returns
// an error on failure (network or misconfiguration). On success, writes a
// fresh Sample to the store under (App, Queue) and returns nil.
func pollQueue(ctx context.Context, client *conductor.Client, app, queue string, s store.Store, logger klog.Logger) error {
	q, err := client.GetQueue(ctx, app, queue)
	if err != nil {
		return err
	}
	if q.WorkerConcurrency == nil || *q.WorkerConcurrency <= 0 {
		// Don't store a load value we can't compute.
		logger.V(2).Info("queue has no worker_concurrency; skipping",
			"queue", queue,
			"workerConcurrency", q.WorkerConcurrency,
			"concurrency", q.Concurrency)
		return nil
	}
	depth, err := client.QueueDepth(ctx, app, queue)
	if err != nil {
		return err
	}
	wc := int32(*q.WorkerConcurrency)
	load := float64(depth) / float64(wc)
	logger.V(2).Info("queue polled",
		"queue", queue,
		"depth", depth,
		"workerConcurrency", wc,
		"load", load)
	s.Set(store.Key{App: app, Queue: queue},
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
