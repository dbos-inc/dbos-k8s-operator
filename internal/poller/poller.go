// Package poller runs one goroutine per configured DBOS app. The goroutine
// drives two independent tickers against the same Conductor client:
//
//   - Metric tick (high cadence, ~1s): queries every queue's depth +
//     worker_concurrency and writes a Sample to the shared store for HPA.
//     Exponential backoff (capped at MaxBackoff) on failures.
//
//   - Version-manager tick (low cadence, ~30s, opt-in): queries Conductor
//     for non-terminal workflows grouped by application_version. Output is
//     the input signal for per-version Deployment lifecycle. For now this
//     tick only logs the per-version counts; later steps will read it and
//     materialize sibling Deployments for old versions with in-flight work.
//
// Both tickers share one *conductor.Client per goroutine. They DO NOT share
// backoff state — a Conductor flake on ListWorkflows shouldn't slow the
// metrics path, and vice versa.
package poller

import (
	"context"
	"math/rand"
	"time"

	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
	"github.com/dbos-inc/dbos-k8s-operator/internal/versionmanager"
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

	// VersionManagerInterval is the cadence for the per-version pending
	// workflow tick. Zero disables it.
	VersionManagerInterval time.Duration

	// VersionMgr, if non-nil, drives K8s side-effects from the version-
	// manager tick: materializes managed Deployments for old versions with
	// pending work and GCs them when those versions drain. Nil = observation
	// only.
	VersionMgr *versionmanager.Manager
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

	// Version-manager tick: only armed when configured. A nil receive
	// channel in a select case is never ready, so a nil timer here just
	// turns off that arm.
	var versionTimer *time.Timer
	var versionCh <-chan time.Time
	if cfg.VersionManagerInterval > 0 {
		versionTimer = time.NewTimer(0) // first version-tick immediate
		defer versionTimer.Stop()
		versionCh = versionTimer.C
	}

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
		case <-versionCh:
			tickVersions(ctx, client, cfg, logger)
			versionTimer.Reset(cfg.VersionManagerInterval)
		}
	}
}

// tickVersions queries Conductor for both:
//   - per-application_version counts of non-terminal workflows
//   - the full version list (with version_metadata, including pod-template-hash)
//
// then walks every version with pending work and surfaces the archiver's
// intent: each old version either gets an "archive needed" log (we know which
// RS to clone from rollout history) or a warning (we don't know — operator
// can't act). The latest version is always skipped because the live
// Deployment is already serving it.
//
// No store writes yet: later steps will read this signal and materialize
// sibling Deployments. No backoff: a single failed tick is fine, the next
// one retries on the regular cadence.
func tickVersions(ctx context.Context, client *conductor.Client, cfg Config, logger klog.Logger) {
	pollCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	counts, err := client.PendingWorkflowsByVersion(pollCtx, cfg.AppName)
	if err != nil {
		logger.V(2).Info("version manager: PendingWorkflowsByVersion failed", "err", err)
		return
	}
	versions, err := client.ListApplicationVersions(pollCtx, cfg.AppName)
	if err != nil {
		logger.V(2).Info("version manager: ListApplicationVersions failed", "err", err)
		return
	}

	// Index versions by name; identify the latest by version_timestamp.
	byName := make(map[string]conductor.ApplicationVersion, len(versions))
	var latestName string
	var latestTS int64 = -1
	for _, v := range versions {
		byName[v.VersionName] = v
		if v.VersionTimestamp > latestTS {
			latestTS = v.VersionTimestamp
			latestName = v.VersionName
		}
	}

	logger.Info("version manager tick",
		"perVersion", counts,
		"latest", latestName)

	var desired []versionmanager.Desired
	for ver, count := range counts {
		if count == 0 {
			continue
		}
		if ver == latestName {
			// The live Deployment serves this; archiver has nothing to do.
			continue
		}
		v, ok := byName[ver]
		if !ok {
			// Pending workflows on a version Conductor's version list
			// doesn't know about. Should be impossible (workflows have
			// foreign-key-ish provenance), but flag it.
			logger.Info("WARNING pending workflows on unknown version",
				"version", ver, "pending", count)
			continue
		}
		if v.VersionMetadata == nil || v.VersionMetadata.PodTemplateHash == "" {
			logger.Info("WARNING version has pending workflows but no pod_template_hash recorded",
				"version", ver, "pending", count)
			continue
		}
		logger.Info("version requires managed deployment",
			"version", ver,
			"podTemplateHash", v.VersionMetadata.PodTemplateHash,
			"pending", count)
		desired = append(desired, versionmanager.Desired{
			Version: ver,
			Hash:    v.VersionMetadata.PodTemplateHash,
		})
	}

	// If a version manager is configured, reconcile K8s managed Deployments
	// against the desired set. A nil VersionMgr = observation-only mode
	// (versionManager.createArchives disabled in config).
	if cfg.VersionMgr != nil {
		if err := cfg.VersionMgr.Reconcile(pollCtx, desired, logger); err != nil {
			logger.Error(err, "version manager reconcile")
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
