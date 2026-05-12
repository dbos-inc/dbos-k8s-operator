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
	Queues      []string
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
