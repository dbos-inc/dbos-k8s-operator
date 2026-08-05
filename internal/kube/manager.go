// Package kube makes the operator own its apps' Deployments. A DBOSApplication
// custom resource declares the app's pod template; the manager reconciles a
// Deployment from each CR via server-side apply (field manager "dbos-operator",
// owner reference for garbage collection) and runs one Conductor poller per CR.
// Scaling stays external: the operator never writes spec.replicas, so KEDA/HPA
// own that field, driven by the desired_executors this operator serves.
//
// Reconciliation is poll-based (ReconcileInterval), not informer-based: at
// this scale a LIST every few seconds is cheaper than watch machinery, and a
// missed event only delays convergence by one interval.
package kube

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/poller"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

const fieldManager = "dbos-operator"

var (
	// GVRApp is the DBOSApplication custom resource.
	GVRApp = schema.GroupVersionResource{Group: "dbos.dev", Version: "v1alpha1", Resource: "dbosapplications"}

	gvrDeployment = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

// Options wires the manager.
type Options struct {
	Client    dynamic.Interface
	Conductor *conductor.Client
	Store     store.Store

	// Namespace limits which DBOSApplications are reconciled; empty means all.
	Namespace string

	ReconcileInterval time.Duration
	PollInterval      time.Duration
	PollMaxBackoff    time.Duration
}

// Manager reconciles DBOSApplications and keeps one poller per app alive.
type Manager struct {
	opts    Options
	pollers map[string]context.CancelFunc // key: namespace/name
	// mu guards the dedupe maps below: they are written from each app's
	// poller goroutine (via OnResult) and cleaned up from the reconcile loop.
	mu sync.Mutex
	// lastStatus dedupes status PATCHes: reconcile-loop status writes happen
	// only when the desired count or error state changed.
	lastStatus map[string]statusKey
	// lastOldVersions dedupes the old-version reports: version → planned
	// replicas per app, logged again only when it changes.
	lastOldVersions map[string]map[string]int
}

// statusKey is the part of a poll result the CR status reflects. Both fields
// matter: a real recommendation of 0 (nothing queued) and a no-policy answer
// both carry desiredExecutors 0, and only noPolicy tells them apart.
type statusKey struct {
	desiredExecutors int
	noPolicy         bool
}

func NewManager(opts Options) *Manager {
	return &Manager{
		opts:            opts,
		pollers:         map[string]context.CancelFunc{},
		lastStatus:      map[string]statusKey{},
		lastOldVersions: map[string]map[string]int{},
	}
}

// Run reconciles until ctx is cancelled, then stops every poller.
func (m *Manager) Run(ctx context.Context) {
	logger := klog.FromContext(ctx).WithValues("component", "kube-manager")
	ticker := time.NewTicker(m.opts.ReconcileInterval)
	defer ticker.Stop()
	for {
		if err := m.reconcileAll(ctx, logger); err != nil {
			logger.Error(err, "reconcile pass failed")
		}
		select {
		case <-ctx.Done():
			for _, cancel := range m.pollers {
				cancel()
			}
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) reconcileAll(ctx context.Context, logger klog.Logger) error {
	var list *unstructured.UnstructuredList
	var err error
	if m.opts.Namespace != "" {
		list, err = m.opts.Client.Resource(GVRApp).Namespace(m.opts.Namespace).List(ctx, metav1.ListOptions{})
	} else {
		list, err = m.opts.Client.Resource(GVRApp).List(ctx, metav1.ListOptions{})
	}
	if err != nil {
		return fmt.Errorf("list DBOSApplications: %w", err)
	}

	live := map[string]bool{}
	for i := range list.Items {
		cr := &list.Items[i]
		key := cr.GetNamespace() + "/" + cr.GetName()
		live[key] = true
		if err := m.reconcileDeployment(ctx, cr, logger); err != nil {
			logger.Error(err, "reconcile deployment", "app", key)
			continue
		}
		m.ensurePoller(ctx, cr, logger)
		// Old versions are reconciled from the stored poll result rather than
		// from the poller goroutine: deletion consults the live Deployment
		// list, so a periodic pass also converges after an operator restart.
		if err := m.reconcileOldVersions(ctx, cr, logger); err != nil {
			logger.Error(err, "reconcile old versions", "app", key)
		}
	}

	// Stop pollers for CRs that disappeared. Their Deployments are removed by
	// garbage collection through the owner reference, not by us.
	for key, cancel := range m.pollers {
		if !live[key] {
			logger.Info("DBOSApplication removed; stopping poller", "app", key)
			cancel()
			delete(m.pollers, key)
			m.mu.Lock()
			delete(m.lastStatus, key)
			delete(m.lastOldVersions, key)
			m.mu.Unlock()
		}
	}
	return nil
}

// appName returns the Conductor application name: spec.appName, defaulting to
// the CR's metadata.name.
func appName(cr *unstructured.Unstructured) string {
	if name, ok, _ := unstructured.NestedString(cr.Object, "spec", "appName"); ok && name != "" {
		return name
	}
	return cr.GetName()
}

// reconcileDeployment server-side-applies the Deployment derived from the CR,
// seeding DBOS__APPVERSION (unless the author pinned it) and capturing the
// version's template snapshot first — the snapshot must exist before any pod
// of the version can register, or a fast follow-up rollout could orphan it.
func (m *Manager) reconcileDeployment(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) error {
	template, ok, err := unstructured.NestedMap(cr.Object, "spec", "template")
	if err != nil || !ok {
		return fmt.Errorf("spec.template missing or malformed: %v", err)
	}
	version, seeded, err := m.resolveAppVersion(ctx, cr, template, logger)
	if err != nil {
		return err
	}
	deployment, err := buildDeployment(cr)
	if err != nil {
		return err
	}
	if err := setStrategy(deployment, cr); err != nil {
		return err
	}
	if seeded {
		if err := pinAppVersion(deployment, version); err != nil {
			return err
		}
	}
	// version can be "" only for an authored valueFrom pin: nothing to bind.
	if version != "" {
		hash, err := hashTemplate(template)
		if err != nil {
			return err
		}
		if err := m.ensureSnapshot(ctx, cr, version, hash, template, logger); err != nil {
			return err
		}
	}
	payload, err := json.Marshal(deployment)
	if err != nil {
		return err
	}
	_, err = m.opts.Client.Resource(gvrDeployment).Namespace(cr.GetNamespace()).Patch(
		ctx, cr.GetName(), types.ApplyPatchType, payload,
		metav1.PatchOptions{FieldManager: fieldManager, Force: ptr.To(true)})
	if err != nil {
		return fmt.Errorf("apply deployment: %w", err)
	}
	return nil
}

// setStrategy copies the CR's spec.strategy — same shape and field names as
// Deployment.spec.strategy — onto the main Deployment, verbatim. Only the
// main Deployment gets it: versioned drain Deployments must not surge on
// their own, since an authored maxSurge is also the drain fleet's total pod
// budget (see drainBudget). A CR without a strategy leaves the field to the
// Deployment defaults.
func setStrategy(deployment map[string]any, cr *unstructured.Unstructured) error {
	strategy, ok, err := unstructured.NestedMap(cr.Object, "spec", "strategy")
	if err != nil {
		return fmt.Errorf("spec.strategy malformed: %v", err)
	}
	if !ok || len(strategy) == 0 {
		return nil
	}
	return unstructured.SetNestedMap(deployment, strategy, "spec", "strategy")
}

// buildDeployment derives the Deployment manifest from a DBOSApplication. The
// pod template passes through verbatim; the operator only asserts the
// name/namespace, the selector labels, and the owner reference.
// spec.replicas is deliberately never applied so the autoscaler owns it (a new
// Deployment defaults to 1), and spec.strategy is set by reconcileDeployment
// for the main Deployment only — never here, where versioned Deployments
// would inherit it.
func buildDeployment(cr *unstructured.Unstructured) (map[string]any, error) {
	template, ok, err := unstructured.NestedMap(cr.Object, "spec", "template")
	if err != nil || !ok {
		return nil, fmt.Errorf("spec.template missing or malformed: %v", err)
	}
	return assembleDeployment(cr, template)
}

// assembleDeployment wraps a pod template — the CR's current one, or an old
// version's snapshot — in the Deployment scaffolding. template is mutated
// (label injection); callers pass a copy they own.
func assembleDeployment(cr *unstructured.Unstructured, template map[string]any) (map[string]any, error) {
	// The selector must match the pod labels; inject app=<name> into the
	// template rather than trusting the CR author to keep them aligned.
	labels, _, _ := unstructured.NestedMap(template, "metadata", "labels")
	if labels == nil {
		labels = map[string]any{}
	}
	labels["app"] = cr.GetName()
	if err := unstructured.SetNestedMap(template, labels, "metadata", "labels"); err != nil {
		return nil, err
	}

	return map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      cr.GetName(),
			"namespace": cr.GetNamespace(),
			"labels": map[string]any{
				"app":                          cr.GetName(),
				"app.kubernetes.io/managed-by": fieldManager,
			},
			"ownerReferences": []any{crOwnerReference(cr)},
		},
		"spec": map[string]any{
			"selector": map[string]any{
				"matchLabels": map[string]any{"app": cr.GetName()},
			},
			"template": template,
		},
	}, nil
}

// crOwnerReference is the controller owner reference every derived object
// carries, so a deleted CR sweeps its Deployments and template snapshots.
func crOwnerReference(cr *unstructured.Unstructured) map[string]any {
	return map[string]any{
		"apiVersion":         GVRApp.Group + "/" + GVRApp.Version,
		"kind":               "DBOSApplication",
		"name":               cr.GetName(),
		"uid":                string(cr.GetUID()),
		"controller":         true,
		"blockOwnerDeletion": true,
	}
}

// ensurePoller starts the app's poller if it isn't running. The poller's
// OnResult hook patches the CR status when the desired count changes.
func (m *Manager) ensurePoller(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) {
	key := cr.GetNamespace() + "/" + cr.GetName()
	if _, running := m.pollers[key]; running {
		return
	}
	pollCtx, cancel := context.WithCancel(ctx)
	m.pollers[key] = cancel
	namespace, name, app := cr.GetNamespace(), cr.GetName(), appName(cr)
	logger.Info("starting poller", "app", key, "conductorApp", app)
	cfg := poller.Config{
		AppName:    app,
		Interval:   m.opts.PollInterval,
		MaxBackoff: m.opts.PollMaxBackoff,
		OnResult: func(r store.Result) {
			m.updateStatus(pollCtx, namespace, name, key, r, logger)
		},
	}
	go poller.Run(pollCtx, cfg, m.opts.Conductor, m.opts.Store)
}

// updateStatus patches the CR's status subresource when desiredExecutors
// changed since the last write, keeping API churn at one PATCH per scale
// change instead of one per tick.
func (m *Manager) updateStatus(ctx context.Context, namespace, name, key string, r store.Result, logger klog.Logger) {
	current := statusKey{desiredExecutors: r.DesiredExecutors, noPolicy: r.NoPolicy}
	m.mu.Lock()
	last, ok := m.lastStatus[key]
	m.mu.Unlock()
	if ok && last == current {
		return
	}
	status := map[string]any{
		"apiVersion": GVRApp.Group + "/" + GVRApp.Version,
		"kind":       "DBOSApplication",
		"metadata":   map[string]any{"name": name, "namespace": namespace},
		"status": map[string]any{
			// 0 with noPolicy=true when Conductor has no policy for the app;
			// 0 with noPolicy=false is a real "nothing queued" recommendation,
			// which is why the dedupe above keys on both.
			"desiredExecutors": r.DesiredExecutors,
			"noPolicy":         r.NoPolicy,
			"observedAt":       r.ObservedAt,
			"lastPolledAt":     r.PolledAt.UTC().Format(time.RFC3339),
		},
	}
	payload, err := json.Marshal(status)
	if err != nil {
		return
	}
	_, err = m.opts.Client.Resource(GVRApp).Namespace(namespace).Patch(
		ctx, name, types.ApplyPatchType, payload,
		metav1.PatchOptions{FieldManager: fieldManager, Force: ptr.To(true)}, "status")
	if err != nil {
		logger.V(2).Error(err, "status patch failed", "app", key)
		return
	}
	m.mu.Lock()
	m.lastStatus[key] = current
	m.mu.Unlock()
}
