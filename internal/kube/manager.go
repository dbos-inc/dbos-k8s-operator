// Package kube reconciles a Deployment from each DBOSApplication CR. The
// operator never writes the main Deployment's spec.replicas — KEDA/HPA own it.
package kube

import (
	"context"
	"encoding/json"
	"fmt"
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
	GVRApp = schema.GroupVersionResource{Group: "dbos.dev", Version: "v1alpha1", Resource: "dbosapplications"}

	gvrDeployment = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

type Options struct {
	Client    dynamic.Interface
	Conductor *conductor.Client
	Store     *store.InMemory

	Namespace string // empty means all namespaces

	ReconcileInterval time.Duration
	PollInterval      time.Duration
}

type Manager struct {
	opts    Options
	pollers map[string]runningPoller // key: namespace/name/uid/appName
}

type runningPoller struct {
	cancel   context.CancelFunc
	storeKey string
}

func NewManager(opts Options) *Manager {
	return &Manager{
		opts:    opts,
		pollers: map[string]runningPoller{},
	}
}

func crKey(cr *unstructured.Unstructured) string {
	return cr.GetNamespace() + "/" + cr.GetName()
}

func pollerKey(cr *unstructured.Unstructured) string {
	return crKey(cr) + "/" + string(cr.GetUID()) + "/" + appName(cr)
}

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
			for _, p := range m.pollers {
				p.cancel()
			}
			return
		case <-ticker.C:
		}
	}
}

func (m *Manager) reconcileAll(ctx context.Context, logger klog.Logger) error {
	// First, list all DBOSApplications Custom Resources
	list, err := m.opts.Client.Resource(GVRApp).Namespace(m.opts.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list DBOSApplications: %w", err)
	}

	// For each live CR, reconcile its Deployment(s) and ensure a poller is running. Then stop any pollers for removed CRs.
	liveCRs := map[string]bool{}
	livePollers := map[string]bool{}
	for i := range list.Items {
		cr := &list.Items[i]
		key := crKey(cr)
		liveCRs[key] = true
		err := m.reconcileDeployment(ctx, cr, logger)
		if err != nil {
			logger.Error(err, "reconcile deployment", "app", key)
			livePollers[pollerKey(cr)] = true // Just a mark so we can keep the poller running while the CR is live, even if the deployment reconciliation failed.
		} else {
			livePollers[m.ensurePoller(ctx, cr, logger)] = true
			if err = m.reconcileOldVersions(ctx, cr, logger); err != nil {
				logger.Error(err, "reconcile old versions", "app", key)
			}
		}
		m.syncStatus(ctx, cr, err, logger)
	}

	for key, p := range m.pollers {
		if livePollers[key] {
			continue
		}
		logger.Info("stopping poller", "poller", key)
		p.cancel()
		delete(m.pollers, key)
		if !liveCRs[p.storeKey] {
			m.opts.Store.Delete(p.storeKey)
		}
	}
	return nil
}

func appName(cr *unstructured.Unstructured) string {
	if name, ok, _ := unstructured.NestedString(cr.Object, "spec", "appName"); ok && name != "" {
		return name
	}
	return cr.GetName()
}

func (m *Manager) reconcileDeployment(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) error {
	// Get the CR definition as provided (and thus, desired) by the user
	template, ok, err := unstructured.NestedMap(cr.Object, "spec", "template")
	if err != nil || !ok {
		return fmt.Errorf("spec.template missing or malformed: %v", err)
	}
	// Resolve the DBOS__APPVERSION this CR uses: seeded from the template hash.
	version, hash, err := m.resolveAppVersion(ctx, cr, template, logger)
	if err != nil {
		return err
	}
	// Build the deployment manifest
	deployment, err := buildDeployment(cr)
	if err != nil {
		return err
	}
	if err := copySpecFields(deployment, cr); err != nil {
		return err
	}
	// Inject the version in the deployment manifest.
	if err := pinAppVersion(deployment, version); err != nil {
		return err
	}
	if err := m.ensureSnapshot(ctx, cr, version, hash, template, logger); err != nil {
		return err
	}
	// Reconcile the Deployment with kubernetes
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

var specFieldsNotCopied = map[string]bool{
	"appName":  true, // Operator configuration
	"template": true, // Already produced by buildDeployment
	"replicas": true, // Owned by KEDA and/or the operator
	"selector": true, // Operator-owned. Used to "partition" old version pods.
}

// Pass through other user-provided fields, outside of spec.template. Latest deployment only.
func copySpecFields(deployment map[string]any, cr *unstructured.Unstructured) error {
	spec, ok, err := unstructured.NestedMap(cr.Object, "spec")
	if err != nil || !ok {
		return fmt.Errorf("spec missing or malformed: %v", err)
	}
	target := deployment["spec"].(map[string]any)
	for key, value := range spec {
		// Some fields are ignored. See map above.
		if specFieldsNotCopied[key] {
			continue
		}
		target[key] = value
	}
	return nil
}

// spec.replicas is deliberately never applied so the autoscaler owns it.
func buildDeployment(cr *unstructured.Unstructured) (map[string]any, error) {
	template, ok, err := unstructured.NestedMap(cr.Object, "spec", "template")
	if err != nil || !ok {
		return nil, fmt.Errorf("spec.template missing or malformed: %v", err)
	}
	return assembleDeployment(cr, template)
}

// template is mutated (label injection); callers pass a copy they own.
func assembleDeployment(cr *unstructured.Unstructured, template map[string]any) (map[string]any, error) {
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
			"ownerReferences": []any{crOwnerReference(cr)}, // The Deployment is owned by the CR: mark the relationship so the Deployment is deleted when the CR is deleted.
		},
		"spec": map[string]any{
			"selector": map[string]any{
				"matchLabels": map[string]any{"app": cr.GetName()},
			},
			"template": template, // Pass through user-provided spec.template, with injected labels and pinned DBOS__APPVERSION if any.
		},
	}, nil
}

func crOwnerReference(cr *unstructured.Unstructured) map[string]any {
	return map[string]any{
		"apiVersion": GVRApp.Group + "/" + GVRApp.Version,
		"kind":       "DBOSApplication",
		"name":       cr.GetName(),
		"uid":        string(cr.GetUID()),
		"controller": true, // A k8s object can have only one controller -- in this case, the DBOSApplication CR.
	}
}

func (m *Manager) ensurePoller(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) string {
	app := appName(cr)
	key := pollerKey(cr)
	if _, running := m.pollers[key]; running {
		return key
	}
	pollCtx, cancel := context.WithCancel(ctx)
	m.pollers[key] = runningPoller{cancel: cancel, storeKey: crKey(cr)}
	logger.Info("starting poller", "app", crKey(cr), "conductorApp", app)
	cfg := poller.Config{
		AppName:  app,
		StoreKey: crKey(cr),
		Interval: m.opts.PollInterval,
	}
	go poller.Run(pollCtx, cfg, m.opts.Conductor, m.opts.Store)
	return key
}

// Update the status of the CR for observability
func (m *Manager) syncStatus(ctx context.Context, cr *unstructured.Unstructured, reconcileErr error, logger klog.Logger) {
	current, _, _ := unstructured.NestedMap(cr.Object, "status")
	status := map[string]any{
		"observedGeneration": cr.GetGeneration(),
		"conditions":         []any{readyCondition(cr, reconcileErr)},
	}
	if r, ok := m.opts.Store.Get(crKey(cr)); ok {
		status["desiredExecutors"] = int64(r.DesiredExecutors)
		status["noPolicy"] = r.NoPolicy
		status["observedAt"] = r.ObservedAt
		status["lastPolledAt"] = r.PolledAt.UTC().Format(time.RFC3339)
	} else {
		for _, k := range []string{"desiredExecutors", "noPolicy", "observedAt", "lastPolledAt"} {
			if v, ok := current[k]; ok {
				status[k] = v
			}
		}
	}
	if !statusChanged(current, status) {
		return
	}
	patch := map[string]any{
		"apiVersion": GVRApp.Group + "/" + GVRApp.Version,
		"kind":       "DBOSApplication",
		"metadata":   map[string]any{"name": cr.GetName(), "namespace": cr.GetNamespace()},
		"status":     status,
	}
	payload, err := json.Marshal(patch)
	if err != nil {
		return
	}
	_, err = m.opts.Client.Resource(GVRApp).Namespace(cr.GetNamespace()).Patch(
		ctx, cr.GetName(), types.ApplyPatchType, payload,
		metav1.PatchOptions{FieldManager: fieldManager, Force: ptr.To(true)}, "status")
	if err != nil {
		logger.Error(err, "status patch failed", "app", crKey(cr))
	}
}

func readyCondition(cr *unstructured.Unstructured, reconcileErr error) map[string]any {
	condStatus, reason, message := "True", "Reconciled", ""
	if reconcileErr != nil {
		condStatus, reason, message = "False", "ReconcileError", reconcileErr.Error()
	}
	transition := time.Now().UTC().Format(time.RFC3339)
	if prev := currentReadyCondition(cr); prev["status"] == condStatus {
		if s, ok := prev["lastTransitionTime"].(string); ok && s != "" {
			transition = s
		}
	}
	return map[string]any{
		"type":               "Ready",
		"status":             condStatus,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": transition,
		"observedGeneration": cr.GetGeneration(),
	}
}

func currentReadyCondition(cr *unstructured.Unstructured) map[string]any {
	status, _, _ := unstructured.NestedMap(cr.Object, "status")
	return readyConditionOf(status)
}

func readyConditionOf(status map[string]any) map[string]any {
	conditions, _ := status["conditions"].([]any)
	for _, c := range conditions {
		if m, ok := c.(map[string]any); ok && m["type"] == "Ready" {
			return m
		}
	}
	return nil
}

func statusChanged(current, desired map[string]any) bool {
	for _, k := range []string{"desiredExecutors", "noPolicy", "observedGeneration"} {
		if current[k] != desired[k] {
			return true
		}
	}
	prev := readyConditionOf(current)
	if prev == nil {
		return true
	}
	next := readyConditionOf(desired)
	for _, k := range []string{"status", "reason", "message", "observedGeneration"} {
		if prev[k] != next[k] {
			return true
		}
	}
	return false
}
