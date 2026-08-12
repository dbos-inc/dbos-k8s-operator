// Package kube reconciles a Deployment from each DBOSApplication CR. The
// operator never writes the main Deployment's spec.replicas — KEDA/HPA own it.
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
	GVRApp = schema.GroupVersionResource{Group: "dbos.dev", Version: "v1alpha1", Resource: "dbosapplications"}

	gvrDeployment = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "deployments"}
)

type Options struct {
	Client    dynamic.Interface
	Conductor *conductor.Client
	Store     store.Store

	Namespace string // empty means all namespaces

	ReconcileInterval time.Duration
	PollInterval      time.Duration
	PollMaxBackoff    time.Duration
}

type Manager struct {
	opts    Options
	pollers map[string]context.CancelFunc // key: namespace/name

	mu              sync.Mutex // guards the dedupe maps below
	lastStatus      map[string]statusKey
	lastOldVersions map[string]map[string]int // app → version → planned replicas
}

// statusKey distinguishes a real recommendation of 0 from a no-policy answer.
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
		if err := m.reconcileOldVersions(ctx, cr, logger); err != nil {
			logger.Error(err, "reconcile old versions", "app", key)
		}
	}

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

func appName(cr *unstructured.Unstructured) string {
	if name, ok, _ := unstructured.NestedString(cr.Object, "spec", "appName"); ok && name != "" {
		return name
	}
	return cr.GetName()
}

// The template snapshot is captured before the apply — it must exist before
// any pod of the version can register.
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
	if err := copySpecFields(deployment, cr); err != nil {
		return err
	}
	if seeded {
		if err := pinAppVersion(deployment, version); err != nil {
			return err
		}
	}
	// "" only for an authored valueFrom pin: nothing to snapshot.
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

var specFieldsNotCopied = map[string]bool{
	"appName":                true,
	"maxOldVersionsReplicas": true,
	"template":               true,
	"replicas":               true,
	"selector":               true,
}

// Main Deployment only — versioned drain Deployments must not surge.
func copySpecFields(deployment map[string]any, cr *unstructured.Unstructured) error {
	spec, ok, err := unstructured.NestedMap(cr.Object, "spec")
	if err != nil || !ok {
		return fmt.Errorf("spec missing or malformed: %v", err)
	}
	target := deployment["spec"].(map[string]any)
	for key, value := range spec {
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
