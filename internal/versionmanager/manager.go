// Package versionmanager materializes a sibling Deployment per (app,
// old-version) that still has in-flight workflows, cloned from the
// historical ReplicaSet whose pod-template-hash matches what Conductor
// recorded for that version.
//
// Why a sibling Deployment instead of scaling the historical RS directly?
// Each RS in K8s rollout history is owned by the live Deployment. Scaling
// such an RS up causes the Deployment controller to immediately scale it
// back to 0 — it's the wrong place to keep state. So we materialize a new
// Deployment, owned by no one (labeled as managed by us), with the
// historical RS's PodSpec.
//
// Lifecycle is declarative: Reconcile(desired) creates any missing managed
// Deployments and deletes any owned managed Deployments not in the desired
// set. Idempotent over repeated calls.
package versionmanager

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

// Labels stamped on every managed Deployment + PodTemplate.
const (
	managedByLabel   = "app.kubernetes.io/managed-by"
	managedByValue   = "dbos-operator"
	appLabel         = "dbos.dev/app"
	versionHashLabel = "dbos.dev/version-hash"

	// podTemplateHashLabel is the K8s-standard label on RSes and pods. Used
	// here to LIST source RSes; never written by the version manager.
	podTemplateHashLabel = "pod-template-hash"
)

// Desired identifies one managed Deployment the version manager should
// ensure exists.
type Desired struct {
	// Version is the Conductor application_version this Deployment serves.
	// Not strictly needed by Reconcile (hash is the only identity used in
	// K8s) but carried for log clarity.
	Version string

	// Hash is the pod-template-hash of the source ReplicaSet to clone from.
	Hash string
}

// Manager owns the K8s side of per-version Deployment lifecycle for one app.
type Manager struct {
	k8s       kubernetes.Interface
	namespace string
	appName   string // also the live Deployment's name (and the prefix for managed Deployments)
}

// New constructs a Manager scoped to a single (namespace, app).
func New(k8sClient kubernetes.Interface, namespace, appName string) *Manager {
	return &Manager{k8s: k8sClient, namespace: namespace, appName: appName}
}

// Reconcile creates managed Deployments for every Desired entry that doesn't
// already exist and deletes any operator-managed Deployment not in the
// desired set. Returns the first error encountered; partial progress (some
// created/deleted, some failed) is normal and recovered on the next tick.
func (m *Manager) Reconcile(ctx context.Context, desired []Desired, logger klog.Logger) error {
	desiredByName := make(map[string]Desired, len(desired))
	for _, d := range desired {
		desiredByName[m.versionDeploymentName(d.Hash)] = d
	}

	// 1. List operator-owned managed Deployments in this namespace for this app.
	existing, err := m.k8s.AppsV1().Deployments(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s,%s=%s",
			managedByLabel, managedByValue,
			appLabel, m.appName,
		),
	})
	if err != nil {
		return fmt.Errorf("list managed deployments: %w", err)
	}
	existingByName := make(map[string]struct{}, len(existing.Items))
	for i := range existing.Items {
		existingByName[existing.Items[i].Name] = struct{}{}
	}

	// 2. Delete managed Deployments not in desired set.
	var firstErr error
	for name := range existingByName {
		if _, want := desiredByName[name]; want {
			continue
		}
		if err := m.k8s.AppsV1().Deployments(m.namespace).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
			logger.Error(err, "delete managed deployment", "name", name)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		logger.Info("managed deployment deleted (version drained or no longer desired)",
			"name", name)
	}

	// 3. Create missing managed Deployments.
	for name, d := range desiredByName {
		if _, ok := existingByName[name]; ok {
			continue
		}
		if err := m.ensure(ctx, d, logger); err != nil {
			logger.Error(err, "ensure managed deployment", "name", name, "hash", d.Hash)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
	}
	return firstErr
}

// ensure resolves the source ReplicaSet for d.Hash and creates the managed
// Deployment.
func (m *Manager) ensure(ctx context.Context, d Desired, logger klog.Logger) error {
	source, err := m.findSourceRS(ctx, d.Hash)
	if err != nil {
		return err
	}
	dep := m.buildVersionDeployment(d, source)
	if _, err := m.k8s.AppsV1().Deployments(m.namespace).Create(ctx, dep, metav1.CreateOptions{}); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Raced with another tick or external apply; not a failure.
			return nil
		}
		return fmt.Errorf("create managed deployment: %w", err)
	}
	logger.Info("managed deployment created",
		"name", dep.Name,
		"version", d.Version,
		"hash", d.Hash,
		"sourceReplicaSet", source.Name)
	return nil
}

// findSourceRS returns a ReplicaSet in our namespace with the given
// pod-template-hash label. There should normally be exactly one; if there
// are multiple (orphaned RSes across deployments, unusual), we pick the
// first.
func (m *Manager) findSourceRS(ctx context.Context, hash string) (*appsv1.ReplicaSet, error) {
	rsList, err := m.k8s.AppsV1().ReplicaSets(m.namespace).List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", podTemplateHashLabel, hash),
	})
	if err != nil {
		return nil, fmt.Errorf("list source replicaset (hash=%s): %w", hash, err)
	}
	if len(rsList.Items) == 0 {
		return nil, fmt.Errorf("no replicaset with %s=%s found in namespace %s (rollout history GC'd?)",
			podTemplateHashLabel, hash, m.namespace)
	}
	return &rsList.Items[0], nil
}

// versionDeploymentName returns the canonical managed Deployment name for a
// given hash.
func (m *Manager) versionDeploymentName(hash string) string {
	return fmt.Sprintf("%s-v-%s", m.appName, hash)
}

// buildVersionDeployment composes a Deployment from the source RS's PodSpec
// and our own labels/selectors. We deliberately drop the source's pod-level
// labels (esp. `app=<appName>`) so the live Service's selector doesn't route
// HTTP traffic to managed pods. Annotations are dropped too; preserving them
// would risk leaking config-hash or other metadata that no longer applies.
func (m *Manager) buildVersionDeployment(d Desired, source *appsv1.ReplicaSet) *appsv1.Deployment {
	replicas := int32(1)
	labels := map[string]string{
		managedByLabel:   managedByValue,
		appLabel:         m.appName,
		versionHashLabel: d.Hash,
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.versionDeploymentName(d.Hash),
			Namespace: m.namespace,
			Labels:    labels,
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: labels,
				},
				Spec: source.Spec.Template.Spec,
			},
		},
	}
}
