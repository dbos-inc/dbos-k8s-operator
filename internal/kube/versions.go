// One Deployment per old version still present in Conductor's autoscale response
package kube

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

const versionLabel = "dbos.dev/app-version"

// Versions here are always operator-seeded (the snapshot gate refuses
// anything else), so they are name- and label-safe verbatim.
// The latest version keeps the bare CR name (KEDA targets it by name).
func versionDeploymentName(app, version string) string {
	return app + "-" + version
}

// Build a Deployment for a specific version, with the given template and replica count.
func buildVersionDeployment(cr *unstructured.Unstructured, version string, replicas int, template map[string]any) (map[string]any, error) {
	deployment, err := assembleDeployment(cr, template)
	if err != nil {
		return nil, err
	}
	if err := unstructured.SetNestedField(deployment, versionDeploymentName(cr.GetName(), version), "metadata", "name"); err != nil {
		return nil, err
	}
	labels, _, _ := unstructured.NestedMap(deployment, "metadata", "labels")
	if labels == nil {
		labels = map[string]any{}
	}
	labels[versionLabel] = version
	if err := unstructured.SetNestedMap(deployment, labels, "metadata", "labels"); err != nil {
		return nil, err
	}
	// The selector must not include app=<name>: the main Deployment's immutable
	// selector is exactly that, and drain pods must not match it.
	if err := unstructured.SetNestedMap(deployment, map[string]any{versionLabel: version}, "spec", "selector", "matchLabels"); err != nil {
		return nil, err
	}
	podLabels, _, _ := unstructured.NestedMap(deployment, "spec", "template", "metadata", "labels")
	if podLabels == nil {
		podLabels = map[string]any{}
	}
	delete(podLabels, "app")
	podLabels[versionLabel] = version
	if err := unstructured.SetNestedMap(deployment, podLabels, "spec", "template", "metadata", "labels"); err != nil {
		return nil, err
	}
	if err := unstructured.SetNestedField(deployment, int64(replicas), "spec", "replicas"); err != nil {
		return nil, err
	}
	if err := pinAppVersion(deployment, version); err != nil {
		return nil, err
	}
	return deployment, nil
}

// Write our version in the manifest -- ignore any existing value.
func pinAppVersion(deployment map[string]any, version string) error {
	containers, ok, err := unstructured.NestedSlice(deployment, "spec", "template", "spec", "containers")
	if err != nil || !ok {
		return fmt.Errorf("containers missing or malformed: %v", err)
	}
	for i, item := range containers {
		container, ok := item.(map[string]any)
		if !ok {
			continue
		}
		env, _, _ := unstructured.NestedSlice(container, "env")
		kept := make([]any, 0, len(env)+1)
		for _, e := range env {
			if v, ok := e.(map[string]any); ok && v["name"] == "DBOS__APPVERSION" {
				continue
			}
			kept = append(kept, e)
		}
		kept = append(kept, map[string]any{"name": "DBOS__APPVERSION", "value": version})
		if err := unstructured.SetNestedSlice(container, kept, "env"); err != nil {
			return err
		}
		containers[i] = container
	}
	return unstructured.SetNestedSlice(deployment, containers, "spec", "template", "spec", "containers")
}

func (m *Manager) reconcileOldVersions(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) error {
	key := crKey(cr)

	// How many old versions do we have sizing recommendations for?
	result, ok := m.opts.Store.Get(key)
	if !ok || result.NoPolicy {
		return nil
	}
	// Results can be marked "stale" when Conductor is unresponsive.
	if result.Stale {
		logger.V(2).Info("skipping old-version reconcile on a stale poll result", "app", key)
		return nil
	}

	keep := make(map[string]bool, len(result.OldVersions))
	var errs []error
	for _, v := range result.OldVersions {
		keep[v.ApplicationVersion] = true
		replicas := max(v.DesiredExecutors, 0) // v.DesiredExecutors should be >= 0, but be defensive
		if err := m.applyVersionDeployment(ctx, cr, v.ApplicationVersion, replicas); err != nil {
			errs = append(errs, err)
			continue
		}
		logger.V(2).Info("old version sized to its recommendation",
			"app", key, "version", v.ApplicationVersion, "replicas", replicas,
			"deployment", versionDeploymentName(cr.GetName(), v.ApplicationVersion))
	}

	if err := m.deleteDepartedVersions(ctx, cr, keep, logger); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// A version with no snapshot is an error, never rebuilt from the current CR
// template — the wrong pod spec under an old identity.
func (m *Manager) applyVersionDeployment(ctx context.Context, cr *unstructured.Unstructured, version string, replicas int) error {
	template, ok, err := m.snapshotTemplate(ctx, cr, version)
	if err != nil {
		return fmt.Errorf("snapshot for version %q: %w", version, err)
	}
	if !ok {
		return fmt.Errorf("no template snapshot for version %q; refusing to run it on the current template", version)
	}
	deployment, err := buildVersionDeployment(cr, version, replicas, template)
	if err != nil {
		return fmt.Errorf("build deployment for version %q: %w", version, err)
	}
	payload, err := json.Marshal(deployment)
	if err != nil {
		return fmt.Errorf("marshal deployment for version %q: %w", version, err)
	}
	_, err = m.opts.Client.Resource(gvrDeployment).Namespace(cr.GetNamespace()).Patch(
		ctx, versionDeploymentName(cr.GetName(), version), types.ApplyPatchType, payload,
		metav1.PatchOptions{FieldManager: fieldManager, Force: new(true)})
	if err != nil {
		return fmt.Errorf("apply deployment for version %q: %w", version, err)
	}
	return nil
}

// List all the current versioned deployments for this app, and delete any that are not in the keep set.
func (m *Manager) deleteDepartedVersions(ctx context.Context, cr *unstructured.Unstructured, keep map[string]bool, logger klog.Logger) error {
	selector := fmt.Sprintf("app=%s,app.kubernetes.io/managed-by=%s,%s",
		cr.GetName(), fieldManager, versionLabel)
	list, err := m.opts.Client.Resource(gvrDeployment).Namespace(cr.GetNamespace()).List(
		ctx, metav1.ListOptions{LabelSelector: selector})
	if err != nil {
		return fmt.Errorf("list versioned deployments: %w", err)
	}
	var errs []error
	for i := range list.Items {
		deployment := &list.Items[i]
		version := deployment.GetLabels()[versionLabel]
		if version == "" || keep[version] {
			continue
		}
		if !ownedBy(deployment, cr) {
			logger.V(2).Info("skipping deployment not owned by this DBOSApplication",
				"app", cr.GetNamespace()+"/"+cr.GetName(), "deployment", deployment.GetName())
			continue
		}
		err := m.opts.Client.Resource(gvrDeployment).Namespace(cr.GetNamespace()).Delete(
			ctx, deployment.GetName(), metav1.DeleteOptions{})
		if err != nil && !apierrors.IsNotFound(err) {
			errs = append(errs, fmt.Errorf("delete deployment %q: %w", deployment.GetName(), err))
			continue
		}
		logger.Info("version left the autoscale response; deployment deleted",
			"app", cr.GetNamespace()+"/"+cr.GetName(),
			"version", version, "deployment", deployment.GetName())
	}
	return errors.Join(errs...)
}

func ownedBy(obj *unstructured.Unstructured, cr *unstructured.Unstructured) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "DBOSApplication" && ref.Name == cr.GetName() && ref.UID == cr.GetUID() {
			return true
		}
	}
	return false
}
