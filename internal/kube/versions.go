// One Deployment per old version still present in Conductor's autoscale response
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

const versionLabel = "dbos.dev/app-version"

func sanitizeVersion(version string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(version) {
		switch {
		case r >= 'a' && r <= 'z' || r >= '0' && r <= '9':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

// Two distinct versions must never share a slug — they would share a Deployment.
func versionSlug(version string) string {
	if version == "" {
		return "unversioned"
	}
	s := sanitizeVersion(version)
	if s == strings.ToLower(version) && len(s) <= 40 {
		return s
	}
	sum := sha256.Sum256([]byte(version))
	hash := hex.EncodeToString(sum[:])[:6]
	if len(s) > 33 {
		s = strings.Trim(s[:33], "-")
	}
	if s == "" {
		return "v-" + hash
	}
	return s + "-" + hash
}

// The latest version keeps the bare CR name (KEDA targets it by name).
func versionDeploymentName(app, version string) string {
	return app + "-" + versionSlug(version)
}

// spec.replicas is written directly — old versions have no ScaledObject.
func buildVersionDeployment(cr *unstructured.Unstructured, version string, replicas int, template map[string]any) (map[string]any, error) {
	deployment, err := assembleDeployment(cr, template)
	if err != nil {
		return nil, err
	}
	suffix := versionSlug(version)
	if err := unstructured.SetNestedField(deployment, versionDeploymentName(cr.GetName(), version), "metadata", "name"); err != nil {
		return nil, err
	}
	for _, path := range [][]string{
		{"metadata", "labels"},
		{"spec", "selector", "matchLabels"},
		{"spec", "template", "metadata", "labels"},
	} {
		labels, _, _ := unstructured.NestedMap(deployment, path...)
		if labels == nil {
			labels = map[string]any{}
		}
		labels[versionLabel] = suffix
		if err := unstructured.SetNestedMap(deployment, labels, path...); err != nil {
			return nil, err
		}
	}
	if err := unstructured.SetNestedField(deployment, int64(replicas), "spec", "replicas"); err != nil {
		return nil, err
	}
	if err := pinAppVersion(deployment, version); err != nil {
		return nil, err
	}
	return deployment, nil
}

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

// No poll yet, no policy, or a stale result leaves the fleet untouched —
// absence of data must never be read as "delete everything".
func (m *Manager) reconcileOldVersions(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) error {
	key := cr.GetNamespace() + "/" + cr.GetName()

	result, ok := m.opts.Store.Get(appName(cr))
	if !ok || result.NoPolicy {
		return nil
	}
	if result.Stale {
		logger.V(2).Info("skipping old-version reconcile on a stale poll result", "app", key)
		return nil
	}

	keep := make(map[string]bool, len(result.OldVersions))
	var errs []error
	for _, v := range result.OldVersions {
		keep[versionSlug(v.ApplicationVersion)] = true
		replicas := max(v.DesiredExecutors, 0)
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
		metav1.PatchOptions{FieldManager: fieldManager, Force: ptr.To(true)})
	if err != nil {
		return fmt.Errorf("apply deployment for version %q: %w", version, err)
	}
	return nil
}

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
		slug := deployment.GetLabels()[versionLabel]
		if slug == "" || keep[slug] {
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
			"version", slug, "deployment", deployment.GetName())
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
