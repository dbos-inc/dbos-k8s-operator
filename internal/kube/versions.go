// Old-application-version handling. Conductor's autoscale response carries one
// entry per version still holding work; workflows only finish on an executor of
// their own version, so each old version needs its own Deployment while its
// entry is present, and is torn down when the entry disappears.
//
// Presence, not size, is what decides existence: an entry with
// desiredExecutors 0 still means unfinished work whose queue depth is 0 (a
// PENDING workflow already dequeued, say), so its Deployment stays — at one
// replica normally, possibly at zero under a drain budget. Only Conductor
// dropping the version from the response — its signal that nothing is left to
// run — deletes it.
//
// Sizing: unbudgeted by default (each version gets its full recommendation,
// floored at 1). When the CR authors spec.maxOldVersionsReplicas, that number
// is the total pod allowance old versions share, spread equally (capped at
// each one's need, leftovers waterfall), newest versions first when there are
// more versions than pods. Versions that get 0 stay present but parked, and
// pick up freed slots as newer versions drain. The budget is independent of
// the latest Deployment: its replicas and rollout surge are governed by
// spec.strategy alone.
//
// Not yet handled: a deletion grace period and PodDisruptionBudgets, so a
// version's last pods can be interrupted mid-workflow (the work is durable and
// recovers, but on the next pod of that version).
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/ptr"
)

// versionLabel marks a versioned Deployment's pods with the application
// version they run, keeping its selector disjoint from the main (latest)
// Deployment's plain app=<name> selector.
const versionLabel = "dbos.dev/app-version"

// sanitizeVersion lowercases an application version and collapses every
// non-alphanumeric rune to '-', yielding a DNS-1123-safe token. Lossy by
// nature: distinct versions can sanitize to the same string, which versionSlug
// resolves.
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

// versionSlug is the token identifying one application version in a Deployment
// name and in the version label: the sanitized version when that is lossless,
// otherwise a truncated form plus a short hash of the original. Two distinct
// versions must never share a slug — they would share a Deployment, and one
// version's pods would run the other's code. Both forms stay within the 40
// characters that keep names and label values comfortably in range.
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

// versionDeploymentName names an old version's Deployment. The latest version
// keeps the bare CR name (KEDA's ScaledObject targets it by name); old
// versions get a version-derived suffix.
func versionDeploymentName(app, version string) string {
	return app + "-" + versionSlug(version)
}

// buildVersionDeployment derives the Deployment for one old application
// version: the regular Deployment scaffolding around the version's template,
// renamed, its selector and pod labels extended with the version label,
// DBOS__APPVERSION pinned in every container so its pods register that
// version, and — unlike the latest Deployment, whose replicas KEDA owns —
// spec.replicas written directly, since old versions have no ScaledObject.
//
// template is the snapshot captured at the version's rollout; nil means no
// snapshot exists (the version predates the operator or its snapshots) and
// falls back to the CR's current template — pods then run the latest template
// pinned to the old version string, which recovers the work but not
// necessarily on the version's own image.
func buildVersionDeployment(cr *unstructured.Unstructured, version string, replicas int, template map[string]any) (map[string]any, error) {
	var deployment map[string]any
	var err error
	if template != nil {
		deployment, err = assembleDeployment(cr, template)
	} else {
		deployment, err = buildDeployment(cr)
	}
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

// pinAppVersion sets DBOS__APPVERSION=<version> on every container, replacing
// any value the CR author set: this Deployment exists to run exactly that
// version.
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

// drainBudget returns the pod allowance old-version Deployments share, and
// whether one applies at all: false when the CR authors no
// spec.maxOldVersionsReplicas, in which case sizing is unbudgeted. The cap is
// absolute — it does not flex with the latest Deployment's replicas or its
// rollout surge, so the app's worst-case pod count is
// latest replicas + rollout surge + maxOldVersionsReplicas.
func drainBudget(cr *unstructured.Unstructured) (int, bool, error) {
	raw, ok, err := unstructured.NestedFieldNoCopy(cr.Object, "spec", "maxOldVersionsReplicas")
	if err != nil {
		return 0, false, fmt.Errorf("spec.maxOldVersionsReplicas: %v", err)
	}
	if !ok {
		return 0, false, nil
	}
	var budget int
	switch v := raw.(type) {
	case int64:
		budget = int(v)
	case float64:
		budget = int(v)
	default:
		return 0, false, fmt.Errorf("spec.maxOldVersionsReplicas: unsupported type %T", raw)
	}
	if budget < 0 {
		return 0, false, fmt.Errorf("spec.maxOldVersionsReplicas: negative value %d", budget)
	}
	return budget, true, nil
}

// allocateDrainBudget spreads budget pods across versions in order, one pod
// per version per round, capped at each version's need: equal shares, with
// leftovers flowing to the versions that can still use them. When there are
// fewer pods than versions the earliest entries win one each — callers pass
// versions newest-registered first, making that LIFO. Never exceeds a
// version's need; a zero budget parks every version at zero.
func allocateDrainBudget(needs []int, budget int) []int {
	alloc := make([]int, len(needs))
	for budget > 0 {
		progressed := false
		for i, need := range needs {
			if budget == 0 {
				break
			}
			if alloc[i] < need {
				alloc[i]++
				budget--
				progressed = true
			}
		}
		if !progressed {
			break
		}
	}
	return alloc
}

// oldVersionStaleAfter bounds how old a poll result may be and still drive
// versioned Deployments. Beyond it the response is no longer evidence of what
// is running, and acting on it could delete a Deployment whose version has
// since gone back to holding work. Mirrors the metrics endpoint's own cutoff.
func (m *Manager) oldVersionStaleAfter() time.Duration {
	return max(3*m.opts.PollInterval, 30*time.Second)
}

// reconcileOldVersions makes the cluster match the latest poll result's
// non-latest entries: one Deployment per entry, sized to its recommendation
// (at least 1) — clipped by the drain budget when the CR authors
// spec.maxOldVersionsReplicas — and deletion of every versioned Deployment of
// this app whose version has left the response.
//
// Deletion is driven by the *live* list of versioned Deployments rather than a
// diff against the previous result, so a version that outlived an operator
// restart is still cleaned up, and drift (a hand-created Deployment carrying
// the labels) converges too.
//
// Three conditions leave the fleet untouched, all of them cases where the
// response is not evidence about versions: no poll has succeeded yet, the app
// has no autoscaling policy (Conductor reports nothing to act on), or the last
// result is stale. Absence of data must never be read as "delete everything".
func (m *Manager) reconcileOldVersions(ctx context.Context, cr *unstructured.Unstructured, logger klog.Logger) error {
	namespace, name := cr.GetNamespace(), cr.GetName()
	key := namespace + "/" + name

	result, ok := m.opts.Store.Get(appName(cr))
	if !ok || result.NoPolicy {
		return nil
	}
	if age := time.Since(result.PolledAt); age > m.oldVersionStaleAfter() {
		logger.V(2).Info("skipping old-version reconcile on a stale poll result",
			"app", key, "age", age.Truncate(time.Second))
		return nil
	}

	budget, budgeted, err := drainBudget(cr)
	if err != nil {
		return err
	}

	// A present version needs at least one executor even at desired 0: work
	// that adds no queue depth (a PENDING workflow already dequeued, say)
	// still needs a pod of its version to finish or recover it.
	needs := make([]int, len(result.OldVersions))
	for i, v := range result.OldVersions {
		needs[i] = max(v.DesiredExecutors, 1)
	}
	allocs := needs
	if budgeted {
		// OldVersions arrives newest-registered first, so allocation order is
		// already LIFO: with the budget short, the newest versions win and the
		// oldest wait parked at zero replicas until slots free up.
		allocs = allocateDrainBudget(needs, budget)
	}

	plan := make(map[string]int, len(result.OldVersions))
	slugs := make(map[string]bool, len(result.OldVersions))
	for i, v := range result.OldVersions {
		plan[v.ApplicationVersion] = allocs[i]
		slugs[versionSlug(v.ApplicationVersion)] = true
	}

	m.mu.Lock()
	last, seen := m.lastOldVersions[key]
	changed := !seen || !maps.Equal(last, plan)
	if changed {
		m.lastOldVersions[key] = plan
	}
	m.mu.Unlock()

	var errs []error
	for _, v := range result.OldVersions {
		replicas := plan[v.ApplicationVersion]
		if err := m.applyVersionDeployment(ctx, cr, v.ApplicationVersion, replicas); err != nil {
			errs = append(errs, err)
			continue
		}
		if changed {
			if replicas == 0 {
				logger.Info("old version queued: drain budget exhausted; deployment parked at zero replicas",
					"app", key, "version", v.ApplicationVersion,
					"desiredExecutors", v.DesiredExecutors, "budget", budget,
					"deployment", versionDeploymentName(name, v.ApplicationVersion))
			} else {
				logger.Info("old version holds work; deployment applied",
					"app", key, "version", v.ApplicationVersion,
					"desiredExecutors", v.DesiredExecutors, "replicas", replicas,
					"deployment", versionDeploymentName(name, v.ApplicationVersion))
			}
		}
	}

	if err := m.deleteDepartedVersions(ctx, cr, slugs, logger); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		// The plan was recorded optimistically for log dedupe; drop it so a
		// failed pass reports again rather than going quiet until counts move.
		m.mu.Lock()
		delete(m.lastOldVersions, key)
		m.mu.Unlock()
		return errors.Join(errs...)
	}
	return nil
}

// applyVersionDeployment server-side-applies one old version's Deployment,
// built from the version's template snapshot when one exists. Unlike the main
// Deployment, spec.replicas is part of the applied manifest: old versions have
// no ScaledObject, so the operator owns their size.
func (m *Manager) applyVersionDeployment(ctx context.Context, cr *unstructured.Unstructured, version string, replicas int) error {
	template, _, err := m.snapshotTemplate(ctx, cr, version)
	if err != nil {
		return fmt.Errorf("snapshot for version %q: %w", version, err)
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

// deleteDepartedVersions removes this app's versioned Deployments whose slug is
// not in keep. The label selector scopes the list to Deployments this operator
// created for this app — the main Deployment carries no version label, so it
// can never match — and the owner reference is checked as well, so a
// same-named resource belonging to something else is left alone.
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

// ownedBy reports whether obj carries a controller owner reference to cr.
func ownedBy(obj *unstructured.Unstructured, cr *unstructured.Unstructured) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Kind == "DBOSApplication" && ref.Name == cr.GetName() && ref.UID == cr.GetUID() {
			return true
		}
	}
	return false
}
