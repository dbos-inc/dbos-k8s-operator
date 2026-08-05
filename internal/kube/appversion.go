// Application-version seeding and template snapshots.
//
// Unless the CR author pins DBOS__APPVERSION themselves, the operator names
// each rollout's version as <ns-epoch>-<template-hash> and injects it into
// every container. The timestamp makes every rollout — including a rollback to
// an earlier template — a version Conductor has never seen, so it registers as
// latest; the hash suffix is the back-pointer to the template that produced it.
//
// The hash covers the CR's spec.template exactly as authored, before the
// operator's own injections (the app label and the seeded env var), serialized
// through encoding/json, which sorts map keys and so is canonical for a given
// field order. Reverting to a byte-identical template therefore reproduces the
// hash; re-authoring an equivalent template with lists reordered does not —
// the same contract as Kubernetes' own pod-template-hash.
//
// Each distinct template is persisted once as an apps/v1 ControllerRevision
// (the mechanism StatefulSet and DaemonSet use for exactly this), keyed by
// hash so every rollback of a template shares one snapshot, owned by the CR
// so deletion cascades. Snapshots are never pruned on version departure: they
// are a few KiB each, and keeping them means a version reappearing after any
// gap can still be rebuilt.
package kube

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
)

var gvrControllerRevision = schema.GroupVersionResource{Group: "apps", Version: "v1", Resource: "controllerrevisions"}

const appVersionEnv = "DBOS__APPVERSION"

// templateHashLen bounds the hex hash embedded in seeded versions and snapshot
// names. Part of the parse contract — changing it re-versions every template.
const templateHashLen = 12

const (
	// snapshotHashAnnotation carries the template hash on a snapshot, letting
	// ensureSnapshot detect a same-name/different-template conflict without
	// deserializing data.
	snapshotHashAnnotation = "dbos.dev/template-hash"
	// snapshotVersionAnnotation records the full version string at capture
	// time, for humans; later versions of the same template do not update it.
	snapshotVersionAnnotation = "dbos.dev/app-version"
)

var seededVersionRe = regexp.MustCompile(`^[0-9]+-[0-9a-f]{` + strconv.Itoa(templateHashLen) + `}$`)

// hashTemplate canonically hashes the CR's spec.template as authored — before
// the operator injects the app label or the seeded version env var.
func hashTemplate(template map[string]any) (string, error) {
	raw, err := json.Marshal(template)
	if err != nil {
		return "", fmt.Errorf("marshal template for hashing: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:templateHashLen], nil
}

// mintSeededVersion names a new rollout. Nanosecond precision so even
// back-to-back rollouts of distinct templates order unambiguously.
func mintSeededVersion(hash string, now time.Time) string {
	return strconv.FormatInt(now.UnixNano(), 10) + "-" + hash
}

// parseSeededVersion extracts the template hash from an operator-seeded
// version. Author-pinned versions do not match and return ok=false.
func parseSeededVersion(version string) (hash string, ok bool) {
	if !seededVersionRe.MatchString(version) {
		return "", false
	}
	return version[strings.LastIndex(version, "-")+1:], true
}

// authoredAppVersion reports whether a pod template sets DBOS__APPVERSION on
// any container, and its literal value. A valueFrom entry counts as authored
// but yields "" — the operator then neither seeds nor snapshots, since it
// cannot know the version.
func authoredAppVersion(template map[string]any) (string, bool) {
	containers, _, _ := unstructured.NestedSlice(template, "spec", "containers")
	for _, item := range containers {
		container, ok := item.(map[string]any)
		if !ok {
			continue
		}
		env, _, _ := unstructured.NestedSlice(container, "env")
		for _, e := range env {
			if v, ok := e.(map[string]any); ok && v["name"] == appVersionEnv {
				value, _ := v["value"].(string)
				return value, true
			}
		}
	}
	return "", false
}

// resolveAppVersion decides the version the main Deployment runs. An authored
// DBOS__APPVERSION always wins (seeded=false). Otherwise the live Deployment
// is the durable register of the current seeded version: while its hash still
// matches the CR template the exact string is reused verbatim — a reconcile
// pass or operator restart must never mint a phantom rollout — and a new
// version is minted only when the template actually changed.
func (m *Manager) resolveAppVersion(ctx context.Context, cr *unstructured.Unstructured, template map[string]any, logger klog.Logger) (version string, seeded bool, err error) {
	if authored, ok := authoredAppVersion(template); ok {
		return authored, false, nil
	}
	hash, err := hashTemplate(template)
	if err != nil {
		return "", false, err
	}
	live, err := m.liveAppVersion(ctx, cr)
	if err != nil {
		return "", false, err
	}
	if h, ok := parseSeededVersion(live); ok && h == hash {
		return live, true, nil
	}
	version = mintSeededVersion(hash, time.Now())
	logger.Info("template changed; new application version",
		"app", cr.GetNamespace()+"/"+cr.GetName(), "version", version, "previous", live)
	return version, true, nil
}

// liveAppVersion reads the DBOS__APPVERSION the app's main Deployment
// currently carries; "" when the Deployment or the env var does not exist.
func (m *Manager) liveAppVersion(ctx context.Context, cr *unstructured.Unstructured) (string, error) {
	live, err := m.opts.Client.Resource(gvrDeployment).Namespace(cr.GetNamespace()).Get(
		ctx, cr.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("get live deployment: %w", err)
	}
	template, _, _ := unstructured.NestedMap(live.Object, "spec", "template")
	version, _ := authoredAppVersion(template)
	return version, nil
}

// snapshotKey names the template a version ran: the embedded hash for seeded
// versions, the version's slug for author-pinned ones.
func snapshotKey(version string) string {
	if hash, ok := parseSeededVersion(version); ok {
		return hash
	}
	return versionSlug(version)
}

func snapshotName(app, version string) string {
	return app + "-tpl-" + snapshotKey(version)
}

// ensureSnapshot persists the version→template binding before the version's
// pods exist. ControllerRevision data is immutable, which turns the
// impossible-unless-bug case — a seeded hash naming a different template —
// into a loud error instead of a silent clobber. An author-pinned version
// reused across template changes is the author's prerogative: latest wins,
// by delete and recreate.
func (m *Manager) ensureSnapshot(ctx context.Context, cr *unstructured.Unstructured, version, hash string, template map[string]any, logger klog.Logger) error {
	name := snapshotName(cr.GetName(), version)
	revisions := m.opts.Client.Resource(gvrControllerRevision).Namespace(cr.GetNamespace())
	snapshot := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "ControllerRevision",
		"metadata": map[string]any{
			"name":      name,
			"namespace": cr.GetNamespace(),
			"labels": map[string]any{
				"app":                          cr.GetName(),
				"app.kubernetes.io/managed-by": fieldManager,
			},
			"annotations": map[string]any{
				snapshotHashAnnotation:    hash,
				snapshotVersionAnnotation: version,
			},
			"ownerReferences": []any{crOwnerReference(cr)},
		},
		"revision": time.Now().UnixNano(),
		"data":     map[string]any{"template": template},
	}}

	_, err := revisions.Create(ctx, snapshot, metav1.CreateOptions{})
	if err == nil || !apierrors.IsAlreadyExists(err) {
		if err != nil {
			return fmt.Errorf("create template snapshot %q: %w", name, err)
		}
		logger.Info("template snapshot captured", "app", cr.GetNamespace()+"/"+cr.GetName(),
			"snapshot", name, "version", version)
		return nil
	}

	existing, err := revisions.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get template snapshot %q: %w", name, err)
	}
	if existing.GetAnnotations()[snapshotHashAnnotation] == hash {
		return nil
	}
	if _, seeded := parseSeededVersion(version); seeded {
		return fmt.Errorf("template snapshot %q holds a different template for hash %s; refusing to overwrite", name, hash)
	}
	if !ownedBy(existing, cr) {
		return fmt.Errorf("template snapshot %q exists but is not owned by this DBOSApplication", name)
	}
	if err := revisions.Delete(ctx, name, metav1.DeleteOptions{}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("replace template snapshot %q: %w", name, err)
	}
	if _, err := revisions.Create(ctx, snapshot, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("replace template snapshot %q: %w", name, err)
	}
	logger.Info("pinned version reused across template changes; snapshot replaced",
		"app", cr.GetNamespace()+"/"+cr.GetName(), "snapshot", name, "version", version)
	return nil
}

// snapshotTemplate loads the pod template a version ran, as captured at its
// rollout. found=false — no snapshot, or one owned by something else — sends
// the caller to the CR's current template, the pre-snapshot behavior.
func (m *Manager) snapshotTemplate(ctx context.Context, cr *unstructured.Unstructured, version string) (map[string]any, bool, error) {
	name := snapshotName(cr.GetName(), version)
	obj, err := m.opts.Client.Resource(gvrControllerRevision).Namespace(cr.GetNamespace()).Get(
		ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get template snapshot %q: %w", name, err)
	}
	if !ownedBy(obj, cr) {
		return nil, false, nil
	}
	template, ok, err := unstructured.NestedMap(obj.Object, "data", "template")
	if err != nil || !ok {
		return nil, false, fmt.Errorf("template snapshot %q malformed: %v", name, err)
	}
	return template, true, nil
}
