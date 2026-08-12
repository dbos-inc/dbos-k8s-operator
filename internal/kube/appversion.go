// Unless the author pins DBOS__APPVERSION, each rollout is seeded as
// <ns-epoch>-<template-hash>: the timestamp makes every rollout (rollbacks
// included) register as latest; templates persist as ControllerRevisions.
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

// Part of the parse contract — changing it re-versions every template.
const templateHashLen = 12

const (
	snapshotHashAnnotation    = "dbos.dev/template-hash"
	snapshotVersionAnnotation = "dbos.dev/app-version" // version at capture time, for humans
)

var seededVersionRe = regexp.MustCompile(`^[0-9]+-[0-9a-f]{` + strconv.Itoa(templateHashLen) + `}$`)

// Hashes spec.template as authored, before the operator's own injections.
func hashTemplate(template map[string]any) (string, error) {
	raw, err := json.Marshal(template)
	if err != nil {
		return "", fmt.Errorf("marshal template for hashing: %w", err)
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:templateHashLen], nil
}

func mintSeededVersion(hash string, now time.Time) string {
	return strconv.FormatInt(now.UnixNano(), 10) + "-" + hash
}

func parseSeededVersion(version string) (hash string, ok bool) {
	if !seededVersionRe.MatchString(version) {
		return "", false
	}
	return version[strings.LastIndex(version, "-")+1:], true
}

// A valueFrom or empty entry counts as authored but yields "".
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

// Authored wins; otherwise the live seeded version is reused while its hash
// matches — a reconcile pass or restart must never mint a phantom rollout.
func (m *Manager) resolveAppVersion(ctx context.Context, cr *unstructured.Unstructured, template map[string]any, logger klog.Logger) (version string, seeded bool, err error) {
	// Use the versioned pinned by the CR definition, if any.
	if authored, ok := authoredAppVersion(template); ok {
		if authored == "" {
			return "", false, fmt.Errorf("%s must be pinned to a literal value; valueFrom (or an empty value) is unsupported: the operator cannot snapshot a version it cannot read", appVersionEnv)
		}
		return authored, false, nil
	}
	// Otherwise, reuse the live seeded version if its hash matches the current template.
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

// Read the currently deployed CR and extract its DBOS__APPVERSION
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

func snapshotKey(version string) string {
	if hash, ok := parseSeededVersion(version); ok {
		return hash
	}
	return versionSlug(version)
}

func snapshotName(app, version string) string {
	return app + "-tpl-" + snapshotKey(version) // remove the timestamp from the seeded version, so we can reuse existing entries during rollback
}

// A seeded hash naming a different template is a loud error; an author-pinned
// version reused across template changes is replaced (latest wins).
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

	// The snapshot already exists.
	existing, err := revisions.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get template snapshot %q: %w", name, err)
	}
	// Same hash means the template is unchanged; nothing to do. That's the common path.
	if existing.GetAnnotations()[snapshotHashAnnotation] == hash {
		return nil
	}
	// This should never happen except if someone screwed with the snapshot directly.
	if _, seeded := parseSeededVersion(version); seeded {
		return fmt.Errorf("template snapshot %q holds a different template for hash %s; refusing to overwrite", name, hash)
	}
	// Reaching this means the user pinned a version and redeployed -- we replace the snapshot with the new template. Latest wins.
	if !ownedBy(existing, cr) { // Just make sure our CR actually owns the snapshot before we delete it :-)
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
