// Each rollout is seeded as <ns-epoch>-<template-hash>: the timestamp makes
// every rollout (rollbacks included) register as latest; templates persist as
// ControllerRevisions.
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
const templateHashLen = 16

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

// The live seeded version is reused while its hash matches.
func (m *Manager) resolveAppVersion(ctx context.Context, cr *unstructured.Unstructured, template map[string]any, logger klog.Logger) (version, hash string, err error) {
	// Fail loudly if the CR comes with a DBOS__APPVERSION. The operator owns it.
	if _, ok := authoredAppVersion(template); ok {
		return "", "", fmt.Errorf("%s is operator-owned; remove it from spec.template", appVersionEnv)
	}
	hash, err = hashTemplate(template)
	if err != nil {
		return "", "", err
	}
	// Reuse the live seeded version if its hash matches the current template.
	// live should be empty if the deployment doesn't exist yet, or if it was adopted and rolled back to a non-seeded version.
	live, err := m.liveAppVersion(ctx, cr)
	if err != nil {
		return "", "", err
	}
	if h, ok := parseSeededVersion(live); ok && h == hash {
		return live, hash, nil
	}
	version = mintSeededVersion(hash, time.Now())
	logger.Info("template changed; new application version",
		"app", cr.GetNamespace()+"/"+cr.GetName(), "version", version, "previous", live)
	return version, hash, nil
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

// Keyed by template hash, not version, so rollbacks reuse existing entries.
func snapshotName(app, hash string) string {
	return app + "-tpl-" + hash
}

// A hash naming a different template is a loud error, never overwritten.
func (m *Manager) ensureSnapshot(ctx context.Context, cr *unstructured.Unstructured, version, hash string, template map[string]any, logger klog.Logger) error {
	name := snapshotName(cr.GetName(), hash)
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
	if err == nil {
		logger.Info("template snapshot captured", "app", cr.GetNamespace()+"/"+cr.GetName(),
			"snapshot", name, "version", version)
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create template snapshot %q: %w", name, err)
	}

	// Snapshot already exists; check that it matches the current template.
	existing, err := revisions.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("get template snapshot %q: %w", name, err)
	}
	// Same hash means the template is unchanged; nothing to do. That's the common path.
	if existing.GetAnnotations()[snapshotHashAnnotation] != hash {
		// This should never happen except if someone screwed with the snapshot directly.
		return fmt.Errorf("template snapshot %q holds a different template for hash %s; refusing to overwrite", name, hash)
	}
	return nil
}

func (m *Manager) snapshotTemplate(ctx context.Context, cr *unstructured.Unstructured, version string) (map[string]any, bool, error) {
	hash, ok := parseSeededVersion(version)
	if !ok {
		return nil, false, nil
	}
	name := snapshotName(cr.GetName(), hash)
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
