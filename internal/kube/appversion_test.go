package kube

import (
	"context"
	"regexp"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
)

func crHash(t *testing.T, cr *unstructured.Unstructured) string {
	t.Helper()
	template, _, _ := unstructured.NestedMap(cr.Object, "spec", "template")
	hash, err := hashTemplate(template)
	if err != nil {
		t.Fatalf("hashTemplate: %v", err)
	}
	return hash
}

func appVersionOf(t *testing.T, manifest map[string]any) string {
	t.Helper()
	containers, _, _ := unstructured.NestedSlice(manifest, "spec", "template", "spec", "containers")
	if len(containers) == 0 {
		t.Fatal("applied manifest has no containers")
	}
	env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
	for _, e := range env {
		if v, ok := e.(map[string]any); ok && v["name"] == appVersionEnv {
			value, _ := v["value"].(string)
			return value
		}
	}
	return ""
}

func mainDeployment(version string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "myapp", "namespace": "dbos"},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{
			"containers": []any{map[string]any{
				"name": "app", "image": "img:v1",
				"env": []any{map[string]any{"name": appVersionEnv, "value": version}},
			}},
		}}},
	}}
}

func snapshotFor(name, hash string, template map[string]any) *unstructured.Unstructured {
	s := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "ControllerRevision",
		"metadata": map[string]any{
			"name":        name,
			"namespace":   "dbos",
			"annotations": map[string]any{snapshotHashAnnotation: hash},
		},
		"revision": int64(1),
		"data":     map[string]any{"template": template},
	}}
	s.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "dbos.dev/v1alpha1", Kind: "DBOSApplication",
		Name: "myapp", UID: "uid-123",
	}})
	return s
}

func TestHashTemplateCanonical(t *testing.T) {
	a := map[string]any{"spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "img:v1"}}}, "metadata": map[string]any{"labels": map[string]any{"x": "y"}}}
	b := map[string]any{"metadata": map[string]any{"labels": map[string]any{"x": "y"}}, "spec": map[string]any{"containers": []any{map[string]any{"name": "app", "image": "img:v1"}}}}
	ha, err := hashTemplate(a)
	if err != nil {
		t.Fatalf("hashTemplate: %v", err)
	}
	hb, _ := hashTemplate(b)
	if ha != hb {
		t.Errorf("hash differs across map insertion order: %q vs %q", ha, hb)
	}
	if len(ha) != templateHashLen {
		t.Errorf("hash length = %d, want %d", len(ha), templateHashLen)
	}
	_ = unstructured.SetNestedField(b, "img:v2", "spec", "containers")
	if hc, _ := hashTemplate(b); hc == ha {
		t.Error("hash unchanged after template change")
	}
}

func TestSeededVersionRoundTrip(t *testing.T) {
	version := mintSeededVersion("9f2c41ab8de30a17", time.Unix(0, 1754300000000123456))
	if version != "1754300000000123456-9f2c41ab8de30a17" {
		t.Errorf("mintSeededVersion = %q", version)
	}
	hash, ok := extractDeploymentHashFromAppVersion(version)
	if !ok || hash != "9f2c41ab8de30a17" {
		t.Errorf("parseSeededVersion(%q) = %q, %v", version, hash, ok)
	}
	for _, in := range []string{"v1.2.3", "authored", "123-DEADBEEFDEADBEEF", "123-9f2c41ab8de3", ""} {
		if _, ok := extractDeploymentHashFromAppVersion(in); ok {
			t.Errorf("parseSeededVersion(%q) accepted a non-seeded version", in)
		}
	}
}

func TestReconcileDeploymentSeedsAndSnapshots(t *testing.T) {
	f := newVersionFixture(t, result(time.Now()))
	if err := f.manager.reconcileDeployment(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileDeployment: %v", err)
	}

	hash := crHash(t, f.cr)
	version := appVersionOf(t, f.applied(t)["myapp"])
	if !regexp.MustCompile(`^[0-9]+-` + hash + `$`).MatchString(version) {
		t.Errorf("seeded version = %q, want <ns-epoch>-%s", version, hash)
	}

	snap, err := f.client.Resource(gvrControllerRevision).Namespace("dbos").Get(
		context.Background(), "myapp-tpl-"+hash, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("snapshot not created: %v", err)
	}
	if got := snap.GetAnnotations()[snapshotVersionAnnotation]; got != version {
		t.Errorf("snapshot version annotation = %q, want %q", got, version)
	}
	template, _, _ := unstructured.NestedMap(snap.Object, "data", "template")
	labels, _, _ := unstructured.NestedMap(template, "metadata", "labels")
	if labels["app"] != nil {
		t.Errorf("snapshot template carries injected app label: %v", labels)
	}
	if h, _ := hashTemplate(template); h != hash {
		t.Errorf("snapshot template hash = %q, want %q — must be the authored template verbatim", h, hash)
	}
}

func TestReconcileDeploymentReusesLiveVersion(t *testing.T) {
	current := "1754300000000123456-"
	f := newVersionFixture(t, result(time.Now()))
	current += crHash(t, f.cr)
	f = newVersionFixture(t, result(time.Now()), mainDeployment(current))
	if err := f.manager.reconcileDeployment(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileDeployment: %v", err)
	}
	if got := appVersionOf(t, f.applied(t)["myapp"]); got != current {
		t.Errorf("version = %q, want the live %q reused", got, current)
	}
}

func TestReconcileDeploymentMintsOnTemplateChange(t *testing.T) {
	stale := "1754300000000123456-aaaaaaaaaaaaaaaa" // hash of a template long gone
	f := newVersionFixture(t, result(time.Now()), mainDeployment(stale))
	if err := f.manager.reconcileDeployment(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileDeployment: %v", err)
	}
	version := appVersionOf(t, f.applied(t)["myapp"])
	if version == stale {
		t.Fatal("stale version reused despite template change")
	}
	if hash, ok := extractDeploymentHashFromAppVersion(version); !ok || hash != crHash(t, f.cr) {
		t.Errorf("minted version = %q, want hash %s", version, crHash(t, f.cr))
	}
}

func TestApplyVersionDeploymentUsesSnapshot(t *testing.T) {
	oldVersion := "1754300000000123456-aaaaaaaaaaaaaaaa"
	orphan := "1754300000000123457-bbbbbbbbbbbbbbbb"
	oldTemplate := map[string]any{"spec": map[string]any{
		"containers": []any{map[string]any{"name": "app", "image": "img:old"}},
	}}
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: oldVersion, DesiredExecutors: 2},
		conductor.VersionRecommendation{ApplicationVersion: orphan, DesiredExecutors: 1},
	), snapshotFor("myapp-tpl-aaaaaaaaaaaaaaaa", "aaaaaaaaaaaaaaaa", oldTemplate))
	err := f.manager.reconcileOldVersions(context.Background(), f.cr, klog.Background())
	if err == nil || !strings.Contains(err.Error(), orphan) {
		t.Fatalf("reconcileOldVersions = %v, want an error naming the snapshot-less version %q", err, orphan)
	}

	applied := f.applied(t)
	snapshotted := versionDeploymentName("myapp", oldVersion)
	containers, _, _ := unstructured.NestedSlice(applied[snapshotted], "spec", "template", "spec", "containers")
	if image, _, _ := unstructured.NestedString(containers[0].(map[string]any), "image"); image != "img:old" {
		t.Errorf("snapshotted version image = %q, want img:old", image)
	}
	if got := appVersionOf(t, applied[snapshotted]); got != oldVersion {
		t.Errorf("pinned DBOS__APPVERSION = %q, want the full version %q", got, oldVersion)
	}
	if _, ok := applied[versionDeploymentName("myapp", orphan)]; ok {
		t.Error("snapshot-less version was applied; want it refused")
	}
}

func TestReconcileDeploymentRejectsUserPin(t *testing.T) {
	cases := map[string]map[string]any{
		"literal": {"name": appVersionEnv, "value": "authored-v7"},
		"valueFrom": {
			"name": appVersionEnv,
			"valueFrom": map[string]any{"configMapKeyRef": map[string]any{
				"name": "release-info", "key": "version",
			}},
		},
	}
	for name, pin := range cases {
		t.Run(name, func(t *testing.T) {
			f := newVersionFixture(t, result(time.Now()))
			container := map[string]any{"name": "app", "image": "img:v1", "env": []any{pin}}
			_ = unstructured.SetNestedSlice(f.cr.Object, []any{container}, "spec", "template", "spec", "containers")
			err := f.manager.reconcileDeployment(context.Background(), f.cr, klog.Background())
			if err == nil || !strings.Contains(err.Error(), appVersionEnv) {
				t.Fatalf("reconcileDeployment = %v, want a rejection naming %s", err, appVersionEnv)
			}
			if _, ok := f.applied(t)["myapp"]; ok {
				t.Error("deployment applied despite the user pin")
			}
		})
	}
}

func TestEnsureSnapshotConflicts(t *testing.T) {
	template := map[string]any{"spec": map[string]any{
		"containers": []any{map[string]any{"name": "app", "image": "img:v2"}},
	}}
	hash, err := hashTemplate(template)
	if err != nil {
		t.Fatalf("hashTemplate: %v", err)
	}

	seeded := mintSeededVersion(hash, time.Unix(0, 1))
	f := newVersionFixture(t, result(time.Now()),
		snapshotFor(snapshotName("myapp", hash), "cccccccccccccccc", nil))
	err = f.manager.ensureSnapshot(context.Background(), f.cr, seeded, hash, template, klog.Background())
	if err == nil {
		t.Error("hash conflict must error, not overwrite")
	}

	f = newVersionFixture(t, result(time.Now()),
		snapshotFor(snapshotName("myapp", hash), hash, nil))
	if err := f.manager.ensureSnapshot(context.Background(), f.cr, seeded, hash, template, klog.Background()); err != nil {
		t.Errorf("matching snapshot must be a no-op, got %v", err)
	}
}
