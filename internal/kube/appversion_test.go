package kube

import (
	"context"
	"regexp"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
)

// crHash hashes the fixture CR's authored template, the way reconcile does.
func crHash(t *testing.T, cr *unstructured.Unstructured) string {
	t.Helper()
	template, _, _ := unstructured.NestedMap(cr.Object, "spec", "template")
	hash, err := hashTemplate(template)
	if err != nil {
		t.Fatalf("hashTemplate: %v", err)
	}
	return hash
}

// appVersionOf pulls DBOS__APPVERSION out of an applied manifest's first
// container. "" when absent.
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

// mainDeployment is the app's live main Deployment as a previous reconcile
// would have left it: the seeded version env var on its container.
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

// snapshotFor is a template snapshot as ensureSnapshot would have stored it.
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
	version := mintSeededVersion("9f2c41ab8de3", time.Unix(0, 1754300000000123456))
	if version != "1754300000000123456-9f2c41ab8de3" {
		t.Errorf("mintSeededVersion = %q", version)
	}
	hash, ok := parseSeededVersion(version)
	if !ok || hash != "9f2c41ab8de3" {
		t.Errorf("parseSeededVersion(%q) = %q, %v", version, hash, ok)
	}
	// The 32-char lowercase form must survive slugging losslessly: version
	// strings become label values and Deployment name suffixes verbatim.
	if got := versionSlug(version); got != version {
		t.Errorf("versionSlug(%q) = %q, want identity", version, got)
	}
	for _, in := range []string{"v1.2.3", "authored", "123-DEADBEEFDEAD", "123-9f2c", ""} {
		if _, ok := parseSeededVersion(in); ok {
			t.Errorf("parseSeededVersion(%q) accepted a non-seeded version", in)
		}
	}
}

// A fresh app gets a minted <ns>-<hash> version injected into its main
// Deployment, and the authored template — no injected labels, no seeded env —
// is captured as a snapshot before the apply.
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

// While the template is unchanged, the live Deployment's version string is
// reused verbatim: reconcile passes and operator restarts mint nothing.
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

// A changed template mints a new version — and so does a rollback, whose new
// version shares the old one's hash and therefore its existing snapshot.
func TestReconcileDeploymentMintsOnTemplateChange(t *testing.T) {
	stale := "1754300000000123456-aaaaaaaaaaaa" // hash of a template long gone
	f := newVersionFixture(t, result(time.Now()), mainDeployment(stale))
	if err := f.manager.reconcileDeployment(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileDeployment: %v", err)
	}
	version := appVersionOf(t, f.applied(t)["myapp"])
	if version == stale {
		t.Fatal("stale version reused despite template change")
	}
	if hash, ok := parseSeededVersion(version); !ok || hash != crHash(t, f.cr) {
		t.Errorf("minted version = %q, want hash %s", version, crHash(t, f.cr))
	}
}

// An authored DBOS__APPVERSION wins: nothing is injected over it, and the
// snapshot is keyed by the authored version's slug.
func TestReconcileDeploymentRespectsAuthoredPin(t *testing.T) {
	f := newVersionFixture(t, result(time.Now()))
	container := map[string]any{
		"name": "app", "image": "img:v1",
		"env": []any{map[string]any{"name": appVersionEnv, "value": "authored-v7"}},
	}
	_ = unstructured.SetNestedSlice(f.cr.Object, []any{container}, "spec", "template", "spec", "containers")
	if err := f.manager.reconcileDeployment(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileDeployment: %v", err)
	}
	if got := appVersionOf(t, f.applied(t)["myapp"]); got != "authored-v7" {
		t.Errorf("version = %q, want the authored pin untouched", got)
	}
	if _, err := f.client.Resource(gvrControllerRevision).Namespace("dbos").Get(
		context.Background(), "myapp-tpl-authored-v7", metav1.GetOptions{}); err != nil {
		t.Errorf("snapshot for the authored version not created: %v", err)
	}
}

// An old version scales up from its snapshot's template, not the CR's current
// one; a version with no snapshot falls back to the CR template.
func TestApplyVersionDeploymentUsesSnapshot(t *testing.T) {
	oldVersion := "1754300000000123456-aaaaaaaaaaaa"
	orphan := "1754300000000123457-bbbbbbbbbbbb"
	oldTemplate := map[string]any{"spec": map[string]any{
		"containers": []any{map[string]any{"name": "app", "image": "img:old"}},
	}}
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: oldVersion, DesiredExecutors: 2},
		conductor.VersionRecommendation{ApplicationVersion: orphan, DesiredExecutors: 1},
	), snapshotFor("myapp-tpl-aaaaaaaaaaaa", "aaaaaaaaaaaa", oldTemplate))
	f.reconcile(t)

	applied := f.applied(t)
	imageOf := func(name string) string {
		containers, _, _ := unstructured.NestedSlice(applied[name], "spec", "template", "spec", "containers")
		image, _, _ := unstructured.NestedString(containers[0].(map[string]any), "image")
		return image
	}
	snapshotted := versionDeploymentName("myapp", oldVersion)
	if got := imageOf(snapshotted); got != "img:old" {
		t.Errorf("snapshotted version image = %q, want img:old", got)
	}
	if got := appVersionOf(t, applied[snapshotted]); got != oldVersion {
		t.Errorf("pinned DBOS__APPVERSION = %q, want the full version %q", got, oldVersion)
	}
	if got := imageOf(versionDeploymentName("myapp", orphan)); got != "img:v1" {
		t.Errorf("snapshot-less version image = %q, want the CR fallback img:v1", got)
	}
}

// Snapshot conflicts: a seeded version naming different template bytes is a
// bug and must fail loudly; an authored version reused across template changes
// is replaced, latest wins.
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
		snapshotFor(snapshotName("myapp", seeded), "cccccccccccc", nil))
	err = f.manager.ensureSnapshot(context.Background(), f.cr, seeded, hash, template, klog.Background())
	if err == nil {
		t.Error("seeded-version hash conflict must error, not overwrite")
	}

	f = newVersionFixture(t, result(time.Now()),
		snapshotFor(snapshotName("myapp", "authored-v7"), "cccccccccccc", nil))
	if err := f.manager.ensureSnapshot(context.Background(), f.cr, "authored-v7", hash, template, klog.Background()); err != nil {
		t.Fatalf("authored-version snapshot replace: %v", err)
	}
	snap, err := f.client.Resource(gvrControllerRevision).Namespace("dbos").Get(
		context.Background(), snapshotName("myapp", "authored-v7"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("replaced snapshot missing: %v", err)
	}
	if got := snap.GetAnnotations()[snapshotHashAnnotation]; got != hash {
		t.Errorf("replaced snapshot hash = %q, want %q", got, hash)
	}

	// Idempotent when the stored template already matches.
	if err := f.manager.ensureSnapshot(context.Background(), f.cr, "authored-v7", hash, template, klog.Background()); err != nil {
		t.Errorf("matching snapshot must be a no-op, got %v", err)
	}
}
