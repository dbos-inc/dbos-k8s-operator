package kube

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// versionFixture wires a manager onto a fake cluster holding the test CR plus
// any pre-existing Deployments, and a store primed with one poll result.
type versionFixture struct {
	manager *Manager
	client  *dynamicfake.FakeDynamicClient
	cr      *unstructured.Unstructured
}

func newVersionFixture(t *testing.T, result store.Result, existing ...*unstructured.Unstructured) *versionFixture {
	t.Helper()
	cr := testCR()
	objects := []runtime.Object{cr}
	for _, o := range existing {
		objects = append(objects, o)
	}
	client := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(
		runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			gvrDeployment:         "DeploymentList",
			gvrControllerRevision: "ControllerRevisionList",
			GVRApp:                "DBOSApplicationList",
		}, objects...)

	// The fake client does not implement server-side apply — it looks the
	// object up and fails instead of creating it. Accept apply patches the way
	// a real API server would; the action is recorded either way, and these
	// tests assert on what was applied, not on the resulting object.
	client.PrependReactor("patch", "deployments", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patch, ok := action.(k8stesting.PatchAction)
		if !ok || patch.GetPatchType() != types.ApplyPatchType {
			return false, nil, nil
		}
		applied := &unstructured.Unstructured{}
		if err := json.Unmarshal(patch.GetPatch(), &applied.Object); err != nil {
			return true, nil, err
		}
		return true, applied, nil
	})

	s := store.NewInMemory()
	s.Set(appName(cr), result)
	m := NewManager(Options{Client: client, Store: s, PollInterval: 5 * time.Second})
	// Only the reconcile actions are of interest; drop the seeding ones.
	client.ClearActions()
	return &versionFixture{manager: m, client: client, cr: cr}
}

func (f *versionFixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.manager.reconcileOldVersions(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileOldVersions: %v", err)
	}
}

// applied returns the deployment name → applied manifest for every
// server-side-apply patch recorded.
func (f *versionFixture) applied(t *testing.T) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, action := range f.client.Actions() {
		patch, ok := action.(k8stesting.PatchAction)
		if !ok || patch.GetPatchType() != types.ApplyPatchType {
			continue
		}
		var manifest map[string]any
		if err := json.Unmarshal(patch.GetPatch(), &manifest); err != nil {
			t.Fatalf("unmarshal applied manifest: %v", err)
		}
		out[patch.GetName()] = manifest
	}
	return out
}

// appliedReplicas reads spec.replicas out of an applied manifest. The manifest
// went through JSON, so the number is a float64 rather than the int64
// unstructured helpers expect.
func appliedReplicas(t *testing.T, manifest map[string]any) int {
	t.Helper()
	value, found, err := unstructured.NestedFieldNoCopy(manifest, "spec", "replicas")
	if err != nil || !found {
		t.Fatalf("spec.replicas missing from applied manifest (err=%v)", err)
	}
	number, ok := value.(float64)
	if !ok {
		t.Fatalf("spec.replicas is %T, want a number", value)
	}
	return int(number)
}

func (f *versionFixture) deleted() []string {
	var names []string
	for _, action := range f.client.Actions() {
		if del, ok := action.(k8stesting.DeleteAction); ok {
			names = append(names, del.GetName())
		}
	}
	return names
}

// versionDeployment is a Deployment as the operator would have left it for an
// old version: the labels the reconciler selects on, and the CR's owner ref.
func versionDeployment(name, slug string, owned bool) *unstructured.Unstructured {
	d := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      name,
			"namespace": "dbos",
			"labels": map[string]any{
				"app":                          "myapp",
				"app.kubernetes.io/managed-by": fieldManager,
				versionLabel:                   slug,
			},
		},
		"spec": map[string]any{"replicas": int64(1)},
	}}
	if owned {
		d.SetOwnerReferences([]metav1.OwnerReference{{
			APIVersion: "dbos.dev/v1alpha1", Kind: "DBOSApplication",
			Name: "myapp", UID: "uid-123",
		}})
	}
	return d
}

func result(polledAt time.Time, versions ...conductor.VersionRecommendation) store.Result {
	return store.Result{OldVersions: versions, PolledAt: polledAt}
}

// Every non-latest entry gets a Deployment sized to its recommendation.
func TestReconcileOldVersionsApplies(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 3},
		conductor.VersionRecommendation{ApplicationVersion: "v2", DesiredExecutors: 7},
	))
	f.reconcile(t)

	applied := f.applied(t)
	if len(applied) != 2 {
		t.Fatalf("applied %d deployments, want 2: %v", len(applied), applied)
	}
	for version, wantReplicas := range map[string]int{"v1": 3, "v2": 7} {
		manifest, ok := applied[versionDeploymentName("myapp", version)]
		if !ok {
			t.Fatalf("no apply for version %q: %v", version, applied)
		}
		if got := appliedReplicas(t, manifest); got != wantReplicas {
			t.Errorf("version %q replicas = %d, want %d", version, got, wantReplicas)
		}
		// The pods must register this version, not the CR's authored one.
		containers, _, _ := unstructured.NestedSlice(manifest, "spec", "template", "spec", "containers")
		if len(containers) == 0 {
			t.Fatalf("version %q manifest has no containers", version)
		}
		env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
		var pinned string
		for _, e := range env {
			if v, ok := e.(map[string]any); ok && v["name"] == "DBOS__APPVERSION" {
				pinned, _ = v["value"].(string)
			}
		}
		if pinned != version {
			t.Errorf("version %q pinned DBOS__APPVERSION = %q, want %q", version, pinned, version)
		}
	}
	if got := f.deleted(); len(got) != 0 {
		t.Errorf("deleted %v, want nothing deleted", got)
	}
}

// desired 0 is not a teardown signal: the version is still in the response, so
// it keeps one executor for work that adds no queue depth.
func TestReconcileOldVersionsKeepsZeroDesiredAtOneReplica(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 0},
	), versionDeployment(versionDeploymentName("myapp", "v1"), versionSlug("v1"), true))
	f.reconcile(t)

	manifest, ok := f.applied(t)[versionDeploymentName("myapp", "v1")]
	if !ok {
		t.Fatalf("no apply for the zero-desired version: %v", f.applied(t))
	}
	if got := appliedReplicas(t, manifest); got != 1 {
		t.Errorf("replicas = %d, want 1", got)
	}
	if got := f.deleted(); len(got) != 0 {
		t.Errorf("deleted %v on a zero-desired version, want nothing deleted", got)
	}
}

// A version that left the response is torn down; one still present is not.
func TestReconcileOldVersionsDeletesDepartedVersions(t *testing.T) {
	staying := versionDeploymentName("myapp", "v1")
	departed := versionDeploymentName("myapp", "v0")
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 2},
	),
		versionDeployment(staying, versionSlug("v1"), true),
		versionDeployment(departed, versionSlug("v0"), true),
	)
	f.reconcile(t)

	deleted := f.deleted()
	if len(deleted) != 1 || deleted[0] != departed {
		t.Fatalf("deleted = %v, want [%s]", deleted, departed)
	}
}

// The main Deployment carries no version label, so it can never be swept up by
// the teardown pass — not even when no old version is reported at all.
func TestReconcileOldVersionsLeavesMainDeployment(t *testing.T) {
	main := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "myapp",
			"namespace": "dbos",
			"labels": map[string]any{
				"app":                          "myapp",
				"app.kubernetes.io/managed-by": fieldManager,
			},
		},
	}}
	f := newVersionFixture(t, result(time.Now()), main)
	f.reconcile(t)

	if got := f.deleted(); len(got) != 0 {
		t.Fatalf("deleted %v, want the main deployment untouched", got)
	}
}

// A Deployment carrying the labels but owned by something else is left alone.
func TestReconcileOldVersionsSkipsForeignDeployment(t *testing.T) {
	foreign := versionDeployment(versionDeploymentName("myapp", "v0"), versionSlug("v0"), false)
	f := newVersionFixture(t, result(time.Now()), foreign)
	f.reconcile(t)

	if got := f.deleted(); len(got) != 0 {
		t.Fatalf("deleted %v, want the unowned deployment untouched", got)
	}
}

// Absence of fresh data is never a teardown signal: no poll yet, no policy, or
// a stale result all leave the fleet exactly as it is.
func TestReconcileOldVersionsIgnoresUnusableResults(t *testing.T) {
	departed := versionDeployment(versionDeploymentName("myapp", "v0"), versionSlug("v0"), true)

	cases := map[string]func(*versionFixture){
		"no poll result yet": func(f *versionFixture) {
			f.manager.opts.Store.Delete(appName(f.cr))
		},
		"no autoscaling policy": func(f *versionFixture) {
			f.manager.opts.Store.Set(appName(f.cr), store.Result{NoPolicy: true, PolledAt: time.Now()})
		},
		"stale result": func(f *versionFixture) {
			f.manager.opts.Store.Set(appName(f.cr), result(time.Now().Add(-time.Hour)))
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			f := newVersionFixture(t, result(time.Now()), departed.DeepCopy())
			mutate(f)
			f.reconcile(t)
			if got := f.deleted(); len(got) != 0 {
				t.Errorf("deleted %v, want nothing deleted", got)
			}
			if got := f.applied(t); len(got) != 0 {
				t.Errorf("applied %v, want nothing applied", got)
			}
		})
	}
}

// latestDeployment is the app's latest-version Deployment as the fixture's
// cluster sees it: KEDA-owned spec.replicas plus the observed pod count. The
// drain budget must ignore both — the tests below plant it to prove that.
func latestDeployment(specReplicas, statusReplicas int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]any{
			"name":      "myapp",
			"namespace": "dbos",
			"labels": map[string]any{
				"app":                          "myapp",
				"app.kubernetes.io/managed-by": fieldManager,
			},
		},
		"spec":   map[string]any{"replicas": specReplicas},
		"status": map[string]any{"replicas": statusReplicas},
	}}
}

func setMaxOldVersionsReplicas(t *testing.T, cr *unstructured.Unstructured, cap int64) {
	t.Helper()
	if err := unstructured.SetNestedField(cr.Object, cap, "spec", "maxOldVersionsReplicas"); err != nil {
		t.Fatalf("set maxOldVersionsReplicas: %v", err)
	}
}

// With maxOldVersionsReplicas authored, old versions share that pod budget:
// equal split, capped at each version's need, leftovers waterfalling — here
// budget 6 over needs 8/2/1 lands 3/2/1.
func TestReconcileOldVersionsBudgetSpreadsEqually(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v2", DesiredExecutors: 8},
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 2},
		conductor.VersionRecommendation{ApplicationVersion: "v0", DesiredExecutors: 1},
	), latestDeployment(4, 4))
	setMaxOldVersionsReplicas(t, f.cr, 6)
	f.reconcile(t)

	applied := f.applied(t)
	for version, wantReplicas := range map[string]int{"v2": 3, "v1": 2, "v0": 1} {
		manifest, ok := applied[versionDeploymentName("myapp", version)]
		if !ok {
			t.Fatalf("no apply for version %q: %v", version, applied)
		}
		if got := appliedReplicas(t, manifest); got != wantReplicas {
			t.Errorf("version %q replicas = %d, want %d", version, got, wantReplicas)
		}
	}
}

// Fewer budget pods than versions: the newest versions (first in Conductor's
// order) get one pod each, the oldest is parked at zero — present, not
// deleted, so it picks up a slot when a newer version drains.
func TestReconcileOldVersionsBudgetParksOldestAtZero(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v2", DesiredExecutors: 5},
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 5},
		conductor.VersionRecommendation{ApplicationVersion: "v0", DesiredExecutors: 5},
	), latestDeployment(1, 1))
	setMaxOldVersionsReplicas(t, f.cr, 2)
	f.reconcile(t)

	applied := f.applied(t)
	for version, wantReplicas := range map[string]int{"v2": 1, "v1": 1, "v0": 0} {
		manifest, ok := applied[versionDeploymentName("myapp", version)]
		if !ok {
			t.Fatalf("no apply for version %q: %v", version, applied)
		}
		if got := appliedReplicas(t, manifest); got != wantReplicas {
			t.Errorf("version %q replicas = %d, want %d", version, got, wantReplicas)
		}
	}
	if got := f.deleted(); len(got) != 0 {
		t.Errorf("deleted %v, want the parked version kept", got)
	}
}

// The budget is absolute: a rollout in flight on the main Deployment (status
// 6 vs spec 4) changes nothing — the drain fleet keeps its full allowance.
func TestReconcileOldVersionsBudgetIgnoresRollout(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v2", DesiredExecutors: 8},
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 2},
		conductor.VersionRecommendation{ApplicationVersion: "v0", DesiredExecutors: 1},
	), latestDeployment(4, 6))
	setMaxOldVersionsReplicas(t, f.cr, 6)
	f.reconcile(t)

	applied := f.applied(t)
	for version, wantReplicas := range map[string]int{"v2": 3, "v1": 2, "v0": 1} {
		if got := appliedReplicas(t, applied[versionDeploymentName("myapp", version)]); got != wantReplicas {
			t.Errorf("version %q replicas = %d, want %d", version, got, wantReplicas)
		}
	}
}

// An explicit 0 is a real budget: every old version parks at zero replicas,
// but none is deleted — presence in the response still means unfinished work.
func TestReconcileOldVersionsBudgetZeroParksAll(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 9},
	), latestDeployment(4, 4))
	setMaxOldVersionsReplicas(t, f.cr, 0)
	f.reconcile(t)

	manifest, ok := f.applied(t)[versionDeploymentName("myapp", "v1")]
	if !ok {
		t.Fatalf("no apply for v1: %v", f.applied(t))
	}
	if got := appliedReplicas(t, manifest); got != 0 {
		t.Errorf("replicas = %d, want 0 (parked)", got)
	}
	if got := f.deleted(); len(got) != 0 {
		t.Errorf("deleted %v, want the parked version kept", got)
	}
}

// Without an authored maxOldVersionsReplicas nothing is budgeted — behavior
// is unchanged even when the main Deployment is present with live counts.
func TestReconcileOldVersionsUnbudgetedWithoutCap(t *testing.T) {
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: "v1", DesiredExecutors: 7},
	), latestDeployment(1, 1))
	f.reconcile(t)

	manifest, ok := f.applied(t)[versionDeploymentName("myapp", "v1")]
	if !ok {
		t.Fatalf("no apply for v1: %v", f.applied(t))
	}
	if got := appliedReplicas(t, manifest); got != 7 {
		t.Errorf("replicas = %d, want the uncapped recommendation 7", got)
	}
}
