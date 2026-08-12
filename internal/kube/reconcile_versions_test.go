package kube

import (
	"context"
	"encoding/json"
	"fmt"
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

	// The fake client does not implement server-side apply; accept the patches.
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
	client.ClearActions()
	return &versionFixture{manager: m, client: client, cr: cr}
}

func (f *versionFixture) reconcile(t *testing.T) {
	t.Helper()
	if err := f.manager.reconcileOldVersions(context.Background(), f.cr, klog.Background()); err != nil {
		t.Fatalf("reconcileOldVersions: %v", err)
	}
}

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

// appliedReplicas reads spec.replicas; the JSON round-trip makes it a float64.
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

func seededVersion(n int) string {
	return fmt.Sprintf("%d-%016d", n, n)
}

// Drain deployments are only built from snapshots; tests seed one per version.
func ownedSnapshots(versions ...string) []*unstructured.Unstructured {
	var out []*unstructured.Unstructured
	for _, v := range versions {
		hash, _ := parseSeededVersion(v)
		template, _, _ := unstructured.NestedMap(testCR().Object, "spec", "template")
		out = append(out, snapshotFor(snapshotName("myapp", hash), "", template))
	}
	return out
}

func TestReconcileOldVersionsApplies(t *testing.T) {
	v1, v2 := seededVersion(1), seededVersion(2)
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: v1, DesiredExecutors: 3},
		conductor.VersionRecommendation{ApplicationVersion: v2, DesiredExecutors: 7},
	), ownedSnapshots(v1, v2)...)
	f.reconcile(t)

	applied := f.applied(t)
	if len(applied) != 2 {
		t.Fatalf("applied %d deployments, want 2: %v", len(applied), applied)
	}
	for version, wantReplicas := range map[string]int{v1: 3, v2: 7} {
		manifest, ok := applied[versionDeploymentName("myapp", version)]
		if !ok {
			t.Fatalf("no apply for version %q: %v", version, applied)
		}
		if got := appliedReplicas(t, manifest); got != wantReplicas {
			t.Errorf("version %q replicas = %d, want %d", version, got, wantReplicas)
		}
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

func TestReconcileOldVersionsParksZeroDesiredWithoutDeleting(t *testing.T) {
	v1 := seededVersion(1)
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: v1, DesiredExecutors: 0},
	), append(ownedSnapshots(v1), versionDeployment(versionDeploymentName("myapp", v1), versionSlug(v1), true))...)
	f.reconcile(t)

	manifest, ok := f.applied(t)[versionDeploymentName("myapp", v1)]
	if !ok {
		t.Fatalf("no apply for the zero-desired version: %v", f.applied(t))
	}
	if got := appliedReplicas(t, manifest); got != 0 {
		t.Errorf("replicas = %d, want 0 (Conductor's rollout cap is honored, not floored)", got)
	}
	if got := f.deleted(); len(got) != 0 {
		t.Errorf("deleted %v on a zero-desired version, want nothing deleted", got)
	}
}

func TestReconcileOldVersionsDeletesDepartedVersions(t *testing.T) {
	v0, v1 := seededVersion(0), seededVersion(1)
	staying := versionDeploymentName("myapp", v1)
	departed := versionDeploymentName("myapp", v0)
	f := newVersionFixture(t, result(time.Now(),
		conductor.VersionRecommendation{ApplicationVersion: v1, DesiredExecutors: 2},
	), append(ownedSnapshots(v1),
		versionDeployment(staying, versionSlug(v1), true),
		versionDeployment(departed, versionSlug(v0), true),
	)...)
	f.reconcile(t)

	deleted := f.deleted()
	if len(deleted) != 1 || deleted[0] != departed {
		t.Fatalf("deleted = %v, want [%s]", deleted, departed)
	}
}

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

func TestReconcileOldVersionsSkipsForeignDeployment(t *testing.T) {
	foreign := versionDeployment(versionDeploymentName("myapp", "v0"), versionSlug("v0"), false)
	f := newVersionFixture(t, result(time.Now()), foreign)
	f.reconcile(t)

	if got := f.deleted(); len(got) != 0 {
		t.Fatalf("deleted %v, want the unowned deployment untouched", got)
	}
}

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
			f.manager.opts.Store.MarkStale(appName(f.cr))
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

