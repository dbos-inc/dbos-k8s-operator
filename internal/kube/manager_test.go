package kube

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func testCR() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "dbos.dev/v1alpha1",
		"kind":       "DBOSApplication",
		"metadata": map[string]any{
			"name":      "myapp",
			"namespace": "dbos",
			"uid":       "uid-123",
		},
		"spec": map[string]any{
			"template": map[string]any{
				"metadata": map[string]any{
					"labels": map[string]any{"custom": "label"},
				},
				"spec": map[string]any{
					"containers": []any{map[string]any{"name": "app", "image": "img:v1"}},
				},
			},
		},
	}}
}

func TestBuildDeployment(t *testing.T) {
	d, err := buildDeployment(testCR())
	if err != nil {
		t.Fatalf("buildDeployment: %v", err)
	}
	u := &unstructured.Unstructured{Object: d}

	if got := u.GetName(); got != "myapp" {
		t.Errorf("name = %q", got)
	}
	if got := u.GetNamespace(); got != "dbos" {
		t.Errorf("namespace = %q", got)
	}

	// Selector and template labels must agree, with CR labels preserved.
	sel, _, _ := unstructured.NestedString(d, "spec", "selector", "matchLabels", "app")
	if sel != "myapp" {
		t.Errorf("selector app label = %q", sel)
	}
	tmplLabels, _, _ := unstructured.NestedMap(d, "spec", "template", "metadata", "labels")
	if tmplLabels["app"] != "myapp" || tmplLabels["custom"] != "label" {
		t.Errorf("template labels = %v", tmplLabels)
	}

	// replicas must not be applied: the autoscaler owns that field.
	if _, found, _ := unstructured.NestedFieldNoCopy(d, "spec", "replicas"); found {
		t.Error("spec.replicas must not be set by the operator")
	}

	// Owner reference enables GC of the Deployment when the CR goes away.
	refs := u.GetOwnerReferences()
	if len(refs) != 1 || refs[0].Kind != "DBOSApplication" || string(refs[0].UID) != "uid-123" || refs[0].Controller == nil || !*refs[0].Controller {
		t.Errorf("ownerReferences = %+v", refs)
	}

	// The pod spec passes through untouched — version seeding happens in
	// reconcileDeployment, not here.
	containers, _, _ := unstructured.NestedSlice(d, "spec", "template", "spec", "containers")
	if len(containers) != 1 {
		t.Fatalf("containers = %+v", containers)
	}
	if env, found, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env"); found {
		t.Errorf("env = %+v, want the authored pod spec left alone", env)
	}
}

// The CR's own env is carried through as authored.
func TestBuildDeploymentKeepsAuthoredEnv(t *testing.T) {
	cr := testCR()
	container := map[string]any{
		"name":  "app",
		"image": "img:v1",
		"env": []any{
			map[string]any{"name": "OTHER", "value": "x"},
			map[string]any{"name": "DBOS__VMID", "value": "pinned-id"},
		},
	}
	_ = unstructured.SetNestedSlice(cr.Object, []any{container}, "spec", "template", "spec", "containers")

	d, err := buildDeployment(cr)
	if err != nil {
		t.Fatalf("buildDeployment: %v", err)
	}
	containers, _, _ := unstructured.NestedSlice(d, "spec", "template", "spec", "containers")
	env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
	if len(env) != 2 {
		t.Fatalf("env = %+v, want the two authored vars only", env)
	}
	if v := env[1].(map[string]any)["value"]; v != "pinned-id" {
		t.Errorf("DBOS__VMID = %v, want the authored value kept", v)
	}
}

func TestBuildDeploymentMissingTemplate(t *testing.T) {
	cr := testCR()
	delete(cr.Object["spec"].(map[string]any), "template")
	if _, err := buildDeployment(cr); err == nil {
		t.Fatal("expected error for missing spec.template")
	}
}

func TestAppNameDefaultsToCRName(t *testing.T) {
	cr := testCR()
	if got := appName(cr); got != "myapp" {
		t.Errorf("appName = %q, want CR name", got)
	}
	_ = unstructured.SetNestedField(cr.Object, "conductor-app", "spec", "appName")
	if got := appName(cr); got != "conductor-app" {
		t.Errorf("appName = %q, want spec.appName override", got)
	}
}

// spec.strategy passes through verbatim to the main Deployment — same field
// names as Deployment.spec.strategy — and its absence sets nothing.
func TestSetStrategy(t *testing.T) {
	cr := testCR()
	d, _ := buildDeployment(cr)
	if err := setStrategy(d, cr); err != nil {
		t.Fatalf("setStrategy: %v", err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(d, "spec", "strategy"); found {
		t.Error("strategy set on the deployment with none authored")
	}

	authored := map[string]any{
		"type": "RollingUpdate",
		"rollingUpdate": map[string]any{
			"maxSurge":       int64(2),
			"maxUnavailable": "0%",
		},
	}
	_ = unstructured.SetNestedMap(cr.Object, authored, "spec", "strategy")
	d, _ = buildDeployment(cr)
	if err := setStrategy(d, cr); err != nil {
		t.Fatalf("setStrategy: %v", err)
	}
	surge, _, _ := unstructured.NestedInt64(d, "spec", "strategy", "rollingUpdate", "maxSurge")
	unavailable, _, _ := unstructured.NestedString(d, "spec", "strategy", "rollingUpdate", "maxUnavailable")
	kind, _, _ := unstructured.NestedString(d, "spec", "strategy", "type")
	if kind != "RollingUpdate" || surge != 2 || unavailable != "0%" {
		t.Errorf("strategy = type %q, maxSurge %d, maxUnavailable %q; want the authored values", kind, surge, unavailable)
	}
}
