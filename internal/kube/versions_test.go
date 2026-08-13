package kube

import (
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestBuildVersionDeployment(t *testing.T) {
	cr := testCR()
	container := map[string]any{
		"name":  "app",
		"image": "img:v1",
		"env": []any{
			map[string]any{"name": "DBOS__APPVERSION", "value": "authored"},
			map[string]any{"name": "OTHER", "value": "x"},
		},
	}
	_ = unstructured.SetNestedSlice(cr.Object, []any{container}, "spec", "template", "spec", "containers")

	template, _, _ := unstructured.NestedMap(cr.Object, "spec", "template")
	d, err := buildVersionDeployment(cr, "v1", 3, template)
	if err != nil {
		t.Fatalf("buildVersionDeployment: %v", err)
	}
	u := &unstructured.Unstructured{Object: d}

	if got := u.GetName(); got != "myapp-v1" {
		t.Errorf("name = %q, want myapp-v1", got)
	}

	replicas, found, _ := unstructured.NestedInt64(d, "spec", "replicas")
	if !found || replicas != 3 {
		t.Errorf("spec.replicas = %d (found=%v), want 3", replicas, found)
	}

	labels, _, _ := unstructured.NestedMap(d, "metadata", "labels")
	if labels["app"] != "myapp" || labels[versionLabel] != "v1" {
		t.Errorf("metadata labels = %v, want app=myapp and %s=v1", labels, versionLabel)
	}
	selector, _, _ := unstructured.NestedMap(d, "spec", "selector", "matchLabels")
	if len(selector) != 1 || selector[versionLabel] != "v1" {
		t.Errorf("selector = %v, want only %s=v1", selector, versionLabel)
	}
	podLabels, _, _ := unstructured.NestedMap(d, "spec", "template", "metadata", "labels")
	if _, ok := podLabels["app"]; ok {
		t.Errorf("pod labels = %v, want no app= label", podLabels)
	}
	if podLabels[versionLabel] != "v1" {
		t.Errorf("pod labels = %v, want %s=v1", podLabels, versionLabel)
	}

	containers, _, _ := unstructured.NestedSlice(d, "spec", "template", "spec", "containers")
	env, _, _ := unstructured.NestedSlice(containers[0].(map[string]any), "env")
	var appVersions []string
	var authored bool
	for _, e := range env {
		v := e.(map[string]any)
		switch v["name"] {
		case "DBOS__APPVERSION":
			appVersions = append(appVersions, v["value"].(string))
		case "OTHER":
			authored = true
		}
	}
	if len(appVersions) != 1 || appVersions[0] != "v1" {
		t.Errorf("DBOS__APPVERSION values = %v, want exactly [v1]", appVersions)
	}
	if !authored {
		t.Error("authored env dropped from the versioned deployment")
	}

	refs := u.GetOwnerReferences()
	if len(refs) != 1 || refs[0].Kind != "DBOSApplication" {
		t.Errorf("ownerReferences = %+v", refs)
	}
}

func TestBuildVersionDeploymentCarriesNoStrategy(t *testing.T) {
	cr := testCR()
	_ = unstructured.SetNestedField(cr.Object, "2", "spec", "strategy", "rollingUpdate", "maxSurge")
	template, _, _ := unstructured.NestedMap(cr.Object, "spec", "template")
	d, err := buildVersionDeployment(cr, "v1", 3, template)
	if err != nil {
		t.Fatalf("buildVersionDeployment: %v", err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(d, "spec", "strategy"); found {
		t.Error("versioned deployment carries spec.strategy, want none")
	}
}
