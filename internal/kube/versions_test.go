package kube

import (
	"slices"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestVersionSlug(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"rollout-1754321098", "rollout-1754321098"},
		{"9a3f0c", "9a3f0c"},
		{"", "unversioned"},
	} {
		if got := versionSlug(tc.in); got != tc.want {
			t.Errorf("versionSlug(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	for _, in := range []string{"V1.2.3+Build", "___", strings.Repeat("ab", 32)} {
		got := versionSlug(in)
		if len(got) > 40 {
			t.Errorf("versionSlug(%q) = %q, longer than 40 chars", in, got)
		}
		if got == sanitizeVersion(in) {
			t.Errorf("versionSlug(%q) = %q, want a disambiguating hash", in, got)
		}
	}

	if a, b := versionSlug("v1.0"), versionSlug("v1+0"); a == b {
		t.Errorf("versionSlug collision: %q and %q both slug to %q", "v1.0", "v1+0", a)
	}
	long := strings.Repeat("ab", 32)
	if a, b := versionSlug(long+"one"), versionSlug(long+"two"); a == b {
		t.Errorf("versionSlug collision on truncated versions: both %q", a)
	}
}

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

	for _, path := range [][]string{
		{"metadata", "labels"},
		{"spec", "selector", "matchLabels"},
		{"spec", "template", "metadata", "labels"},
	} {
		labels, _, _ := unstructured.NestedMap(d, path...)
		if labels["app"] != "myapp" || labels[versionLabel] != "v1" {
			t.Errorf("labels at %v = %v, want app=myapp and %s=v1", path, labels, versionLabel)
		}
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

func TestAllocateDrainBudget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		needs  []int
		budget int
		want   []int
	}{
		{"waterfall", []int{8, 2, 1}, 6, []int{3, 2, 1}},
		{"lifo", []int{1, 1, 1}, 2, []int{1, 1, 0}},
		{"zero budget parks everything", []int{5, 5}, 0, []int{0, 0}},
		{"ample budget never exceeds needs", []int{2, 3}, 99, []int{2, 3}},
		{"no versions", []int{}, 5, []int{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := allocateDrainBudget(tc.needs, tc.budget); !slices.Equal(got, tc.want) {
				t.Errorf("allocateDrainBudget(%v, %d) = %v, want %v", tc.needs, tc.budget, got, tc.want)
			}
		})
	}
}
