package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(write(t, `
conductor:
  orgName: myorg
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Poller.Interval.Native() != 30*time.Second {
		t.Errorf("interval = %v", cfg.Poller.Interval.Native())
	}
	if cfg.HTTP.Listen != ":8080" {
		t.Errorf("listen = %q", cfg.HTTP.Listen)
	}
	if cfg.Kubernetes.ReconcileInterval.Native() != 10*time.Second {
		t.Errorf("reconcileInterval = %v", cfg.Kubernetes.ReconcileInterval.Native())
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(write(t, `
conductor:
  orgName: myorg
  endpoint: http://conductor.dbos.svc.cluster.local:8090
poller:
  interval: 2s
http:
  listen: ":9090"
kubernetes:
  namespace: dbos
  reconcileInterval: 5s
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Conductor.Endpoint == "" || cfg.Kubernetes.Namespace != "dbos" {
		t.Errorf("cfg = %+v", cfg)
	}
	if cfg.Poller.Interval.Native() != 2*time.Second {
		t.Errorf("durations not parsed: %+v", cfg)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]string{
		"missing orgName":            `poller: {interval: 1s}`,
		"negative poll interval":     "conductor: {orgName: o}\npoller: {interval: -1s}",
		"negative reconcileInterval": "conductor: {orgName: o}\nkubernetes: {reconcileInterval: -5s}",
	}
	for name, content := range cases {
		if _, err := Load(write(t, content)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
