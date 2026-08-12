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
	if cfg.Poller.MaxBackoff.Native() != 30*time.Second {
		t.Errorf("maxBackoff = %v", cfg.Poller.MaxBackoff.Native())
	}
	if cfg.HTTP.Listen != ":8080" {
		t.Errorf("listen = %q", cfg.HTTP.Listen)
	}
	if cfg.Kubernetes.ReconcileInterval.Native() != 10*time.Second {
		t.Errorf("reconcileInterval = %v", cfg.Kubernetes.ReconcileInterval.Native())
	}
}

func TestLoadDerivedDefaults(t *testing.T) {
	cases := []struct {
		interval    string
		wantBackoff time.Duration
	}{
		{"5s", 30 * time.Second},  // floor applies
		{"60s", 60 * time.Second}, // scales with the interval
	}
	for _, tc := range cases {
		cfg, err := Load(write(t, "conductor: {orgName: myorg}\npoller: {interval: "+tc.interval+"}"))
		if err != nil {
			t.Fatalf("interval %s: %v", tc.interval, err)
		}
		if cfg.Poller.MaxBackoff.Native() != tc.wantBackoff {
			t.Errorf("interval %s: maxBackoff = %v, want %v", tc.interval, cfg.Poller.MaxBackoff.Native(), tc.wantBackoff)
		}
	}
}

func TestLoadFull(t *testing.T) {
	cfg, err := Load(write(t, `
conductor:
  orgName: myorg
  endpoint: http://conductor.dbos.svc.cluster.local:8090
poller:
  interval: 2s
  maxBackoff: 20s
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
	if cfg.Poller.Interval.Native() != 2*time.Second || cfg.Poller.MaxBackoff.Native() != 20*time.Second {
		t.Errorf("durations not parsed: %+v", cfg)
	}
}

func TestLoadRejects(t *testing.T) {
	cases := map[string]string{
		"missing orgName":        `poller: {interval: 1s}`,
		"backoff below interval": "conductor: {orgName: o}\npoller: {interval: 10s, maxBackoff: 1s}",
	}
	for name, content := range cases {
		if _, err := Load(write(t, content)); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
