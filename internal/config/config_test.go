package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDurationUnmarshal(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want time.Duration
	}{
		{"string", `"5s"`, 5 * time.Second},
		{"nanoseconds", `2500000000`, 2500 * time.Millisecond},
		{"zero string", `"0s"`, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var d Duration
			if err := json.Unmarshal([]byte(tc.in), &d); err != nil {
				t.Fatalf("unmarshal %q: %v", tc.in, err)
			}
			if d.Native() != tc.want {
				t.Fatalf("got %v, want %v", d.Native(), tc.want)
			}
		})
	}
}

func TestDurationUnmarshalRejectsGarbage(t *testing.T) {
	var d Duration
	if err := json.Unmarshal([]byte(`"not-a-duration"`), &d); err == nil {
		t.Fatalf("expected error for bad duration string")
	}
	if err := json.Unmarshal([]byte(`true`), &d); err == nil {
		t.Fatalf("expected error for bool")
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	return p
}

func TestLoadAppliesDefaults(t *testing.T) {
	p := writeConfig(t, `
conductor:
  orgName: org
  jwtPath: /tmp/jwt
apps:
  - name: app1
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Poller.Interval.Native() != time.Second {
		t.Fatalf("default Interval = %v, want 1s", cfg.Poller.Interval.Native())
	}
	if cfg.Poller.MaxBackoff.Native() != 30*time.Second {
		t.Fatalf("default MaxBackoff = %v, want 30s", cfg.Poller.MaxBackoff.Native())
	}
}

func TestLoadParsesUserValues(t *testing.T) {
	p := writeConfig(t, `
conductor:
  orgName: my-org
  endpoint: http://conductor.local:8090
  jwtPath: /var/run/jwt
  insecureSkipVerify: true
poller:
  interval: 2s
  maxBackoff: 1m
apps:
  - name: a1
  - name: a2
metricsAPI:
  enabled: true
`)
	cfg, err := Load(p)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Conductor.OrgName != "my-org" {
		t.Errorf("OrgName = %q", cfg.Conductor.OrgName)
	}
	if cfg.Conductor.Endpoint != "http://conductor.local:8090" {
		t.Errorf("Endpoint = %q", cfg.Conductor.Endpoint)
	}
	if !cfg.Conductor.InsecureSkipVerify {
		t.Errorf("InsecureSkipVerify = false, want true")
	}
	if cfg.Poller.Interval.Native() != 2*time.Second {
		t.Errorf("Interval = %v", cfg.Poller.Interval.Native())
	}
	if cfg.Poller.MaxBackoff.Native() != time.Minute {
		t.Errorf("MaxBackoff = %v", cfg.Poller.MaxBackoff.Native())
	}
	if len(cfg.Apps) != 2 || cfg.Apps[0].Name != "a1" || cfg.Apps[1].Name != "a2" {
		t.Errorf("Apps = %v", cfg.Apps)
	}
	if !cfg.MetricsAPI.Enabled {
		t.Errorf("MetricsAPI.Enabled = false, want true")
	}
}

func TestLoadValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing orgName",
			yaml: `
conductor:
  jwtPath: /tmp/jwt
apps:
  - name: a
`,
			want: "orgName",
		},
		{
			name: "missing jwtPath",
			yaml: `
conductor:
  orgName: o
apps:
  - name: a
`,
			want: "jwtPath",
		},
		{
			name: "no apps",
			yaml: `
conductor:
  orgName: o
  jwtPath: /tmp/jwt
apps: []
`,
			want: "at least one app",
		},
		{
			name: "empty app name",
			yaml: `
conductor:
  orgName: o
  jwtPath: /tmp/jwt
apps:
  - name: ""
`,
			want: "apps[0].name",
		},
		{
			name: "maxBackoff smaller than interval",
			yaml: `
conductor:
  orgName: o
  jwtPath: /tmp/jwt
poller:
  interval: 10s
  maxBackoff: 1s
apps:
  - name: a
`,
			want: "maxBackoff",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := writeConfig(t, tc.yaml)
			_, err := Load(p)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
}

func TestLoadJWT(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "jwt")
	if err := os.WriteFile(p, []byte("  hunter2  \n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	tok, err := LoadJWT(p)
	if err != nil {
		t.Fatalf("LoadJWT: %v", err)
	}
	if tok != "hunter2" {
		t.Fatalf("token = %q, want %q (trimmed)", tok, "hunter2")
	}
}

func TestLoadJWTEmpty(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "jwt")
	if err := os.WriteFile(p, []byte("   \n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := LoadJWT(p); err == nil {
		t.Fatalf("expected error for empty jwt file")
	}
}

func TestLoadJWTMissing(t *testing.T) {
	if _, err := LoadJWT("/nonexistent/jwt"); err == nil {
		t.Fatalf("expected error for missing jwt file")
	}
}
