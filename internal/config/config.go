// Package config loads the operator's static configuration from a YAML file
// (typically mounted from a ConfigMap) and validates it. There is no CRD,
// no controller-runtime, no dynamic reconciliation: changes to the ConfigMap
// take effect on pod restart.
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"sigs.k8s.io/yaml"
)

// Duration wraps time.Duration so YAML/JSON values like "1s" / "30s" decode
// correctly. time.Duration's default UnmarshalJSON expects a nanosecond int.
type Duration time.Duration

// UnmarshalJSON accepts either a quoted duration string ("1s") or a numeric
// nanosecond value. Strings are parsed via time.ParseDuration.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err == nil {
		v, err := time.ParseDuration(s)
		if err != nil {
			return fmt.Errorf("parse duration %q: %w", s, err)
		}
		*d = Duration(v)
		return nil
	}
	var n int64
	if err := json.Unmarshal(b, &n); err != nil {
		return fmt.Errorf("duration must be a string or nanoseconds: %w", err)
	}
	*d = Duration(n)
	return nil
}

// Native returns the underlying time.Duration.
func (d Duration) Native() time.Duration { return time.Duration(d) }

// Config is the top-level operator configuration.
type Config struct {
	Conductor  Conductor  `json:"conductor"`
	Poller     Poller     `json:"poller,omitempty"`
	Apps       []App      `json:"apps"`
	MetricsAPI MetricsAPI `json:"metricsAPI,omitempty"`
}

// Conductor describes the Conductor instance the operator polls.
type Conductor struct {
	// Endpoint is the full base URL of the Conductor HTTP API up through and
	// including any cloud-specific path prefix (e.g. /conductor/v1alpha1).
	// Optional; when empty the operator derives it from the DBOS_DOMAIN env
	// var as https://${DBOS_DOMAIN}/conductor/v1alpha1.
	Endpoint string `json:"endpoint,omitempty"`

	// OrgName is the Conductor organization name (Conductor resolves the
	// internal org ID server-side).
	OrgName string `json:"orgName"`

	// JWTPath is the filesystem path to a file containing the bearer JWT.
	// Typically a Secret mounted as a file. Read once at startup.
	JWTPath string `json:"jwtPath"`

	// InsecureSkipVerify disables TLS verification of the Conductor endpoint.
	// Only for local/dev clusters.
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
}

// Poller controls per-app poll cadence. All apps share these knobs.
type Poller struct {
	// Interval is the steady-state poll cadence (default 1s).
	Interval Duration `json:"interval,omitempty"`

	// MaxBackoff caps the per-app interval after consecutive failed ticks
	// (default 30s). The operator doubles the interval on each failed tick
	// (with ±10% jitter) up to this cap; resets to Interval on success.
	MaxBackoff Duration `json:"maxBackoff,omitempty"`
}

// App is one DBOS application whose queues should be polled.
type App struct {
	// Name is the DBOS application name as registered in Conductor.
	Name string `json:"name"`

	// Queues lists the DBOS queue names whose load should be exposed.
	Queues []string `json:"queues"`
}

// MetricsAPI toggles the External Metrics API server (consumed by HPA).
// The TLS port and cert directory are CLI flags on the underlying AdapterBase
// (--secure-port, --tls-cert-file, --tls-private-key-file), not part of this config.
type MetricsAPI struct {
	Enabled bool `json:"enabled,omitempty"`
}

// Load reads and validates the operator config from path. Defaults are
// applied for omitted optional fields.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// LoadJWT reads the bearer token from the file referenced by Conductor.JWTPath
// and returns its trimmed contents. Called separately from Load so the JWT can
// be re-read on rotation in the future without re-parsing the whole config.
func LoadJWT(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read jwt %s: %w", path, err)
	}
	tok := strings.TrimSpace(string(raw))
	if tok == "" {
		return "", fmt.Errorf("jwt file %s is empty", path)
	}
	return tok, nil
}

func (c *Config) applyDefaults() {
	if c.Poller.Interval == 0 {
		c.Poller.Interval = Duration(time.Second)
	}
	if c.Poller.MaxBackoff == 0 {
		c.Poller.MaxBackoff = Duration(30 * time.Second)
	}
}

func (c *Config) validate() error {
	if c.Conductor.OrgName == "" {
		return errors.New("conductor.orgName is required")
	}
	if c.Conductor.JWTPath == "" {
		return errors.New("conductor.jwtPath is required")
	}
	if len(c.Apps) == 0 {
		return errors.New("at least one app must be configured")
	}
	for i, a := range c.Apps {
		if a.Name == "" {
			return fmt.Errorf("apps[%d].name is required", i)
		}
		if len(a.Queues) == 0 {
			return fmt.Errorf("apps[%d].queues must list at least one queue", i)
		}
		for j, q := range a.Queues {
			if q == "" {
				return fmt.Errorf("apps[%d].queues[%d] is empty", i, j)
			}
		}
	}
	if c.Poller.MaxBackoff.Native() < c.Poller.Interval.Native() {
		return errors.New("poller.maxBackoff must be >= poller.interval")
	}
	return nil
}
