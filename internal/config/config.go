// Package config loads the operator's static configuration from a YAML file
// (typically mounted from a ConfigMap) and validates it. Apps are not listed
// here — they are DBOSApplication custom resources discovered at runtime.
// ConfigMap changes take effect on pod restart.
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

func (d Duration) Native() time.Duration { return time.Duration(d) }

type Config struct {
	Conductor  Conductor  `json:"conductor"`
	Poller     Poller     `json:"poller,omitempty"`
	HTTP       HTTP       `json:"http,omitempty"`
	Kubernetes Kubernetes `json:"kubernetes,omitempty"`
}

// Conductor describes the Conductor instance the operator polls.
type Conductor struct {
	// Endpoint is the full base URL of the Conductor HTTP API up through and
	// including any cloud-specific path prefix (e.g. /conductor/v1alpha1).
	// Optional; when empty the operator derives it from the DBOS_DOMAIN env
	// var as https://${DBOS_DOMAIN}/conductor/v1alpha1.
	Endpoint string `json:"endpoint,omitempty"`

	// OrgName is the Conductor organization, passed as the :org_id URL segment.
	OrgName string `json:"orgName"`

	// InsecureSkipVerify disables TLS verification of the Conductor endpoint.
	// Only for local/dev clusters.
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`
}

// JWTEnvVar is the environment variable from which LoadJWT reads the bearer
// JWT. Populated in the Deployment from a Secret via env.valueFrom.secretKeyRef.
const JWTEnvVar = "DBOS_CONDUCTOR_JWT"

// Poller controls the per-app autoscale poll cadence.
type Poller struct {
	// Interval is the steady-state poll cadence (default 30s, matching KEDA's
	// default polling frequency — polling much faster than the consumer reads
	// only loads Conductor for nothing).
	Interval Duration `json:"interval,omitempty"`

	// MaxBackoff caps the per-app interval after consecutive failed ticks
	// (default: interval, but at least 30s). The operator doubles the interval
	// on each failed tick (with ±10% jitter) up to this cap; resets to
	// Interval on success.
	MaxBackoff Duration `json:"maxBackoff,omitempty"`
}

// HTTP configures the plain-HTTP metrics endpoint KEDA polls.
type HTTP struct {
	// Listen is the address of the metrics endpoint (default ":8080").
	Listen string `json:"listen,omitempty"`
}

// Kubernetes configures CR discovery and Deployment reconciliation.
type Kubernetes struct {
	// Namespace limits which DBOSApplications are reconciled; empty means all
	// namespaces (requires cluster-scoped RBAC).
	Namespace string `json:"namespace,omitempty"`

	// ReconcileInterval is how often CRs are re-listed and Deployments
	// re-applied (default 10s).
	ReconcileInterval Duration `json:"reconcileInterval,omitempty"`
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

// LoadJWT reads the bearer token from the DBOS_CONDUCTOR_JWT environment
// variable and returns its trimmed contents. Called separately from Load so
// the JWT source can change independently of the YAML config.
func LoadJWT() (string, error) {
	tok := strings.TrimSpace(os.Getenv(JWTEnvVar))
	if tok == "" {
		return "", fmt.Errorf("%s is unset or empty", JWTEnvVar)
	}
	return tok, nil
}

func (c *Config) applyDefaults() {
	if c.Poller.Interval == 0 {
		c.Poller.Interval = Duration(30 * time.Second)
	}
	// The derived defaults below scale with the interval so that an explicit
	// interval alone always yields a valid, sensible config.
	if c.Poller.MaxBackoff == 0 {
		c.Poller.MaxBackoff = max(c.Poller.Interval, Duration(30*time.Second))
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8080"
	}
	if c.Kubernetes.ReconcileInterval == 0 {
		c.Kubernetes.ReconcileInterval = Duration(10 * time.Second)
	}
}

func (c *Config) validate() error {
	if c.Conductor.OrgName == "" {
		return errors.New("conductor.orgName is required")
	}
	if c.Poller.MaxBackoff.Native() < c.Poller.Interval.Native() {
		return errors.New("poller.maxBackoff must be >= poller.interval")
	}
	return nil
}
