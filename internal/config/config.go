// Package config loads and validates the operator's static YAML configuration.
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

// Duration wraps time.Duration so values like "30s" decode correctly.
type Duration time.Duration

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

type Conductor struct {
	// Full base URL; empty derives https://${DBOS_DOMAIN}/conductor/v1alpha1.
	Endpoint string `json:"endpoint,omitempty"`

	OrgName string `json:"orgName"`

	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"` // dev only
}

const APIKeyEnvVar = "DBOS_API_KEY"

type Poller struct {
	Interval Duration `json:"interval,omitempty"` // default 30s, matching KEDA's
}

type HTTP struct {
	Listen string `json:"listen,omitempty"` // default ":8080"
}

type Kubernetes struct {
	// Empty means all namespaces (requires cluster-scoped RBAC).
	Namespace string `json:"namespace,omitempty"`

	ReconcileInterval Duration `json:"reconcileInterval,omitempty"` // default 10s
}

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

func LoadAPIKey() (string, error) {
	tok := strings.TrimSpace(os.Getenv(APIKeyEnvVar))
	if tok == "" {
		return "", fmt.Errorf("%s is unset or empty", APIKeyEnvVar)
	}
	return tok, nil
}

func (c *Config) applyDefaults() {
	if c.Poller.Interval == 0 {
		c.Poller.Interval = Duration(30 * time.Second)
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
	if c.Poller.Interval.Native() <= 0 {
		return errors.New("poller.interval must be positive")
	}
	if c.Kubernetes.ReconcileInterval.Native() <= 0 {
		return errors.New("kubernetes.reconcileInterval must be positive")
	}
	return nil
}
