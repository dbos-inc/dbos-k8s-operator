// Command operator runs the DBOS metrics operator: a single binary that polls
// Conductor on a configured cadence and exposes per-queue load to HPA via the
// External Metrics API. There is no controller-runtime, no CRD, and no leader
// election — all configuration is static and loaded from a ConfigMap-mounted
// YAML file.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	basecmd "sigs.k8s.io/custom-metrics-apiserver/pkg/cmd"

	"github.com/dbos-inc/dbos-k8s-operator/internal/config"
	"github.com/dbos-inc/dbos-k8s-operator/internal/metricsadapter"
	"github.com/dbos-inc/dbos-k8s-operator/internal/poller"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

// adapter wires our External Metrics provider onto AdapterBase. AdapterBase
// owns the apiserver, TLS termination, and aggregated-API delegation.
type adapter struct {
	basecmd.AdapterBase
}

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	configPath := flag.String("config", "/etc/dbos-operator/config.yaml",
		"path to the operator config YAML (mounted from a ConfigMap)")

	// klog registers its own flags (-v, -log_dir, ...) onto flag.CommandLine so
	// AddGoFlagSet below exposes them as pflag long-form (--v=2) for the Adapter.
	klog.InitFlags(nil)

	// AdapterBase registers its own flag set. We share os.Args with it so its
	// --secure-port / --tls-cert-file / --tls-private-key-file / klog flags
	// are picked up too.
	a := &adapter{}
	a.Flags().AddGoFlagSet(flag.CommandLine)
	if err := a.Flags().Parse(os.Args[1:]); err != nil {
		fatal("parse flags: %v", err)
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal("load config: %v", err)
	}
	jwt, err := config.LoadJWT(cfg.Conductor.JWTPath)
	if err != nil {
		fatal("load jwt: %v", err)
	}

	klog.InfoS("starting dbos-operator",
		"apps", len(cfg.Apps),
		"interval", cfg.Poller.Interval.Native(),
		"maxBackoff", cfg.Poller.MaxBackoff.Native(),
		"metricsAPI", cfg.MetricsAPI.Enabled,
	)

	s := store.NewInMemory()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	// One poller goroutine per configured app.
	for _, app := range cfg.Apps {
		pcfg := poller.Config{
			AppName:     app.Name,
			Queues:      app.Queues,
			OrgName:     cfg.Conductor.OrgName,
			Endpoint:    cfg.Conductor.Endpoint,
			Token:       jwt,
			InsecureTLS: cfg.Conductor.InsecureSkipVerify,
			Interval:    cfg.Poller.Interval.Native(),
			MaxBackoff:  cfg.Poller.MaxBackoff.Native(),
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			poller.Run(ctx, pcfg, s)
		}()
	}

	// External Metrics API server (HTTPS, aggregated). Blocks until ctx is cancelled.
	if cfg.MetricsAPI.Enabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.WithExternalMetrics(metricsadapter.New(s))
			klog.InfoS("starting external metrics adapter")
			if err := a.Run(ctx); err != nil {
				klog.ErrorS(err, "external metrics adapter exited")
			}
		}()
	} else {
		klog.InfoS("external metrics adapter disabled by config")
	}

	wg.Wait()
	klog.InfoS("operator shutdown complete")
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
