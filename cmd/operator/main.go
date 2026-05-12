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
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"
	basecmd "sigs.k8s.io/custom-metrics-apiserver/pkg/cmd"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/config"
	"github.com/dbos-inc/dbos-k8s-operator/internal/deployment"
	"github.com/dbos-inc/dbos-k8s-operator/internal/metricsadapter"
	"github.com/dbos-inc/dbos-k8s-operator/internal/poller"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
	"github.com/dbos-inc/dbos-k8s-operator/internal/versionmanager"
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

	// k8s clientset for the deployment watchers. Built lazily so the operator
	// still works in environments without K8s API access (e.g. dev loops).
	// Built once and shared across all watchers.
	var k8sClient kubernetes.Interface
	if anyAppHasDeployment(cfg.Apps) {
		k8sClient, err = newKubeClient()
		if err != nil {
			fatal("build kubernetes client: %v", err)
		}
	}

	// Shared Conductor client for the deployment watchers' metadata writes.
	// The pollers build their own clients internally — leaving that alone for
	// now to minimize blast radius on the poller path.
	var condClient *conductor.Client
	if k8sClient != nil {
		condClient, err = conductor.New(conductor.Options{
			Endpoint:           cfg.Conductor.Endpoint,
			OrgName:            cfg.Conductor.OrgName,
			Token:              jwt,
			InsecureSkipVerify: cfg.Conductor.InsecureSkipVerify,
		})
		if err != nil {
			fatal("build conductor client: %v", err)
		}
	}

	var wg sync.WaitGroup

	// One poller goroutine per configured app. Each goroutine runs both
	// the queue-metric tick (always on) and, if enabled, the version-
	// manager tick.
	var versionMgrInterval time.Duration
	if cfg.VersionManager.Enabled {
		versionMgrInterval = cfg.VersionManager.Interval.Native()
	}
	for _, app := range cfg.Apps {
		var vm *versionmanager.Manager
		if cfg.VersionManager.Enabled && cfg.VersionManager.CreateArchives && app.Namespace != "" && k8sClient != nil {
			vm = versionmanager.New(k8sClient, app.Namespace, app.Name)
		}
		pcfg := poller.Config{
			AppName:                app.Name,
			Queues:                 app.Queues,
			OrgName:                cfg.Conductor.OrgName,
			Endpoint:               cfg.Conductor.Endpoint,
			Token:                  jwt,
			InsecureTLS:            cfg.Conductor.InsecureSkipVerify,
			Interval:               cfg.Poller.Interval.Native(),
			MaxBackoff:             cfg.Poller.MaxBackoff.Native(),
			VersionManagerInterval: versionMgrInterval,
			VersionMgr:             vm,
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			poller.Run(ctx, pcfg, s)
		}()
	}

	// One deployment watcher goroutine per configured app that opted in
	// (i.e. specified a namespace). app.Name doubles as the K8s Deployment
	// name — the operator enforces that the DBOS app name and the
	// Deployment name match.
	for _, app := range cfg.Apps {
		if app.Namespace == "" {
			continue
		}
		w := deployment.New(deployment.Config{
			AppName:      app.Name,
			Namespace:    app.Namespace,
			Name:         app.Name,
			ResyncPeriod: 5 * time.Minute,
		}, k8sClient, condClient)
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.Run(ctx)
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

// anyAppHasDeployment reports whether at least one app is configured with a
// namespace. Used to skip k8s-client construction when no watcher would run.
func anyAppHasDeployment(apps []config.App) bool {
	for _, a := range apps {
		if a.Namespace != "" {
			return true
		}
	}
	return false
}

// newKubeClient builds a clientset using the in-cluster config if available,
// otherwise falling back to KUBECONFIG. Useful for local runs.
func newKubeClient() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		// Fall back to KUBECONFIG / default kubeconfig for local development.
		cfg, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
		if err != nil {
			return nil, fmt.Errorf("no in-cluster config and no kubeconfig: %w", err)
		}
	}
	return kubernetes.NewForConfig(cfg)
}
