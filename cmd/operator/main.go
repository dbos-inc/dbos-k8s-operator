// Command operator reconciles DBOSApplication Deployments, polls Conductor for
// desired executor counts, and serves them over HTTP for KEDA.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/component-base/logs"
	"k8s.io/klog/v2"

	"github.com/dbos-inc/dbos-k8s-operator/internal/conductor"
	"github.com/dbos-inc/dbos-k8s-operator/internal/config"
	"github.com/dbos-inc/dbos-k8s-operator/internal/kube"
	"github.com/dbos-inc/dbos-k8s-operator/internal/metricshttp"
	"github.com/dbos-inc/dbos-k8s-operator/internal/store"
)

func main() {
	logs.InitLogs()
	defer logs.FlushLogs()

	configPath := flag.String("config", "/etc/dbos-operator/config.yaml", "path to the operator config YAML (mounted from a ConfigMap)")
	kubeconfig := flag.String("kubeconfig", "", "path to a kubeconfig; empty uses in-cluster config")
	klog.InitFlags(nil)
	flag.Parse()

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal("load config: %v", err)
	}
	apiKey, err := config.LoadAPIKey()
	if err != nil {
		fatal("load API key: %v", err)
	}

	conductorClient, err := conductor.New(conductor.Options{
		Endpoint:           cfg.Conductor.Endpoint,
		OrgName:            cfg.Conductor.OrgName,
		Token:              apiKey,
		InsecureSkipVerify: cfg.Conductor.InsecureSkipVerify,
	})
	if err != nil {
		fatal("build conductor client: %v", err)
	}

	restCfg, err := loadRESTConfig(*kubeconfig)
	if err != nil {
		fatal("load kubernetes config: %v", err)
	}
	dynClient, err := dynamic.NewForConfig(restCfg)
	if err != nil {
		fatal("build kubernetes client: %v", err)
	}

	klog.InfoS("starting dbos-operator",
		"namespace", cfg.Kubernetes.Namespace,
		"reconcileInterval", cfg.Kubernetes.ReconcileInterval.Native(),
		"pollInterval", cfg.Poller.Interval.Native(),
		"listen", cfg.HTTP.Listen,
	)

	s := store.NewInMemory()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup

	manager := kube.NewManager(kube.Options{
		Client:            dynClient,
		Conductor:         conductorClient,
		Store:             s,
		Namespace:         cfg.Kubernetes.Namespace,
		ReconcileInterval: cfg.Kubernetes.ReconcileInterval.Native(),
		PollInterval:      cfg.Poller.Interval.Native(),
	})
	wg.Add(1)
	go func() {
		defer wg.Done()
		manager.Run(ctx)
	}()

	server := &http.Server{
		Addr:              cfg.HTTP.Listen,
		Handler:           metricshttp.NewServer(s).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		klog.InfoS("metrics HTTP endpoint listening", "addr", cfg.HTTP.Listen)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			klog.ErrorS(err, "metrics HTTP endpoint exited")
			cancel()
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = server.Shutdown(shutdownCtx)
	wg.Wait()
	klog.InfoS("operator shutdown complete")
}

func loadRESTConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig == "" {
		if cfg, err := rest.InClusterConfig(); err == nil {
			return cfg, nil
		}
	}
	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		rules.ExplicitPath = kubeconfig
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
