# DBOS Kubernetes Metrics Operator

Single-binary, ConfigMap-driven operator that polls DBOS Conductor for queue
load and exposes the result to the Kubernetes Horizontal Pod Autoscaler via
the External Metrics API (`external.metrics.k8s.io/v1beta1`).

No CRD. No controller-runtime. No reconciler. Just a poller, a shared
in-memory store, and one read frontend.

## Architecture

```
   ┌────────────────────── Operator pod ─────────────────────┐
   │                                                          │
   │   ┌─ Poller goroutine(s) ─┐  writes                      │
   │   │  one per app from     │ ─────────► in-memory store   │
   │   │  the ConfigMap        │            (sync.RWMutex     │
   │   │  HTTP → Conductor     │             over map)        │
   │   └───────────────────────┘                  ▲           │
   │                                              │ reads     │
   │                                  ┌───────────┴────────┐  │
   │                                  │ External Metrics   │ ─┼──► HPA
   │                                  │ API (HTTPS, 6443)  │  │
   │                                  └────────────────────┘  │
   └──────────────────────────────────────────────────────────┘
```

The store keeps `(namespace, app, queue) → Sample` where Sample is
`{Depth, WorkerConcurrency, Load, ObservedAt}`. The frontend is read-only.
Configuration is static — the ConfigMap is loaded once at pod start; changes
take effect on restart.

The store is a clean seam: a future Prometheus exporter or KEDA gRPC scaler
can be added later as additional read frontends without touching the poller.

## Layout

```
cmd/operator/main.go              load config → spawn poller goroutines → start metrics adapter → wait for SIGTERM
internal/store/store.go           Store interface + InMemory impl
internal/config/config.go         YAML config loader + validation + defaults
internal/conductor/client.go      bearer-JWT REST client (GetQueue, QueueDepth)
internal/poller/poller.go         per-app goroutine; exponential backoff with ±10% jitter
internal/metricsadapter/provider.go   ExternalMetricsProvider over the store

config/manager/                   Namespace, ServiceAccount, ConfigMap, Deployment, Service
config/rbac/                      auth-delegator, auth-reader (aggregation API requirements);
                                  hpa-reader (lets the HPA controller call our API)
config/apiservice/                APIService registering external.metrics.k8s.io with kube-apiserver
config/cert-manager/              Issuer + Certificate (serving cert for the metrics API)
config/default/                   kustomize root that ties it all together

hack/make-certs.sh                Self-signed fallback for clusters without cert-manager

Dockerfile, Makefile, go.mod
```

## Configuration

The ConfigMap (`config/manager/configmap.yaml`) holds the operator's runtime
config — Conductor org, JWT path, app/queue list, poll cadence. The Conductor
JWT itself lives in a separate `conductor-jwt` Secret the user creates before
first deploy.

## Prerequisites

- Kubernetes ≥ 1.27
- cert-manager (one-line install:
  `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml`)
- Container registry the cluster can pull from (we use ECR in `us-east-1`)
- A long-lived Conductor JWT, stored as `conductor-jwt` Secret in the operator's namespace

See `docs/cheatsheet.md` for build, deploy, inspect, and tear-down commands.
