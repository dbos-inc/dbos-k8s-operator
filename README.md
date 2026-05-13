# DBOS Kubernetes Metrics Operator

A single-binary operator that polls **DBOS Conductor** for queue load and exposes the result to **Kubernetes HPA** via the External Metrics API.

## What it does

For every DBOS app you list in its ConfigMap, the operator:

1. **Discovers queues** from Conductor every poll interval. Adding or removing a queue in your app shows up automatically.
2. **Polls queue depth** (`ENQUEUED + PENDING` workflow count) for each queue that has a `worker_concurrency` set.
3. **Computes load** per queue: `load = depth / worker_concurrency`.
4. **Aggregates across queues**: for each app, the operator reports the **max** load across its queues. HPA scales the app's Deployment to match the busiest queue.
5. **Serves an aggregated External Metrics API** named `dbos_queue_load`, labelled by `app`, on HTTPS port 6443.

## Prerequisites

- Kubernetes ≥ 1.27
- cert-manager (one-line install: `kubectl apply -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml`)
- A long-lived Conductor JWT
- A container registry the cluster can pull from

## Install

The operator image is published to [GHCR](ghcr.io/dbos-inc/dbos-k8s-operator).

Two pieces of state are user-provided and **not** shipped in the install bundle: the Conductor JWT (a Secret) and the operator's runtime config (a ConfigMap). The Deployment references both by name and will keep its pod in `ContainerCreating` until they exist — applying out of order is safe, just visibly pending.

1. **Apply the operator manifests.** This creates the `dbos-operator` namespace along with the rest of the bundle.
   ```bash
   kubectl apply -f https://github.com/dbos-inc/dbos-k8s-operator/releases/download/[version]/install.yaml
   ```

2. **Create the JWT Secret:**
   ```bash
   kubectl -n dbos-operator create secret generic conductor-jwt \
     --from-literal=token="<long-lived JWT>"
   ```

3. **Create the runtime ConfigMap.** Copy `config/manager/configmap.yaml`, edit the `orgName`, `endpoint`, and `apps[]` fields for your environment (schema in [Configuring](#configuring)), then:
   ```bash
   kubectl apply -f path/to/your/configmap.yaml
   ```

See `docs/cheatsheet.md` for inspection, rollout, and teardown commands.

## Configuring

The operator's runtime config is a YAML file mounted from a ConfigMap named `dbos-operator` at `/etc/dbos-operator/config.yaml`. The ConfigMap is **not** included in install.yaml or in the `make deploy` bundle. Use `config/manager/configmap.yaml` as a template:

```yaml
conductor:
  # Conductor org name. "local" for self hosted without OAuth.
  orgName: local

  # When self-hosting Conductor, full base URL of Conductor's HTTP API up through any cloud path prefix.
  # Not necessary when using DBOS-managed Conductor
  endpoint: http://conductor.dbos-conductor.svc.cluster.local:8090

poller:
  interval: 1s
  maxBackoff: 30s

apps:
  # One entry per DBOS app to autoscale.
  - name: dbos-k8s-app

metricsAPI:
  enabled: true
```

Apply the edited ConfigMap, then restart the operator to pick it up (the config is read once at startup):

```bash
kubectl apply -f path/to/your/configmap.yaml
kubectl -n dbos-operator rollout restart deployment/dbos-operator
```

The TLS port and cert paths are container args in `config/manager/deployment.yaml` (`--secure-port`, `--tls-cert-file`, `--tls-private-key-file`), not part of the YAML above.

## Security

### TLS certificate

The operator serves the External Metrics API over HTTPS and is registered as an aggregated APIServer, so `kube-apiserver` must trust the cert it presents. Out of the box, `config/cert-manager/` ships a namespace-scoped `Issuer` of type `selfSigned` (`dbos-operator-selfsigned`) and a `Certificate` (`dbos-operator-serving-cert`). The `cert-manager.io/inject-ca-from` annotation on the `APIService` lets cert-manager's `cainjector` populate `spec.caBundle` automatically. No user input needed beyond installing cert-manager.

Alternatives if the self-signed default isn't acceptable:

- **Different cert-manager Issuer** (Vault, ACME, internal CA): edit `certificate.yaml`'s `issuerRef`, or replace `issuer.yaml` with your own `Issuer`/`ClusterIssuer`.
- **Bring-your-own Secret**: drop `config/cert-manager/` and create the `dbos-operator-serving-cert` Secret yourself (with `tls.crt`, `tls.key`, `ca.crt`). The `inject-ca-from` annotation will still pick up `ca.crt`.

### ServiceAccount

The pod runs as the `dbos-operator` ServiceAccount (`config/manager/serviceaccount.yaml`). It is the in-cluster identity used to call back into `kube-apiserver` for delegated authentication and authorization on every incoming HPA request.

### RBAC

The three bindings in `config/rbac/` are the standard aggregated-APIServer set:

- **`auth_delegator.yaml`** — binds the SA to `system:auth-delegator`, granting `tokenreviews` and `subjectaccessreviews`. Lets the operator ask `kube-apiserver` "is this caller authenticated, and may they read external metrics?" on each incoming request.
- **`auth_reader.yaml`** — `RoleBinding` in `kube-system` granting read on the `extension-apiserver-authentication` ConfigMap, which holds the front-proxy CA bundle. The operator needs this at startup to know which client certs (presented by `kube-apiserver` when proxying) to trust.
- **`hpa_reader.yaml`** — the reverse direction: a `ClusterRole` + `ClusterRoleBinding` that lets the HPA controller's own ServiceAccount (`system:horizontal-pod-autoscaler` in `kube-system`) call our `external.metrics.k8s.io` endpoints. Without it, every HPA poll returns 403.

The operator does not need any other cluster permissions: it neither lists nor mutates application workloads.

## Wiring up HPA

Reference `dbos_queue_load` as an `External` metric and select your app by the `app` label.

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: dbos-k8s-app
  namespace: [namespace]
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: dbos-k8s-app
  minReplicas: 1
  maxReplicas: 50
  metrics:
    - type: External
      external:
        metric:
          name: dbos_queue_load
          selector:
            matchLabels:
              app: dbos-k8s-app          # = the `apps[].name` in the operator's ConfigMap
        target:
          type: AverageValue
          averageValue: "1"
```

## Developing

```
   ┌────────────────────── Operator pod ─────────────────────┐
   │                                                          │
   │   ┌─ Poller goroutine ────┐  writes                      │
   │   │  one per app from     │ ─────────► in-memory store   │
   │   │  the ConfigMap        │            (app,queue)→load  │
   │   │  HTTP → Conductor     │                              │
   │   └───────────────────────┘                  ▲           │
   │                                              │ reads     │
   │                                  ┌───────────┴────────┐  │
   │                                  │ External Metrics   │ ─┼──► HPA
   │                                  │ API (HTTPS, 6443)  │  │
   │                                  └────────────────────┘  │
   └──────────────────────────────────────────────────────────┘
```

```
cmd/operator/main.go                  load config → spawn one poller per app → start the metrics adapter → wait for SIGTERM
internal/config/config.go             YAML loader + validation + defaults
internal/conductor/client.go          bearer-JWT REST client for Conductor (ListQueues, QueueDepth)
internal/poller/poller.go             per-app goroutine; ticker with exponential backoff + ±10% jitter
internal/store/store.go               Store interface + in-memory impl (sync.RWMutex over map)
internal/metricsadapter/provider.go   ExternalMetricsProvider that reads the store and aggregates per app

config/manager/                       Namespace, ServiceAccount, ConfigMap, Deployment, Service
config/rbac/                          auth-delegator + auth-reader (aggregation-API requirements);
                                      hpa-reader (lets the HPA controller call our API)
config/apiservice/                    APIService registering external.metrics.k8s.io with kube-apiserver
config/cert-manager/                  Issuer + Certificate for the metrics API serving cert
config/default/                       kustomize root that ties it all together

Dockerfile, Makefile, go.mod
docs/cheatsheet.md                    inspection / build / teardown commands
```

**Data flow:** `poller` is the only writer to `store`. `metricsadapter` is
the only reader exposed externally. The `store` interface is the seam — a
Prometheus exporter or a KEDA gRPC scaler can be added later as additional
read frontends without touching the poller.

**From source:**

```bash
IMG=<your-registry>/dbos-operator:dev
make docker-build docker-push IMG=$IMG
make deploy IMG=$IMG
```

**Local build & checks:**

```bash
make build   # binary into bin/operator
make vet
make fmt
```

**Iterating on a live cluster:** edit code, then

```bash
make docker-build docker-push IMG=$IMG
make rollout-restart           # imagePullPolicy: Always re-pulls :dev
```
