# DBOS Kubernetes Operator

A single-binary operator that owns each DBOS app's Deployment through a
`DBOSApplication` custom resource, polls **DBOS Conductor** for the executor
count the app's stored **autoscaling policy** implies, and serves that count
over plain HTTP for **KEDA**'s `metrics-api` scaler.

## What it does

For every `DBOSApplication` resource in the cluster, the operator:

1. **Owns the Deployment.** The CR carries the app's pod template
   (`spec.template`, exactly as in a Deployment); the operator reconciles a
   Deployment from it via server-side apply and sets an owner reference, so
   deleting the CR garbage-collects the Deployment. `spec.replicas` is never
   written — the autoscaler owns that field.
2. **Asks Conductor for the desired executor count** every tick with a single
   parameterless call (`GET .../autoscale`). Conductor computes
   it from the app's stored autoscaling policy — a queue selection (empty =
   all scalable queues) edited in the DBOS console's Fleet Control page;
   policy changes take effect within one poll interval, no operator restart.
   The answer carries one entry per application version. Conductor reports 0
   when nothing is queued — set `minReplicaCount` on the ScaledObject if you
   want a floor.
3. **Keeps a Deployment per old application version.** Workflows only finish on
   an executor of the version that enqueued them, so every non-latest entry in
   the response gets its own Deployment — named `<app>-<version-slug>`, pods
   labelled `dbos.dev/app-version` and pinned to that version via
   `DBOS__APPVERSION`, sized to that entry's `desired_executors` (at least 1,
   since work with no queue depth still needs a pod). It is deleted only when
   the version leaves the response, which is Conductor's signal that nothing of
   it is left to run — a recommendation of 0 is *not* a teardown signal. These
   Deployments have no ScaledObject, so the operator writes their
   `spec.replicas` directly.

   Authoring `spec.strategy.rollingUpdate.maxSurge` on the CR (same field,
   same int-or-percent forms as a Deployment; the whole `spec.strategy` block
   passes through to the main Deployment verbatim) additionally turns that
   surge into the old versions' total pod budget: latest replicas + maxSurge
   is the app's whole allowance, and old versions share whatever surge the
   latest Deployment's own rollout isn't consuming at that moment — split
   equally, capped at each version's recommendation, leftovers waterfalling.
   When there are more versions than budget pods, the newest versions get one
   pod each and the rest park at 0 replicas until slots free up, so old
   backlog can never crowd out the latest fleet. Without `maxSurge`, old
   versions are sized to their recommendation, uncapped.
4. **Serves the result over HTTP**: `GET /apps/<app>/queue-based-autoscaling`
   returns the latest version's entry verbatim (snake_case,
   `desired_executors`), so a KEDA ScaledObject that used to poll Conductor
   directly only changes its `url`. The latest in-memory reading is served
   however old it is; only an app with no reading at all answers 503.
5. **Reports status** on the CR (`kubectl get dbosapp` shows the desired
   count and last poll time).

## Prerequisites

- Kubernetes ≥ 1.27
- A long-lived Conductor JWT
- KEDA (for actual scaling; any metrics-api consumer works)
- A container registry the cluster can pull from

No cert-manager and no API aggregation: the metrics endpoint is plain HTTP
inside the cluster.

## Install

1. **Apply the operator bundle** (namespace, CRD, RBAC, manager):
   ```bash
   kubectl apply -k config/default
   ```

2. **Create the JWT Secret:**
   ```bash
   kubectl -n dbos-operator create secret generic conductor-jwt \
     --from-literal=token="<long-lived JWT>"
   ```

3. **Create the runtime ConfigMap.** Copy `config/manager/configmap.yaml`,
   edit `orgName` / `endpoint` / `kubernetes.namespace`, then:
   ```bash
   kubectl apply -f path/to/your/configmap.yaml
   ```

4. **Declare your apps.** One `DBOSApplication` per app (sample in
   `config/samples/dbos-starter-python.yaml`):
   ```yaml
   apiVersion: dbos.dev/v1alpha1
   kind: DBOSApplication
   metadata:
     name: my-app
     namespace: my-ns
   spec:
     appName: my-app        # Conductor app name; defaults to metadata.name
     template:              # pod template, as in Deployment.spec.template
       spec:
         containers:
           - name: app
             image: registry/my-app:tag
   ```
   Applying a CR whose name matches an existing Deployment **adopts** it via
   server-side apply — no pod churn if the template matches.

5. **Point KEDA at the operator:**
   ```yaml
   triggers:
     - type: metrics-api
       metadata:
         url: "http://dbos-operator.dbos-operator.svc.cluster.local:8080/apps/my-app/queue-based-autoscaling"
         valueLocation: "desired_executors"
         targetValue: "1"
   ```

## Configuring

The runtime config is a YAML file mounted from the `dbos-operator` ConfigMap
at `/etc/dbos-operator/config.yaml`. It is **not** part of the install bundle:

```yaml
conductor:
  # Conductor org (passed as the :org_id URL segment).
  orgName: local
  # When self-hosting Conductor, full base URL of Conductor's HTTP API up
  # through any cloud path prefix. Not necessary with DBOS-managed Conductor.
  endpoint: http://conductor.dbos-conductor.svc.cluster.local:8090

poller:
  interval: 5s        # autoscale poll cadence per app (default 30s = KEDA's default)
  maxBackoff: 30s     # failure backoff cap (exponential, ±10% jitter); default max(interval, 30s)

http:
  listen: ":8080"     # KEDA-facing metrics endpoint

kubernetes:
  namespace: ""       # namespace to watch; empty = all
  reconcileInterval: 10s
```

Apply the edited ConfigMap, then `kubectl -n dbos-operator rollout restart
deployment/dbos-operator` (config is read once at startup). Apps need no
restart — CRs are re-listed every `reconcileInterval`.

## Security

- **RBAC** (`config/rbac/operator.yaml`): read `dbosapplications`, write
  their `status`, and get/list/create/patch/delete `deployments` (delete only
  ever targets a versioned Deployment this operator owns). Nothing else.
- **Conductor JWT** comes from the `conductor-jwt` Secret via env var.
- The metrics endpoint performs no authentication (in-cluster, read-only);
  KEDA's bearer header is accepted and ignored, so an existing
  `TriggerAuthentication` can stay in place.

## Developing

```
  ┌────────────────────────── Operator pod ──────────────────────────┐
  │                                                                  │
  │  ┌─ kube manager ────────────┐    ┌─ poller (one per CR) ─────┐  │
  │  │ list DBOSApplications     │    │ GET autoscale            │  │
  │  │ SSA Deployment per CR     ├───►│ write store, patch status│  │
  │  │ start/stop pollers        │    │                          │  │
  │  └───────────────────────────┘    └───────────┬──────────────┘  │
  │                                               ▼                 │
  │                                        in-memory store          │
  │                                               ▲                 │
  │                                   ┌───────────┴─────────┐       │
  │                                   │ HTTP :8080          │ ──────┼──► KEDA
  │                                   │ /apps/<app>/...     │       │
  │                                   └─────────────────────┘       │
  └──────────────────────────────────────────────────────────────────┘
```

```
cmd/operator/main.go            load config → kube manager + HTTP server → wait for SIGTERM
internal/config/config.go       YAML loader + validation + defaults
internal/conductor/client.go    bearer-JWT REST client (QueueAutoscale)
internal/kube/manager.go        CR list loop, SSA Deployment reconcile, poller lifecycle, CR status
internal/kube/versions.go       per-old-version Deployments: apply while reported, delete once absent
internal/poller/poller.go       per-app tick: autoscale GET → store; backoff + jitter
internal/store/store.go         Store interface + in-memory impl (app → latest result)
internal/metricshttp/server.go  KEDA-facing HTTP endpoint (verbatim body)

config/crd/                     DBOSApplication CRD
config/manager/                 Namespace, ServiceAccount, ConfigMap template, Deployment, Service
config/rbac/                    ClusterRole + binding
config/samples/                 example DBOSApplication
config/default/                 kustomize root
```

**Data flow:** the kube manager is the only component talking to
`kube-apiserver`; pollers are the only writers to `store`; `metricshttp` is
the only external reader.

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
make test
```
