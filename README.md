# DBOS Kubernetes Operator

The DBOS operator helps you perform fleet management for DBOS applications in Kubernetes based on DBOS queues utilization.

1. It exposes a metrics endpoint for KEDA to scale workers based on a DBOS queue utilization. This endpoint serves a desired executor count for the _latest version_ of your application.
2. It manages deployments for older versions of your workflows, also based on a DBOS queue utilization.

The DBOS Operator takes ownership of the application Deployment, which you can now configure using a `DBOSApplication` resource.

Using the DBOS operator requires a DBOS API key (self-hosted Conductor or a DBOS Teams subscription).

## How it works

At the core of the operator are **autoscaling policies**, as defined in DBOS Conductor. A DBOS application can be configured with an autoscaling policy, which lets you specificy a DBOS queue to autoscale for. The queue must be unpartitioned and have worker concurrency set.

Periodically, the operator polls DBOS Conductor to obtain a desired number of executors, per version, for the policy's queue.
The metrics endpoint servees the desired number of executor for the _latest version_ of the application.

When you run long-lived workflows and want them to keep executing on an older code base, you must maintain one deployment per workflow version.
The DBOS operator manages these deployments for you, sizing them based on a DBOS queue utilization, and within some configurable limits.

## Prerequisites

You'll need a DBOS organization name and a DBOS API key. You can find your organization name and generate an API key in the DBOS Conductor interface, or using its API. One operator serves one organization: the API key is org-scoped scoped and requires the `application.read` permission on every application you want the operator to manage.

You'll also need KEDA installed.

## Installing the operator

### Using Helm

```bash
helm install dbos-operator oci://ghcr.io/dbos-inc/charts/dbos-operator -n dbos-operator --create-namespace --set config.orgName=<your-org>
# Self-hosting Conductor? Add:
#   --set config.endpoint=http://conductor.<namespace>.svc.cluster.local:8090

kubectl -n dbos-operator create secret generic dbos-api-key --from-literal=token=<your DBOS API key>
```

The chart does not create the Secret.
To use a Secret you already manage, point the chart at it with `--set apiKey.existingSecret=<name> --set apiKey.key=<key>`.
Note the operator expects an environment variable named `DBOS_API_KEY`.

The chart also installs the DBOSApplication CRD.

### Using install.yaml
Every release attaches an install.yaml pinned to that release's image, installing into the dbos-operator namespace.

```bash
kubectl apply -f https://github.com/dbos-inc/dbos-k8s-operator/releases/latest/download/install.yaml

kubectl -n dbos-operator create secret generic dbos-api-key --from-literal=token=<your DBOS API key>

# The rendered config ships with orgName: CHANGEME. Set yours:
kubectl -n dbos-operator edit configmap dbos-operator
kubectl -n dbos-operator rollout restart deployment/dbos-operator
```

### Configuration

The runtime config is a YAML file mounted from the dbos-operator ConfigMap at /etc/dbos-operator/config.yaml, read once at startup.
All fields except conductor.orgName are optional.

```yaml
conductor:
  # Conductor organization
  orgName: local
  # When self-hosting Conductor, full base URL of Conductor's HTTP API up
  # through any cloud path prefix. Not necessary with DBOS-managed Conductor.
  endpoint: http://conductor.dbos-conductor.svc.cluster.local:8090

poller:
  interval: 10s        # autoscale poll cadence per app (default 30s = KEDA's default)
  maxBackoff: 30s     # failure backoff cap; default max(interval, 30s)

http:
  listen: ":8080"     # KEDA-facing metrics endpoint

kubernetes:
  namespace: ""       # namespace to watch; empty = all
  reconcileInterval: 10s
```

With Helm, every field maps to a `config.*` value (see `charts/dbos-operator/values.yaml`); changing one via `helm upgrade` restarts the pod automatically, with the updated configuration. If you installed from `install.yaml`, edit the `dbos-operator` ConfigMap and `kubectl -n dbos-operator rollout restart deployment/dbos-operator`.

### RBAC
The chart grants the operator a ClusterRole limited to what it manages: read `dbosapplications` CRs and write their `status`, manage `deployments` (delete only targets the old-version Deployments the operator itself created), and manage the `controllerrevisions` it uses to snapshot pod templates per app version. The role is cluster-scoped so one operator can watch every namespace; set `config.watchNamespace` to narrow it.

## Deploying an application

### DBOSApplication CR manifest
Here is an example manifest for a DBOS Custom Resource. This is very much a standard deployment, with a few specific parameters.

```yaml
  apiVersion: dbos.dev/v1alpha1
  kind: DBOSApplication
  metadata:
    name: dbos-starter-python
    namespace: dbos
  spec:
    appName: dbos-starter-python
    maxOldVersionsReplicas: 3 # <- total pod budget shared by old-version deployments
    template:
      metadata:
        labels:
          dbos.dev/starter: "true"
      spec:
        containers:
          - name: app
            image: # your image
            imagePullPolicy: Always
            ports:
              - containerPort: 3001
                protocol: TCP
            env:
              - name: DBOS_SYSTEM_DATABASE_URL
                valueFrom:
                  secretKeyRef:
                    name: dbos-app-db
                    key: python-url
              - name: DBOS_CONDUCTOR_URL
                valueFrom:
                  configMapKeyRef:
                    name: dbos-app-config
                    key: conductor-url
              - name: DBOS_CONDUCTOR_KEY # Your application's Conductor API key
                valueFrom:
                  secretKeyRef:
                    name: dbos-app-conductor-key
                    key: api-key
    # ...
```

### Fields reference
`maxOldVersionsReplicas`: how many pods, in addition to the latest deployment's own fleet, can run old versions simultaneously, across all old versions. Priority is given to later versions.

Every other `Deployment.spec` field (`strategy`, `minReadySeconds`, `progressDeadlineSeconds`, ...) is passed through verbatim to the app's main Deployment, except `replicas` (owned by the autoscaler) and `selector` (owned by the operator). Old-version drain Deployments don't inherit these fields.

### Migrating an existing Deployment
Copy the existing Deployment `spec.template` into the Custom Resource, apply, then repoint KEDA's ScaledObject (or remove your HPA.)
Applying a CR whose name matches an existing Deployment **adopts** it via server-side apply (no pod churn if the template matches.)

### Application versions
By default, the operator generates and inject a DBOS application version using the `DBOS__APPVERSION` environment variable.
Internally it uses that version to record which CR manifest should be used to run old application versions.
You can set `DBOS__APPVERSION` yourself: the operator will replace mapped version entries with the latest CR manifest.

## Pointing KEDA to the operator

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: dbos-starter-python
  namespace: dbos               # same namespace as the DBOSApplication
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: dbos-starter-python   # the Deployment the operator creates, named after the DBOSApplication
  minReplicaCount: 1
  maxReplicaCount: 10
  pollingInterval: 5            # seconds; align with the operator's poller.interval
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 30   # don't thrash on bursty queues
  triggers:
    - type: metrics-api
      metadata:
        url: "http://dbos-operator.dbos-operator.svc.cluster.local:8080/apps/dbos-starter-python/autoscale"
        valueLocation: "desiredExecutors"
        targetValue: "1"
```

## Verifying
```bash
kubectl get dbosapp
  NAME                  DESIRED   POLLED
  dbos-starter-python   3         2026-08-11T16:14:02Z

kubectl get dbosapp dbos-starter-python -n dbos -o jsonpath='{.status}' | jq .
  {
    "desiredExecutors": 0,
    "lastPolledAt": "2026-08-11T23:23:59Z",
    "noPolicy": false,
    "observedAt": 1786490639438
  }
```

`DESIRED` is Conductor's recommendation for the latest version; if it stays 0 and `status.noPolicy` is true, the app has no autoscaling policy, and the metrics endpoint return a 404 error so KEDA holds replicas.

## Under the hood

For every `DBOSApplication` resource in the cluster, the operator:

1. **Owns the Deployment.** The Custom Resource carries the app's pod template
   (`spec.template`, exactly as in a Deployment); `spec.replicas`
   is never written by the operator itself on the latest deployment.
2. **Asks Conductor for the desired executor count** periodically.
   Conductor computes it from the app's configured autoscaling policy,
   which is based on a single queue. The answer carries one entry per application version.
3. **Keeps a Deployment per old application version.** Workflows only finish on
   an executor of the version that enqueued them, so every non-latest entry in
   the response gets its own Deployment, named `<app>-<version-slug>`, pods
   labelled `dbos.dev/app-version` and pinned to that version via
   `DBOS__APPVERSION`. It is deleted only when the version leaves the response,
   which is Conductor's signal that the policy's queue has no pending work.
   These Deployments' replicas are written by the operator directly (they
   have no ScaledObject).

   Authoring `spec.maxOldVersionsReplicas` on the CR caps the old versions'
   total pod count: all old versions share that budget, split equally and
   capped at each version's recommendation, with leftovers waterfalling.
   When old versions demand more replicas than available, the operator
   prioritizes later versions first (LIFO); versions that get 0 stay parked
   until slots free up. The budget is additive to the latest deployment's
   replicas and rollout surge.
   Without `maxOldVersionsReplicas`, old versions are sized to their
   recommendation, uncapped.
4. **Serves the latest's version desired executor count over an HTTP metrics endpoint**:
   `GET /apps/<app>/autoscale` allows a KEDA ScaledObject to size the latest version's
   deployment based on queue load.

## Development

### Building from sources

```bash
IMG=<your-registry>/dbos-operator:dev
make docker-build docker-push IMG=$IMG
make deploy IMG=$IMG ORG=myorganization
```

### Run the tests
```bash
make build   # binary into bin/operator
make vet
make test
```
