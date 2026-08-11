# DBOS Kubernetes Operator

The DBOS operator helps you perform fleet management for DBOS applications in Kubernetes.

It performs two main tasks:
- Expose a metrics endpoint for KEDA to scale workers based on a DBOS queue utilization.
- Manage deployments for older versions of your workflows on that queue.

To use the DBOS Operator, you also deploy your application using a DBOS CRD.

Using the DBOS operator requires a DBOS Conductor license key (self-hosted) or a DBOS Teams subscription.

## Autoscaling

At the core of the operator are autoscaling policies, as defined in DBOS Conductor. A DBOS application can be configured with an autoscaling policy, which lets you specificy a DBOS queue to autoscale for. The queue must be unpartitioned and have worker_concurrency set.

Periodically, the operator polls DBOS Conductor to obtain a desired number of executors, per version, for the policy's queue.

The operator exposes a metrics endpoint for KEDA (queue-based autoscaling), informing it about the desired number of executor for the _latest version_ of the application. You can compose your existing KEDA scaledOjbects with queue-based autoscaling.

## Long lived workflows and versioning

When you run long-lived workflows and want them to keep executing on an older code base, you must maintain one deployment per workflow version.
The DBOS operator manages these deployments for you, based on the application's autoscaling policy.

## DBOS Operator

### Installation

Helm chart

### Configuration

- APIkey secret

The runtime config is a YAML file mounted from the `dbos-operator` ConfigMap at `/etc/dbos-operator/config.yaml`.

```yaml
conductor:
  # Conductor organization
  orgName: local
  # When self-hosting Conductor, full base URL of Conductor's HTTP API up
  # through any cloud path prefix. Not necessary with DBOS-managed Conductor.
  endpoint: http://conductor.dbos-conductor.svc.cluster.local:8090

poller:
  interval: 5s        # autoscale poll cadence per app (default 30s = KEDA's default)
  maxBackoff: 30s     # failure backoff cap; default max(interval, 30s)

http:
  listen: ":8080"     # KEDA-facing metrics endpoint

kubernetes:
  namespace: ""       # namespace to watch; empty = all
  reconcileInterval: 10s
```

You can find a template under `config/manager/configmap.yaml`.
Apply the edited ConfigMap, then `kubectl -n dbos-operator rollout restart deployment/dbos-operator` (config is read once at startup).
Apps need no restart: Custom Resources are re-listed every `reconcileInterval`.

### RBAC
(`config/rbac/operator.yaml`): read `dbosapplications`, write their `status`, and get/list/create/patch/delete `deployments` (delete only ever targets a versioned Deployment this operator owns).


## DBOSApplication CRD

Here is an example manifest for a DBOS CRD. This is very much a standard deployment, with a few specific parameters.

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
              - name: SSL_CERT_FILE
                value: /certs/ca.crt
              - name: APP_BASE_PATH
                value: /python/
            readinessProbe:
              httpGet:
                path: /health
                port: 3001
              initialDelaySeconds: 15
              periodSeconds: 10
            livenessProbe:
              httpGet:
                path: /health
                port: 3001
              initialDelaySeconds: 40
              periodSeconds: 30
            resources:
              requests:
                cpu: 100m
                memory: 256Mi
              limits:
                cpu: 500m
                memory: 512Mi
            volumeMounts:
              - name: tls
                mountPath: /certs
                readOnly: true
        volumes:
          - name: tls
            secret:
              secretName: dbos-app-ca
              items:
                - key: ca.crt
                  path: ca.crt
```

`maxOldVersionsReplicas`: how many pods, in addition to the latest deployment's own fleet, can run old versions simultaneously — a single total shared by all old versions. Priority is given to later versions. You can also set `spec.strategy` (exactly as in a Deployment) to control the latest deployment's rollout; it plays no role in old-version sizing.

Applying a CR whose name matches an existing Deployment **adopts** it via server-side apply — no pod churn if the template matches.

**Version management**: by default, the operator generates and inject a DBOS application version. Internally it uses that version to map CR manifests, used when deploying older code versions. You can set `DBOS__APPVERSION` yourself, and when using the same version twice, the operator will always replace its mapping with the latest CR manifest.

## Pointing KEDA to the operator

```yaml
triggers:
  - type: metrics-api
    metadata:
      url: "http://[operator-hostname-in-cluster]:8080/apps/my-app/autoscale"
      valueLocation: "desiredExecutors"
      targetValue: "1"
```

## The operator: under the hood

For every `DBOSApplication` resource in the cluster, the operator:

1. **Owns the Deployment.** The Custom Resource carries the app's pod template
   (`spec.template`, exactly as in a Deployment); `spec.replicas`
   is never written by the operator itself.
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




## Building from sources

```bash
IMG=<your-registry>/dbos-operator:dev
make docker-build docker-push IMG=$IMG
make deploy IMG=$IMG
```

```bash
make build   # binary into bin/operator
make vet
make test
```
