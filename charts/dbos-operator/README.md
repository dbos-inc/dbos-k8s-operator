# dbos-operator

Helm chart for the DBOS Kubernetes operator: watches `DBOSApplication`
resources, manages their Deployments (including old-version drain fleets),
polls Conductor for autoscale recommendations, and serves them to KEDA/HPA
over a plain-HTTP metrics endpoint.

## Install

```bash
helm install dbos-operator oci://ghcr.io/dbos-inc/charts/dbos-operator \
  -n dbos-operator --create-namespace \
  --set config.orgName=<your-org>

kubectl -n dbos-operator create secret generic dbos-api-key \
  --from-literal=token=<your DBOS API key>
```

KEDA is not part of this chart — install it once per cluster from the
upstream release manifest, then point a `ScaledObject` metrics-api trigger at
`http://dbos-operator.dbos-operator.svc.cluster.local:8080/apps/<app>/autoscale`
(valueLocation `desiredExecutors`).

## Values

| Key | Default | Description |
|---|---|---|
| `config.orgName` | — (required) | Conductor organization name |
| `config.endpoint` | `""` | Conductor API base URL; empty derives `https://<dbosDomain>/conductor/v1alpha1` |
| `config.insecureSkipVerify` | `false` | Skip TLS verification of the Conductor endpoint (dev only) |
| `config.pollerInterval` | `30s` | Per-app autoscale poll cadence |
| `config.watchNamespace` | `""` | Namespace to watch; empty = all |
| `config.reconcileInterval` | `10s` | CR re-list / Deployment re-apply cadence |
| `dbosDomain` | `cloud.dbos.dev` | Used only when `config.endpoint` is empty |
| `apiKey.existingSecret` | `dbos-api-key` | Secret holding the DBOS API key (org-scoped Conductor credential) |
| `apiKey.key` | `token` | Key within that secret |
| `image.repository` | `ghcr.io/dbos-inc/dbos-k8s-operator` | Operator image |
| `image.tag` | chart `appVersion` | Image tag override |
| `crds.install` | `true` | Manage the DBOSApplication CRD with the chart (kept on uninstall) |
| `replicas` | `1` | Operator replicas |
| `verbosity` | `2` | klog verbosity |
| `service.port` | `8080` | Metrics Service port |
| `resources` | small | Operator container resources |

Standard `nameOverride`, `fullnameOverride`, `serviceAccount`, `nodeSelector`,
`tolerations`, and `affinity` values are also supported.

## CRD upgrades

The CRD is templated into the chart (not a `crds/` directory), so `helm
upgrade` upgrades it. It carries `helm.sh/resource-policy: keep`: uninstalling
the release leaves the CRD and all DBOSApplications in place. To adopt a CRD
installed out of band (e.g. by `install.yaml`), either delete it first or
install with `--set crds.install=false`.
