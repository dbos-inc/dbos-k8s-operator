# DBOS Operator Cheatsheet

Quick reference. Scoped to the conventional install:
namespace `dbos-operator`, Deployment `dbos-operator`, ECR image
`500883621673.dkr.ecr.us-east-1.amazonaws.com/dbos-operator:dev`.

---

## Inspect / status / health

```bash
NS=dbos-operator
SEL='app.kubernetes.io/name=dbos-operator'

# --- Pod ---
kubectl -n $NS get pods                                  # status
kubectl -n $NS describe pod -l "$SEL"                    # events, restarts, mounts
kubectl -n $NS logs -l "$SEL" --tail=50                  # last 50 lines
kubectl -n $NS logs -l "$SEL" -f                         # follow live
kubectl -n $NS logs -l "$SEL" --previous                 # logs from a previous crash

# --- Deployment & rollout ---
kubectl -n $NS rollout status deployment/dbos-operator
kubectl -n $NS rollout history deployment/dbos-operator
kubectl -n $NS get deployment dbos-operator -o jsonpath='{.spec.template.spec.containers[0].image}'; echo

# --- Container env ---
kubectl -n $NS get pod -l "$SEL" -o jsonpath='{.items[0].spec.containers[0].env}'; echo
# Note: image is distroless — `kubectl exec` does not work.

# --- Operator config (the ConfigMap mounted into the pod) ---
kubectl -n $NS get cm dbos-operator -o jsonpath='{.data.config\.yaml}'; echo

# --- Secrets the pod relies on ---
kubectl -n $NS get secret conductor-jwt dbos-operator-serving-cert
kubectl -n $NS describe secret dbos-operator-serving-cert | head -10

# --- Events ---
kubectl -n $NS get events --sort-by='.lastTimestamp' | tail -20

# --- APIService (registration with kube-apiserver) ---
kubectl get apiservice v1beta1.external.metrics.k8s.io
kubectl describe apiservice v1beta1.external.metrics.k8s.io | tail -15

# --- cert-manager status (serving cert lifecycle) ---
kubectl -n $NS get certificate,issuer,certificaterequest
kubectl -n $NS describe certificate dbos-operator-serving-cert | tail -15
```

## Test the External Metrics API (what HPA sees)

```bash
# Discovery: what metrics does the API expose?
kubectl get --raw '/apis/external.metrics.k8s.io/v1beta1' | jq .

# All instances of dbos_queue_load in a namespace:
kubectl get --raw '/apis/external.metrics.k8s.io/v1beta1/namespaces/default/dbos_queue_load' | jq .

# Filter by HPA-style labelSelector:
kubectl get --raw '/apis/external.metrics.k8s.io/v1beta1/namespaces/default/dbos_queue_load?labelSelector=queue%3DtaskQueue%2Capp%3Ddbos-k8s-app' | jq .

# Health probes (HTTPS; insecure-skip-verify because the cert SAN is the in-cluster service name):
kubectl -n dbos-operator port-forward svc/dbos-operator 6443:443 &
curl -sk https://localhost:6443/livez; echo
curl -sk https://localhost:6443/readyz; echo
kill %1
```

## Build / deploy / update

```bash
# Defaults
ACCT=500883621673; REGION=us-east-1
REGISTRY=$ACCT.dkr.ecr.$REGION.amazonaws.com
IMG=$REGISTRY/dbos-operator:dev
cd /Users/max/codeZ/transact/k8s-operator

# --- ECR auth (token expires in ~12h) ---
aws ecr get-login-password --region $REGION \
  | docker login --username AWS --password-stdin $REGISTRY

# --- Build & push ---
docker build --platform linux/amd64 -t $IMG .
docker push $IMG

# --- Apply manifests (assumes cert-manager already installed) ---
kubectl apply -k config/default

# --- Pick up a freshly pushed image with the same tag ---
kubectl -n dbos-operator rollout restart deployment/dbos-operator
kubectl -n dbos-operator rollout status  deployment/dbos-operator

# --- Edit operator config ---
# Modify config/manager/configmap.yaml, then:
kubectl apply -k config/manager
kubectl -n dbos-operator rollout restart deployment/dbos-operator   # ConfigMap changes don't auto-reload

# --- Pause without uninstalling ---
kubectl -n dbos-operator scale deployment/dbos-operator --replicas=0
kubectl -n dbos-operator scale deployment/dbos-operator --replicas=1   # resume
```

## Provision the Conductor JWT Secret (required before first deploy)

```bash
# In the operator's namespace:
kubectl -n dbos-operator create secret generic conductor-jwt \
  --from-literal=token="<paste long-lived JWT here>"

# Or copy from another namespace where you already have it:
kubectl get secret conductor-jwt -n default -o json \
  | python3 -c "import json,sys; d=json.load(sys.stdin); d['metadata']={'name':'conductor-jwt','namespace':'dbos-operator'}; print(json.dumps(d))" \
  | kubectl apply -f -
```

## Tear down

```bash
# --- Remove the operator (Namespace, Deployment, RBAC, APIService, cert) ---
kubectl delete -k config/default
# Anything in the operator's Namespace is cascade-deleted by the Namespace removal.

# --- Optional: remove cert-manager too (if you installed it for this only) ---
kubectl delete -f https://github.com/cert-manager/cert-manager/releases/latest/download/cert-manager.yaml
```

## Troubleshooting

| Symptom | Most likely cause | Fix |
|---|---|---|
| Pod stuck `ContainerCreating`, `MountVolume.SetUp failed` | `conductor-jwt` or `dbos-operator-serving-cert` Secret missing | Create the JWT Secret; check cert-manager Certificate status |
| Pod CrashLoop, logs show flag help dump | Unknown flag passed in `args` | Compare `deployment.yaml` args against what the binary accepts |
| Pod CrashLoop, `read-only file system` on `apiserver.crt` | Adapter trying to write self-signed certs into a read-only Secret mount | Use `--tls-cert-file`/`--tls-private-key-file` instead of `--cert-dir` |
| `APIService AVAILABLE=False (MissingEndpoints)` | Pod isn't Ready | Wait, then check `kubectl describe pod` |
| `APIService AVAILABLE=False (FailedDiscoveryCheck)` | TLS / caBundle mismatch | Confirm cainjector populated the `caBundle`; check the cert SAN matches the Service DNS name |
| External Metrics API returns empty list | Poller hasn't successfully reached Conductor yet | Check pod logs for `queue poll failed`; verify ConfigMap `orgName` is correct |
| Conductor returns 404 "organization not found" | Placeholder orgName still in ConfigMap | Edit `config/manager/configmap.yaml`, re-apply, restart |
| Conductor returns 401 | JWT expired/invalid | Re-mint, recreate the Secret, restart the pod |

## Files of interest in this repo

```
config/manager/configmap.yaml        operator config (orgName, app/queue list, poll cadence)
config/manager/deployment.yaml       Pod spec (args, volumes, image)
config/cert-manager/                 cert-manager Issuer + Certificate
config/apiservice/apiservice.yaml    External Metrics API registration
hack/make-certs.sh                   self-signed fallback for clusters without cert-manager
```
