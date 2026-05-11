#!/usr/bin/env bash
# Generate a self-signed CA + serving cert for the External Metrics API server.
# Used as the cert-manager-free fallback. Writes to hack/certs/ by default.
#
# After running: create the K8s Secret with `make deploy-self-signed-cert`,
# then apply the APIService with the CA bundle inlined via
# `make deploy-self-signed-apiservice`.
set -euo pipefail

OUT="${1:-hack/certs}"
NS="${2:-dbos-operator}"
SVC="${3:-dbos-operator}"
DAYS=825

mkdir -p "$OUT"

cat > "$OUT/openssl.cnf" <<EOF
[req]
distinguished_name = req
[v3_ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
[v3_svc]
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = @alt_names
[alt_names]
DNS.1 = $SVC
DNS.2 = $SVC.$NS
DNS.3 = $SVC.$NS.svc
DNS.4 = $SVC.$NS.svc.cluster.local
EOF

# CA
openssl genrsa -out "$OUT/ca.key" 4096
openssl req -x509 -new -key "$OUT/ca.key" -out "$OUT/ca.crt" -days "$DAYS" \
  -subj "/CN=dbos-operator-ca" -extensions v3_ca -config "$OUT/openssl.cnf"

# Serving cert
openssl genrsa -out "$OUT/tls.key" 4096
openssl req -new -key "$OUT/tls.key" -out "$OUT/tls.csr" \
  -subj "/CN=$SVC.$NS.svc"
openssl x509 -req -in "$OUT/tls.csr" -CA "$OUT/ca.crt" -CAkey "$OUT/ca.key" \
  -CAcreateserial -out "$OUT/tls.crt" -days "$DAYS" \
  -extensions v3_svc -extfile "$OUT/openssl.cnf"

echo
echo "Wrote: $OUT/{ca.crt,ca.key,tls.crt,tls.key}"
