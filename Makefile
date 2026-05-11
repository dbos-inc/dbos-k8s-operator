# DBOS Kubernetes Metrics Operator
#
# Single-binary, ConfigMap-driven operator. No CRD, no controller-runtime,
# no kubebuilder. Just a poller + three frontends (HPA / Prometheus / KEDA)
# behind a shared in-memory store.

IMG          ?= controller:latest
REGION       ?= us-east-1
NAMESPACE    ?= dbos-operator

.PHONY: all
all: tidy build

##@ Codegen / quality

.PHONY: tidy
tidy: ## Resolve module deps.
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test: ## Run unit tests.
	go test ./...

.PHONY: fmt
fmt:
	gofmt -s -w .

##@ Build

.PHONY: build
build: ## Compile the binary locally.
	go build -o bin/operator ./cmd/operator

.PHONY: docker-build
docker-build: ## Build the container image (pass IMG=...).
	docker build --platform linux/amd64 -t $(IMG) .

.PHONY: docker-push
docker-push: ## Push the container image (pass IMG=...).
	docker push $(IMG)

##@ Deploy

.PHONY: deploy
deploy: ## Apply the full operator install (kustomize default overlay; pass IMG=...).
	cd config/manager && \
	  sed -i.bak "s|image: .*|image: $(IMG)|" deployment.yaml && rm deployment.yaml.bak
	kubectl apply -k config/default

.PHONY: undeploy
undeploy: ## Remove every resource the default overlay applied (cascades to CRs only if a CRD exists).
	kubectl delete -k config/default --ignore-not-found

.PHONY: rollout-restart
rollout-restart: ## Pick up a freshly pushed image with the same tag.
	kubectl -n $(NAMESPACE) rollout restart deployment/dbos-operator
	kubectl -n $(NAMESPACE) rollout status deployment/dbos-operator

##@ Self-signed cert path (for clusters without cert-manager)

CERTS_DIR ?= hack/certs

.PHONY: certs
certs: ## Generate a self-signed CA + serving cert into $(CERTS_DIR).
	./hack/make-certs.sh $(CERTS_DIR) $(NAMESPACE) dbos-operator

.PHONY: deploy-self-signed-cert
deploy-self-signed-cert: ## Create the serving-cert TLS Secret from $(CERTS_DIR). Requires the namespace to already exist.
	@test -f $(CERTS_DIR)/tls.crt -a -f $(CERTS_DIR)/tls.key || { echo "Run 'make certs' first."; exit 1; }
	kubectl -n $(NAMESPACE) create secret tls dbos-operator-serving-cert \
	  --cert=$(CERTS_DIR)/tls.crt --key=$(CERTS_DIR)/tls.key \
	  --dry-run=client -o yaml | kubectl apply -f -

.PHONY: deploy-self-signed-apiservice
deploy-self-signed-apiservice: ## Apply the APIService with caBundle inlined from $(CERTS_DIR)/ca.crt.
	@test -f $(CERTS_DIR)/ca.crt || { echo "Run 'make certs' first."; exit 1; }
	@CA_B64=$$(base64 < $(CERTS_DIR)/ca.crt | tr -d '\n'); \
	  sed -e "s|caBundle: \"\"|caBundle: \"$$CA_B64\"|" \
	      -e "/cert-manager.io\/inject-ca-from/d" \
	      config/apiservice/apiservice.yaml | kubectl apply -f -
