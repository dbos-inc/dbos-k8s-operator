# DBOS Kubernetes Metrics Operator

IMG          ?= controller:latest
ORG          ?= local
REGION       ?= us-east-1
NAMESPACE    ?= dbos-operator

.PHONY: all
all: tidy build

##@ Codegen / quality

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
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
	# Cross-compile natively on the host. The Dockerfile does COPY + ENTRYPOINT.
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o bin/operator ./cmd/operator
	docker build --platform linux/amd64 -t $(IMG) .

.PHONY: docker-push
docker-push:
	docker push $(IMG)

##@ Deploy

.PHONY: deploy
deploy: ## Install/upgrade the operator from the local chart (pass IMG=... ORG=...; extra values via HELM_ARGS=...).
	helm upgrade --install dbos-operator charts/dbos-operator \
	  -n $(NAMESPACE) --create-namespace \
	  --set image.repository=$(firstword $(subst :, ,$(IMG))) \
	  --set-string image.tag=$(lastword $(subst :, ,$(IMG))) \
	  --set config.orgName=$(ORG) \
	  $(HELM_ARGS)

.PHONY: install-yaml
install-yaml: ## Regenerate install.yaml from the chart (same rendering as the release workflow).
	@{ \
	  echo "# DO NOT EDIT BY HAND."; \
	  echo "# Generated via 'make install-yaml' (or the release workflow): helm template charts/dbos-operator."; \
	  echo "# One fixed rendering — namespace dbos-operator, orgName CHANGEME."; \
	  echo "---"; \
	  echo "apiVersion: v1"; \
	  echo "kind: Namespace"; \
	  echo "metadata:"; \
	  echo "  name: dbos-operator"; \
	  helm template dbos-operator charts/dbos-operator \
	    --namespace dbos-operator \
	    --set config.orgName=CHANGEME; \
	} > install.yaml
	@echo "wrote install.yaml ($$(wc -l < install.yaml) lines)"

.PHONY: undeploy
undeploy: ## Uninstall the operator release (the CRD and DBOSApplications are kept).
	helm uninstall dbos-operator -n $(NAMESPACE) --ignore-not-found

.PHONY: rollout-restart
rollout-restart: ## Pick up a freshly pushed image with the same tag.
	kubectl -n $(NAMESPACE) rollout restart deployment/dbos-operator
	kubectl -n $(NAMESPACE) rollout status deployment/dbos-operator