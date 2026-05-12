# DBOS Kubernetes Metrics Operator

IMG          ?= controller:latest
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
deploy: ## Apply the full operator install (kustomize default overlay; pass IMG=...).
	cd config/manager && \
	  sed -i.bak "s|image: .*|image: $(IMG)|" deployment.yaml && rm deployment.yaml.bak
	kubectl apply -k config/default

.PHONY: install-yaml
install-yaml: ## Regenerate install.yaml from the kustomize default overlay.
	@{ \
	  echo "# DO NOT EDIT BY HAND."; \
	  echo "# Generated from config/ via 'make install-yaml' (or the release workflow)."; \
	  echo "# Source: kubectl kustomize config/default"; \
	  echo "# To change this file, edit the manifests under config/ and re-run the target."; \
	  echo "---"; \
	  kubectl kustomize config/default; \
	} > install.yaml
	@echo "wrote install.yaml ($$(wc -l < install.yaml) lines)"

.PHONY: undeploy
undeploy: ## Remove every resource the default overlay applied
	kubectl delete -k config/default --ignore-not-found

.PHONY: rollout-restart
rollout-restart: ## Pick up a freshly pushed image with the same tag.
	kubectl -n $(NAMESPACE) rollout restart deployment/dbos-operator
	kubectl -n $(NAMESPACE) rollout status deployment/dbos-operator