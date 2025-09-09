# VexDB Makefile
# This Makefile provides convenient commands for building, testing, and deploying VexDB

# Variables
GO_VERSION := 1.21
DOCKER_REGISTRY := ghcr.io
IMAGE_NAME := vexdb
NAMESPACE := vexdb
KUBECONFIG := ~/.kube/config

# Go variables
GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOPATH ?= $(shell go env GOPATH)
GOCACHE := $(GOPATH)/cache

# Docker variables
DOCKER_BUILDKIT := 1
export DOCKER_BUILDKIT

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
PURPLE := \033[0;35m
CYAN := \033[0;36m
WHITE := \033[0;37m
NC := \033[0m # No Color

# Help target
.PHONY: help
help: ## Show this help message
	@echo "$(CYAN)VexDB Development Commands$(NC)"
	@echo ""
	@echo "$(GREEN)Development:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "$(YELLOW)%-20s$(NC) %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*?##/ { printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Building:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "$(YELLOW)%-20s$(NC) %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*?##/ { if ($$2 ~ /build/) printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Testing:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "$(YELLOW)%-20s$(NC) %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*?##/ { if ($$2 ~ /test/) printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Docker:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "$(YELLOW)%-20s$(NC) %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*?##/ { if ($$2 ~ /docker/) printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Kubernetes:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "$(YELLOW)%-20s$(NC) %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*?##/ { if ($$2 ~ /k8s/) printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "$(GREEN)Deployment:$(NC)"
	@awk 'BEGIN {FS = ":.*##"; printf "$(YELLOW)%-20s$(NC) %s\n", "Target", "Description"} /^[a-zA-Z_-]+:.*?##/ { if ($$2 ~ /deploy/) printf "$(YELLOW)%-20s$(NC) %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Development targets
.PHONY: install
install: ## Install Go dependencies
	@echo "$(BLUE)Installing Go dependencies...$(NC)"
	go mod download
	go mod tidy

.PHONY: generate
generate: ## Generate code from proto files
	@echo "$(BLUE)Generating code from proto files...$(NC)"
	protoc --go_out=. --go-grpc_out=. proto/vexdb.proto

.PHONY: format
format: ## Format Go code
	@echo "$(BLUE)Formatting Go code...$(NC)"
	go fmt ./...
	goimports -w .

.PHONY: lint
lint: ## Run linter
	@echo "$(BLUE)Running linter...$(NC)"
	golangci-lint run

.PHONY: vet
vet: ## Run go vet
	@echo "$(BLUE)Running go vet...$(NC)"
	go vet ./...

.PHONY: security-scan
security-scan: ## Run security scan
	@echo "$(BLUE)Running security scan...$(NC)"
	gosec ./...

# Building targets
.PHONY: build
build: ## Build all services
	@echo "$(BLUE)Building all services...$(NC)"
	$(MAKE) build-vexinsert
	$(MAKE) build-vexstorage
	$(MAKE) build-vexsearch

.PHONY: build-vexinsert
build-vexinsert: ## Build vexinsert service
	@echo "$(BLUE)Building vexinsert service...$(NC)"
	go build -o bin/vexinsert ./cmd/vexinsert

.PHONY: build-vexstorage
build-vexstorage: ## Build vexstorage service
	@echo "$(BLUE)Building vexstorage service...$(NC)"
	go build -o bin/vexstorage ./cmd/vexstorage

.PHONY: build-vexsearch
build-vexsearch: ## Build vexsearch service
	@echo "$(BLUE)Building vexsearch service...$(NC)"
	go build -o bin/vexsearch ./cmd/vexsearch

.PHONY: build-all-platforms
build-all-platforms: ## Build all services for all platforms
	@echo "$(BLUE)Building all services for all platforms...$(NC)"
	@for service in vexinsert vexstorage vexsearch; do \
		for os in linux darwin windows; do \
			for arch in amd64 arm64; do \
				if [ "$$os" == "windows" ]; then \
					ext=".exe"; \
				else \
					ext=""; \
				fi; \
				echo "$(BLUE)Building $$service for $$os/$$arch...$(NC)"; \
				GOOS=$$os GOARCH=$$arch go build -o bin/$$service-$$os-$$arch$$ext ./cmd/$$service; \
			done; \
		done; \
	done

# Testing targets
.PHONY: test
test: ## Run all tests
	@echo "$(BLUE)Running all tests...$(NC)"
	$(MAKE) test-unit
	$(MAKE) test-integration

.PHONY: test-unit
test-unit: ## Run unit tests
	@echo "$(BLUE)Running unit tests...$(NC)"
	go test -v -race -coverprofile=coverage.out -covermode=atomic ./...

.PHONY: test-integration
test-integration: ## Run integration tests
	@echo "$(BLUE)Running integration tests...$(NC)"
	go test -v -tags=integration ./internal/search/integration/...
	go test -v -tags=integration ./internal/protocol/...

.PHONY: test-coverage
test-coverage: ## Run tests with coverage
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	go test -v -race -coverprofile=coverage.out -covermode=atomic -coverpkg=./... ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "$(GREEN)Coverage report generated: coverage.html$(NC)"

.PHONY: test-benchmark
test-benchmark: ## Run benchmark tests
	@echo "$(BLUE)Running benchmark tests...$(NC)"
	go test -v -bench=. -benchmem -run=^$ ./internal/search/benchmark/...

# Docker targets
.PHONY: docker-build
docker-build: ## Build Docker images
	@echo "$(BLUE)Building Docker images...$(NC)"
	$(MAKE) docker-build-vexinsert
	$(MAKE) docker-build-vexstorage
	$(MAKE) docker-build-vexsearch

.PHONY: docker-build-vexinsert
docker-build-vexinsert: ## Build vexinsert Docker image
	@echo "$(BLUE)Building vexinsert Docker image...$(NC)"
	docker build -t $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexinsert:latest -f cmd/vexinsert/Dockerfile .

.PHONY: docker-build-vexstorage
docker-build-vexstorage: ## Build vexstorage Docker image
	@echo "$(BLUE)Building vexstorage Docker image...$(NC)"
	docker build -t $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexstorage:latest -f cmd/vexstorage/Dockerfile .

.PHONY: docker-build-vexsearch
docker-build-vexsearch: ## Build vexsearch Docker image
	@echo "$(BLUE)Building vexsearch Docker image...$(NC)"
	docker build -t $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexsearch:latest -f cmd/vexsearch/Dockerfile .

.PHONY: docker-push
docker-push: ## Push Docker images
	@echo "$(BLUE)Pushing Docker images...$(NC)"
	$(MAKE) docker-push-vexinsert
	$(MAKE) docker-push-vexstorage
	$(MAKE) docker-push-vexsearch

.PHONY: docker-push-vexinsert
docker-push-vexinsert: ## Push vexinsert Docker image
	@echo "$(BLUE)Pushing vexinsert Docker image...$(NC)"
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexinsert:latest

.PHONY: docker-push-vexstorage
docker-push-vexstorage: ## Push vexstorage Docker image
	@echo "$(BLUE)Pushing vexstorage Docker image...$(NC)"
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexstorage:latest

.PHONY: docker-push-vexsearch
docker-push-vexsearch: ## Push vexsearch Docker image
	@echo "$(BLUE)Pushing vexsearch Docker image...$(NC)"
	docker push $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexsearch:latest

.PHONY: docker-compose-up
docker-compose-up: ## Start services with Docker Compose
	@echo "$(BLUE)Starting services with Docker Compose...$(NC)"
	docker-compose up -d

.PHONY: docker-compose-down
docker-compose-down: ## Stop services with Docker Compose
	@echo "$(BLUE)Stopping services with Docker Compose...$(NC)"
	docker-compose down

.PHONY: docker-compose-logs
docker-compose-logs: ## Show Docker Compose logs
	@echo "$(BLUE)Showing Docker Compose logs...$(NC)"
	docker-compose logs -f

# Kubernetes targets
.PHONY: k8s-deploy
k8s-deploy: ## Deploy to Kubernetes
	@echo "$(BLUE)Deploying to Kubernetes...$(NC)"
	kubectl apply -f kubernetes/vexdb-deployment.yaml

.PHONY: k8s-delete
k8s-delete: ## Delete from Kubernetes
	@echo "$(BLUE)Deleting from Kubernetes...$(NC)"
	kubectl delete -f kubernetes/vexdb-deployment.yaml

.PHONY: k8s-logs
k8s-logs: ## Show Kubernetes logs
	@echo "$(BLUE)Showing Kubernetes logs...$(NC)"
	kubectl logs -f -n $(NAMESPACE) deployment/vexinsert-deployment

.PHONY: k8s-port-forward
k8s-port-forward: ## Port forward Kubernetes services
	@echo "$(BLUE)Port forwarding Kubernetes services...$(NC)"
	kubectl port-forward -n $(NAMESPACE) service/vexinsert-service 8080:8080 &
	kubectl port-forward -n $(NAMESPACE) service/vexstorage-service 8082:8082 &
	kubectl port-forward -n $(NAMESPACE) service/vexsearch-service 8083:8083 &

.PHONY: k8s-status
k8s-status: ## Show Kubernetes status
	@echo "$(BLUE)Showing Kubernetes status...$(NC)"
	kubectl get all -n $(NAMESPACE)

# Deployment targets
.PHONY: deploy-staging
deploy-staging: ## Deploy to staging
	@echo "$(BLUE)Deploying to staging...$(NC)"
	kubectl config use-context staging
	$(MAKE) k8s-deploy

.PHONY: deploy-production
deploy-production: ## Deploy to production
	@echo "$(BLUE)Deploying to production...$(NC)"
	kubectl config use-context production
	$(MAKE) k8s-deploy

# Clean targets
.PHONY: clean
clean: ## Clean build artifacts
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -rf bin/
	rm -rf coverage.out coverage.html
	go clean -cache

.PHONY: clean-docker
clean-docker: ## Clean Docker images
	@echo "$(BLUE)Cleaning Docker images...$(NC)"
	docker rmi $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexinsert:latest || true
	docker rmi $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexstorage:latest || true
	docker rmi $(DOCKER_REGISTRY)/$(IMAGE_NAME)-vexsearch:latest || true

.PHONY: clean-k8s
clean-k8s: ## Clean Kubernetes resources
	@echo "$(BLUE)Cleaning Kubernetes resources...$(NC)"
	$(MAKE) k8s-delete

.PHONY: clean-all
clean-all: ## Clean all artifacts
	@echo "$(BLUE)Cleaning all artifacts...$(NC)"
	$(MAKE) clean
	$(MAKE) clean-docker
	$(MAKE) clean-k8s

# Development environment targets
.PHONY: dev-up
dev-up: ## Start development environment
	@echo "$(BLUE)Starting development environment...$(NC)"
	$(MAKE) docker-compose-up
	@echo "$(GREEN)Development environment started!$(NC)"
	@echo "$(YELLOW)VexInsert API: http://localhost:8080$(NC)"
	@echo "$(YELLOW)VexStorage API: http://localhost:8082$(NC)"
	@echo "$(YELLOW)VexSearch API: http://localhost:8083$(NC)"
	@echo "$(YELLOW)Metrics: http://localhost:9090$(NC)"
	@echo "$(YELLOW)Grafana: http://localhost:3000$(NC)"
	@echo "$(YELLOW)Jaeger: http://localhost:16686$(NC)"

.PHONY: dev-down
dev-down: ## Stop development environment
	@echo "$(BLUE)Stopping development environment...$(NC)"
	$(MAKE) docker-compose-down

.PHONY: dev-logs
dev-logs: ## Show development environment logs
	@echo "$(BLUE)Showing development environment logs...$(NC)"
	$(MAKE) docker-compose-logs

# Monitoring targets
.PHONY: monitor-up
monitor-up: ## Start monitoring stack
	@echo "$(BLUE)Starting monitoring stack...$(NC)"
	docker-compose up -d prometheus grafana jaeger

.PHONY: monitor-down
monitor-down: ## Stop monitoring stack
	@echo "$(BLUE)Stopping monitoring stack...$(NC)"
	docker-compose stop prometheus grafana jaeger

# Documentation targets
.PHONY: docs
docs: ## Generate documentation
	@echo "$(BLUE)Generating documentation...$(NC)"
	go install golang.org/x/tools/cmd/godoc@latest
	godoc -html ./internal > docs/api.html
	godoc -html ./cmd > docs/services.html
	@echo "$(GREEN)Documentation generated in docs/ directory$(NC)"

# Release targets
.PHONY: release
release: ## Create a new release
	@echo "$(BLUE)Creating a new release...$(NC)"
	@read -p "Enter release version (e.g., v1.0.0): " version; \
		git tag -a $$version -m "Release $$version"; \
		git push origin $$version; \
		$(MAKE) build-all-platforms; \
		$(MAKE) docker-build; \
		$(MAKE) docker-push

# Utility targets
.PHONY: version
version: ## Show version information
	@echo "$(BLUE)VexDB Version Information$(NC)"
	@echo "$(YELLOW)Go Version:$(NC) $(shell go version)"
	@echo "$(YELLOW)Git Commit:$(NC) $(shell git rev-parse HEAD)"
	@echo "$(YELLOW)Git Branch:$(NC) $(shell git rev-parse --abbrev-ref HEAD)"
	@echo "$(YELLOW)Build Date:$(NC) $(shell date -u +'%Y-%m-%dT%H:%M:%SZ')"

.PHONY: check-deps
check-deps: ## Check dependencies
	@echo "$(BLUE)Checking dependencies...$(NC)"
	@which go > /dev/null || (echo "$(RED)Go is not installed$(NC)" && exit 1)
	@which docker > /dev/null || (echo "$(RED)Docker is not installed$(NC)" && exit 1)
	@which kubectl > /dev/null || (echo "$(RED)kubectl is not installed$(NC)" && exit 1)
	@echo "$(GREEN)All dependencies are installed$(NC)"

# Default target
.DEFAULT_GOAL := help