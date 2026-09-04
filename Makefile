SHELL := /bin/sh
COMPOSE := docker compose
TOOLS_DIR := $(CURDIR)/bin
GOLANGCI_LINT := $(TOOLS_DIR)/golangci-lint
GOLANGCI_LINT_VERSION := v2.13.2
GO_ENV := GOCACHE=$(TOOLS_DIR)/go-build-cache
LINT_ENV := $(GO_ENV) GOLANGCI_LINT_CACHE=$(TOOLS_DIR)/golangci-cache

.PHONY: help deps-test up down restart logs ps test test-unit test-integration test-functional-integration run-admin install-lint fmt fmt-check lint lint-fix check tidy

help:
	@echo "Targets:"
	@echo "  deps-test         Install integration test dependencies"
	@echo "  up                Start local PostgreSQL"
	@echo "  down              Stop local dependencies"
	@echo "  restart           Restart local dependencies"
	@echo "  logs              Tail dependency logs"
	@echo "  ps                Show dependency status"
	@echo "  test              Run unit tests"
	@echo "  test-unit         Run unit tests"
	@echo "  test-integration  Run integration tests (requires Docker)"
	@echo "  test-functional-integration  Run PostgreSQL functional suites"
	@echo "  run-admin         Run operational CLI/web server"
	@echo "  install-lint      Install the pinned golangci-lint version"
	@echo "  fmt               Format Go files with golangci-lint"
	@echo "  fmt-check         Check formatting without modifying files"
	@echo "  lint              Run golangci-lint"
	@echo "  lint-fix          Apply safe automatic lint fixes"
	@echo "  check             Run formatting, lint, and unit test checks"
	@echo "  tidy              Run go mod tidy"

deps-test:
	go mod download

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down -v

restart: down up

logs:
	$(COMPOSE) logs -f

ps:
	$(COMPOSE) ps

test: test-unit

test-unit:
	$(GO_ENV) go test ./...

test-integration: deps-test
	$(GO_ENV) go test -tags integration ./...

test-functional-integration: deps-test
	$(GO_ENV) go test -count=1 -v -tags integration ./functional

run-admin:
	go run ./cmd/schedulor-admin

install-lint: $(GOLANGCI_LINT)

$(GOLANGCI_LINT):
	mkdir -p $(TOOLS_DIR)
	$(GO_ENV) GOBIN=$(TOOLS_DIR) go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)

fmt: install-lint
	$(LINT_ENV) $(GOLANGCI_LINT) fmt ./...

fmt-check: install-lint
	$(LINT_ENV) $(GOLANGCI_LINT) fmt --diff ./...

lint: install-lint
	$(LINT_ENV) $(GOLANGCI_LINT) run ./...

lint-fix: install-lint
	$(LINT_ENV) $(GOLANGCI_LINT) run --fix ./...

check: fmt-check lint test-unit

tidy:
	go mod tidy
