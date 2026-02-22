SHELL := /bin/sh
COMPOSE := docker compose

.PHONY: help deps-test up down restart logs ps test test-unit test-integration run-cli fmt tidy

help:
	@echo "Targets:"
	@echo "  deps-test         Install integration test dependencies"
	@echo "  up                Start local dependencies (postgres, redis, kafka)"
	@echo "  down              Stop local dependencies"
	@echo "  restart           Restart local dependencies"
	@echo "  logs              Tail dependency logs"
	@echo "  ps                Show dependency status"
	@echo "  test              Run unit tests"
	@echo "  test-unit         Run unit tests"
	@echo "  test-integration  Run integration tests (requires Docker)"
	@echo "  run-cli           Run schedulor CLI"
	@echo "  fmt               Run gofmt on all Go files"
	@echo "  tidy              Run go mod tidy"

deps-test:
	go get github.com/jackc/pgx/v5/stdlib github.com/testcontainers/testcontainers-go github.com/testcontainers/testcontainers-go/modules/postgres

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
	go test ./...

test-integration: deps-test
	go test -tags integration ./...

run-cli:
	go run ./cmd/schedulor

fmt:
	gofmt -w $$(find . -name '*.go' -type f)

tidy:
	go mod tidy
