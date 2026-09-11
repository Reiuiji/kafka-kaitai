# ==============================================================================
# KafkaKaitai — Makefile
# ==============================================================================

SHELL := /bin/bash

# Detect Go compiler binary across standard system paths
GO ?= $(shell which go 2>/dev/null || which /usr/bin/go 2>/dev/null || which /usr/lib/go/bin/go 2>/dev/null || echo "go")
GOPATH ?= $(shell $(GO) env GOPATH 2>/dev/null || echo $(HOME)/go)

# Export robust PATH with Go bin and system utilities
export PATH := $(GOPATH)/bin:/usr/lib/go/bin:/usr/local/go/bin:/usr/local/bin:/usr/bin:/bin:$(PATH)

BINARY_NAME ?= kafka-kaitai
BIN_DIR ?= bin
WAILS ?= $(shell which wails3 2>/dev/null || echo $(GOPATH)/bin/wails3)

# Container Engine Auto-detection (podman or sudo podman if rootless namespaces are restricted)
PODMAN ?= $(shell podman info >/dev/null 2>&1 && echo "podman" || echo "sudo podman")

# Kafka Container Options
KAFKA_IMAGE ?= docker.io/apache/kafka:latest
KAFKA_CONTAINER ?= kafka-local
KAFKA_PORT ?= 9092

.PHONY: all help build build-frontend build-backend bindings dev test clean \
        podman-pull-kafka kafka-pull kafka-up kafka-down kafka-status kafka-logs

# Default target
all: build

help:
	@echo "KafkaKaitai Build & Podman Management Targets:"
	@echo ""
	@echo "  make build               Build frontend and Go binary (output: $(BIN_DIR)/$(BINARY_NAME))"
	@echo "  make build-frontend      Compile frontend Vite assets"
	@echo "  make build-backend       Compile Go desktop application"
	@echo "  make bindings            Re-generate TypeScript bindings from Go services"
	@echo "  make dev                 Run Wails v3 development server with live reload"
	@echo "  make test                Run all unit and integration tests"
	@echo "  make clean               Clean build artifacts"
	@echo ""
	@echo "Kafka Podman Targets (using: $(PODMAN)):"
	@echo "  make podman-pull-kafka   Pull the official Kafka image ($(KAFKA_IMAGE)) via Podman"
	@echo "  make kafka-pull          Alias for podman-pull-kafka"
	@echo "  make kafka-up            Start a local single-node Kafka container in KRaft mode"
	@echo "  make kafka-down          Stop and remove the local Kafka container"
	@echo "  make kafka-status        Check if the local Kafka container is running"
	@echo "  make kafka-logs          View logs of the local Kafka container"

# ------------------------------------------------------------------------------
# Build & Dev Targets
# ------------------------------------------------------------------------------

bindings:
	@echo "==> Generating TypeScript bindings..."
	@PATH="$$PATH:$(GOPATH)/bin" $(WAILS) generate bindings -ts

build-frontend:
	@echo "==> Building frontend bundle..."
	@npm --prefix frontend run build

build-backend:
	@echo "==> Compiling Go binary with $(GO)..."
	@mkdir -p $(BIN_DIR)
	@$(GO) build -o $(BIN_DIR)/$(BINARY_NAME) .
	@echo "==> Binary ready: $(BIN_DIR)/$(BINARY_NAME)"

build: build-frontend build-backend

dev:
	@echo "==> Starting Wails v3 in dev mode..."
	@PATH="$$PATH:$(GOPATH)/bin" $(WAILS) dev

test:
	@echo "==> Running Go test suite with $(GO)..."
	@$(GO) test -v ./internal/kaitai ./models ./services

clean:
	@echo "==> Cleaning build artifacts..."
	@rm -rf $(BIN_DIR) frontend/dist

# ------------------------------------------------------------------------------
# Podman Kafka Targets
# ------------------------------------------------------------------------------

podman-pull-kafka:
	@echo "==> Pulling Kafka image: $(KAFKA_IMAGE) via $(PODMAN)..."
	@$(PODMAN) pull $(KAFKA_IMAGE)

kafka-pull: podman-pull-kafka

kafka-up:
	@echo "==> Starting local Kafka container ($(KAFKA_CONTAINER)) on port $(KAFKA_PORT) using $(PODMAN)..."
	@if $(PODMAN) ps -a --format '{{.Names}}' | grep -Eq "^$(KAFKA_CONTAINER)$$"; then \
		echo "Container $(KAFKA_CONTAINER) already exists. Starting it..."; \
		$(PODMAN) start $(KAFKA_CONTAINER); \
	else \
		$(PODMAN) run -d \
			--name $(KAFKA_CONTAINER) \
			-p $(KAFKA_PORT):9092 \
			$(KAFKA_IMAGE); \
	fi
	@echo "==> Kafka is starting on localhost:$(KAFKA_PORT)"
	@echo "    Run 'make kafka-logs' to monitor startup"

kafka-down:
	@echo "==> Stopping and removing Kafka container ($(KAFKA_CONTAINER))..."
	@-$(PODMAN) stop $(KAFKA_CONTAINER) 2>/dev/null || true
	@-$(PODMAN) rm $(KAFKA_CONTAINER) 2>/dev/null || true
	@echo "==> Container stopped."

kafka-status:
	@echo "==> Checking Kafka container status:"
	@$(PODMAN) ps -a --filter "name=^$(KAFKA_CONTAINER)$$"

kafka-logs:
	@$(PODMAN) logs -f $(KAFKA_CONTAINER)
