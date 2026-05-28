# sandbox-builder — OCI → EROFS image builder.
#
# flatten-ctl is pure Go (CGO_ENABLED=0). It invokes `mkfs.erofs` at runtime
# (resolved via PATH / its own dir); that binary is produced by sandbox-deps
# (`make -C ../sandbox-deps erofs`).

SHELL := /bin/bash

.PHONY: all build flatten-ctl test vet bench clean help

# ---------------------------------------------------------------------------
# Architecture selection (identical block across all kuasar-sandbox repos)
# ---------------------------------------------------------------------------
HOST_ARCH   := $(shell uname -m)
TARGET_ARCH ?= $(HOST_ARCH)
ifeq ($(TARGET_ARCH),amd64)
  override TARGET_ARCH := x86_64
endif
ifeq ($(TARGET_ARCH),arm64)
  override TARGET_ARCH := aarch64
endif
ifeq ($(TARGET_ARCH),x86_64)
  GO_ARCH := amd64
else ifeq ($(TARGET_ARCH),aarch64)
  GO_ARCH := arm64
else
  $(error unsupported TARGET_ARCH=$(TARGET_ARCH); supported: x86_64, aarch64)
endif

# ---------------------------------------------------------------------------
# Build settings
# ---------------------------------------------------------------------------
GO             := go
GO_BUILD_FLAGS := -trimpath
BINDIR         := bin/$(TARGET_ARCH)

define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
all: build

build: flatten-ctl

flatten-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/flatten-ctl ./cmd/flatten-ctl
	$(call link_bin,flatten-ctl)

test:
	CGO_ENABLED=0 $(GO) test ./...

vet:
	CGO_ENABLED=0 $(GO) vet ./...

clean:
	rm -rf bin build

# Go micro-benchmarks. No e2e here — pkg/image unit tests cover the read
# surface and the OCI→EROFS engine is exercised by sandbox-* e2e in kuasar-sandbox.
bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

help:
	@echo "sandbox-builder. Targets:"
	@echo "  build / flatten-ctl   build the OCI→EROFS CLI (pure Go)"
	@echo "  test / vet / clean"
	@echo "  TARGET_ARCH           x86_64 (default) | aarch64"
