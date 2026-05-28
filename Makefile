# sandbox-accelerator — storage acceleration layer (manifest / cache / store).
#
# Builds three daemons:
#   manifest-ctl, store-ctl  — pure Go (CGO_ENABLED=0)
#   cache-ctl                — CGO, statically links librocksdb.a
#
# The thin client surface that downstream repos import (pkg/manifest,
# pkg/{cache,store}/client) is CGO-free; only cache-ctl pulls RocksDB.

SHELL := /bin/bash

.PHONY: all build manifest-ctl store-ctl cache-ctl deps-rocksdb test vet bench test-e2e test-e2e-cache test-e2e-cluster perf-cache perf-cache-remote dedup-report clean help

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
BUILD_DIR      := build/$(TARGET_ARCH)
ROCKS_PREFIX   := $(abspath $(BUILD_DIR)/rocksdb)

# CGO flags for cache-ctl: static link librocksdb.a + libstdc++; glibc stays
# dynamic. RocksDB is built without compression so the link line stays minimal.
CGO_CFLAGS         := -I$(ROCKS_PREFIX)/include
CGO_LDFLAGS_STATIC := -L$(ROCKS_PREFIX)/lib -Wl,-Bstatic -lrocksdb -lstdc++ -Wl,-Bdynamic -lm -lpthread -ldl
GOLDFLAGS_STATIC   := -linkmode=external -extldflags "-static-libstdc++ -static-libgcc"

# Native-only symlink: bin/<name> -> $(TARGET_ARCH)/<name>. $(1) = basename.
define link_bin
@if [ "$(HOST_ARCH)" = "$(TARGET_ARCH)" ]; then \
   mkdir -p bin && ln -sfn $(TARGET_ARCH)/$(1) bin/$(1); \
 fi
endef

# ---------------------------------------------------------------------------
# Targets
# ---------------------------------------------------------------------------
all: build

build: manifest-ctl store-ctl cache-ctl

manifest-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/manifest-ctl ./cmd/manifest-ctl
	$(call link_bin,manifest-ctl)

store-ctl:
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=0 $(GO) build $(GO_BUILD_FLAGS) -o $(BINDIR)/store-ctl ./cmd/store-ctl
	$(call link_bin,store-ctl)

# cache-ctl requires CGO for RocksDB.
cache-ctl: deps-rocksdb
	@mkdir -p $(BINDIR)
	GOOS=linux GOARCH=$(GO_ARCH) CGO_ENABLED=1 \
	    CGO_CFLAGS="$(CGO_CFLAGS)" CGO_LDFLAGS="$(CGO_LDFLAGS_STATIC)" \
	    $(GO) build $(GO_BUILD_FLAGS) -ldflags '$(GOLDFLAGS_STATIC)' -o $(BINDIR)/cache-ctl ./cmd/cache-ctl
	$(call link_bin,cache-ctl)

# Build librocksdb.a locally (no compression). ~minutes cold.
deps-rocksdb: $(ROCKS_PREFIX)/lib/librocksdb.a
$(ROCKS_PREFIX)/lib/librocksdb.a:
	TARGET_ARCH="$(TARGET_ARCH)" \
	BUILD_DIR="$(abspath $(BUILD_DIR))" \
	BINDIR="$(abspath $(BINDIR))" \
	TARBALL_CACHE="$(abspath build/tarball)" \
		bash deps/build-rocksdb.sh

# Unit tests. cache/rocks tests need librocksdb (dynamic link is fine here).
test: deps-rocksdb
	CGO_CFLAGS="$(CGO_CFLAGS)" \
	CGO_LDFLAGS="-L$(ROCKS_PREFIX)/lib -lrocksdb -lstdc++ -lm -lpthread -ldl" \
		$(GO) test ./...

# vet the CGO-free client surface (no librocksdb needed).
vet:
	CGO_ENABLED=0 $(GO) vet ./pkg/manifest/... ./pkg/store/client/... ./pkg/cache/client/... ./cmd/manifest-ctl ./cmd/store-ctl

clean:
	rm -rf bin build

# ---------------------------------------------------------------------------
# Tests + benchmarks + perf
# ---------------------------------------------------------------------------
# e2e/perf scripts find platform binaries in $(SBIN) (the umbrella's assembled
# bin/<arch>/). Run `make -C ../kuasar-sandbox build` first, or invoke from the
# umbrella's `make test-e2e` which builds + drives every repo's tests.
SBIN := $(abspath ../kuasar-sandbox/bin/$(TARGET_ARCH))

bench:
	CGO_ENABLED=0 $(GO) test -bench=. -benchmem -run=^$$ ./...

test-e2e: test-e2e-cache test-e2e-cluster

test-e2e-cache:
	BIN=$(SBIN) bash test/e2e/e2e_cache.sh

test-e2e-cluster:
	BIN=$(SBIN) bash test/e2e/e2e_cluster_rolling.sh

perf-cache:
	BIN=$(SBIN) bash test/scripts/bench_cache.sh

perf-cache-remote:
	BIN=$(SBIN) bash test/scripts/bench_cache_remote.sh

dedup-report:
	BIN=$(SBIN) bash test/scripts/dedup_report.sh

help:
	@echo "sandbox-accelerator. Targets:"
	@echo "  build         build manifest-ctl + store-ctl + cache-ctl"
	@echo "  manifest-ctl  pure Go"
	@echo "  store-ctl     pure Go"
	@echo "  cache-ctl     CGO + librocksdb (auto deps-rocksdb)"
	@echo "  deps-rocksdb  build local librocksdb.a"
	@echo "  test          unit tests (needs librocksdb for cache/rocks)"
	@echo "  vet           vet the CGO-free client surface"
	@echo "  clean         remove bin/ + build/"
	@echo "  TARGET_ARCH   x86_64 (default) | aarch64"
