#!/bin/bash
# gen-proto.sh — regenerate Go stubs from .proto files.
#
# Requires protoc + protoc-gen-go + protoc-gen-go-grpc on PATH.
# Install the Go plugins if missing:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#
# The generated .pb.go and _grpc.pb.go files are committed to git so
# downstream consumers don't need protoc to build.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PROJECT_ROOT"

for tool in protoc protoc-gen-go protoc-gen-go-grpc; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "ERROR: $tool not found on PATH" >&2
        echo "  install: https://grpc.io/docs/languages/go/quickstart/" >&2
        exit 1
    fi
done

PROTO_FILES=(
    pkg/store/pb/store.proto
    pkg/cache/pb/info.proto
)

for pf in "${PROTO_FILES[@]}"; do
    echo "generating $pf"
    protoc \
        --go_out=. \
        --go_opt=paths=source_relative \
        --go-grpc_out=. \
        --go-grpc_opt=paths=source_relative \
        "$pf"
done

echo "done"
