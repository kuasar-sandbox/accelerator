#!/bin/bash

# Allocate loopback ports without reusing a port already handed out during the
# current E2E run. The lease file also makes concurrent command substitutions
# safe; the daemon readiness checks remain responsible for confirming bind.
e2e_free_port() {
    local lease_file=${E2E_PORT_LEASE_FILE:?E2E_PORT_LEASE_FILE must be set}

    (
        flock -x 9
        python3 - "$lease_file" <<'PY'
import socket
import sys

lease_path = sys.argv[1]
try:
    with open(lease_path, encoding="ascii") as lease:
        leased = {int(line) for line in lease if line.strip()}
except FileNotFoundError:
    leased = set()

for _ in range(1024):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    if port in leased:
        continue
    with open(lease_path, "a", encoding="ascii") as lease:
        lease.write(f"{port}\n")
    print(port)
    break
else:
    raise RuntimeError("could not allocate a unique E2E port")
PY
    ) 9>"${lease_file}.lock"
}
