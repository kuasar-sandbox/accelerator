#!/bin/bash
# Dependency-free /proc sampler (no sysstat/fio/iostat needed).
# Samples CPU (/proc/stat), disk (/proc/diskstats), NIC (/proc/net/dev) @1s.
# Usage: procmon.sh <outfile> [disk_dev=vda] [nic=eth0]
out="${1:-/tmp/procmon.csv}"; dev="${2:-vda}"; nic="${3:-eth0}"
echo "ts,cpu_user,cpu_sys,cpu_idle,cpu_iowait,rd_ios,rd_sec,wr_ios,wr_sec,io_ms,rx_bytes,tx_bytes" > "$out"
while :; do
  ts=$(date +%s)
  # /proc/stat line0: cpu user nice system idle iowait irq softirq ...
  read -r _ cu _ cs ci cw _ < /proc/stat
  d=$(awk -v dv="$dev" '$3==dv{printf "%s,%s,%s,%s,%s",$4,$6,$8,$10,$13}' /proc/diskstats)
  n=$(awk -F'[: ]+' -v nd="$nic" '($1==nd||$2==nd){printf "%s,%s",$3,$11}' /proc/net/dev)
  echo "${ts},${cu},${cs},${ci},${cw},${d},${n}" >> "$out"
  sleep 1
done
