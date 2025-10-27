#!/usr/bin/env bash
# etcd_metrics_delta.sh
# Requires bash >= 4

set -euo pipefail

# ---- 配置区 ----
interval=10   # 秒
endpoints=(
  "http://192.168.0.38:2379"
  "http://192.168.0.38:3379"
  "http://192.168.0.38:4379"
  "http://192.168.0.82:2379"
  "http://192.168.0.82:3379"
  "http://192.168.0.82:4379"
  "http://192.168.0.223:2379"
  "http://192.168.0.223:3379"
  "http://192.168.0.223:4379"
)
metrics=(
  "etcd_server_proposals_committed_total"
  "etcd_debugging_store_reads_total"
  "etcd_debugging_store_writes_total"
)
curl_opts=("--silent" "--show-error" "--fail" "--max-time" "5")
# ------------------

declare -A prev   # prev["$endpoint|$metric"]=value

# get sum of a metric (handles lines with labels and non-labeled)
get_metric_sum() {
  local ep="$1"; local metric="$2"
  # fetch metrics, select lines that start with metric or metric{..., take last column as value, sum them
  local out
  if ! out=$(curl "${curl_opts[@]}" "$ep/metrics" 2>&1); then
    echo "ERR_FETCH:$out" >&2
    return 1
  fi
  # awk: match first field exactly metric or metric{..., sum last field
  echo "$out" | awk -v m="$metric" '
    $1 ~ ("^"m"($|\\{)") { val = $NF; 
      # handle scientific notation and possible trailing comments
      gsub(/[^0-9eE+.-]/, "", val);
      s+=val+0
    } 
    END { if (s=="") s=0; printf "%0.0f", s }'
}

# initial snapshot
ts_init=$(date +%s)
for ep in "${endpoints[@]}"; do
  for metric in "${metrics[@]}"; do
    val=$(get_metric_sum "$ep" "$metric" 2>/dev/null) || { echo "[$ep] failed to fetch $metric (initial), will retry in loop" >&2; val=0; }
    prev["$ep|$metric"]="$val"
  done
done

echo "Started sampling every ${interval}s. Initial timestamp: $ts_init"
echo "Format: <unix_ts> <endpoint> <metric> delta"

# loop
while true; do
  ts=$(date +%s)
  for ep in "${endpoints[@]}"; do
    for metric in "${metrics[@]}"; do
      new=$(get_metric_sum "$ep" "$metric" 2>/dev/null) || { echo "$ts $ep $metric ERROR_FETCH" >&2; continue; }
      key="$ep|$metric"
      prev_val=${prev[$key]:-0}

      # handle potential non-integer or reset:
      # if new < prev -> assume counter reset, treat delta = new
      # otherwise delta = new - prev
      # use awk for safe arithmetic (handles large ints)
      delta=$(awk -v n="$new" -v p="$prev_val" 'BEGIN{ if (n < p) { printf "%0.0f", n } else { printf "%0.0f", n-p } }')
      echo "$ts $ep $metric $delta"
      prev["$key"]="$new"
    done
  done

  sleep "$interval"
done

