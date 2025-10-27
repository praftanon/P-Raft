
de_delta.sh
# 使用示例: ./etcd_node_delta.sh

ENDPOINTS=(
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

# 需要采集的 metric 列表
METRICS=("etcd_server_proposals_committed_total" "etcd_debugging_store_reads_total" "etcd_debugging_store_writes_total")

# 临时文件保存上次值
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# helper: read metric value from /metrics (returns value or empty)
get_metric_val() {
  local url="$1"
  local metric="$2"
  # 抓取并提取最后一行（防止帮助文本干扰）
  curl -sS "${url}/metrics" \
    | awk -v m="$metric" '$1==m {print $2}' \
    | tail -n1
}

# 初始抓一次，写入 TMPDIR/<instance>.<metric>
for ep in "${ENDPOINTS[@]}"; do
  inst=$(echo "$ep" | sed -E 's|https?://||')
  for m in "${METRICS[@]}"; do
    v=$(get_metric_val "$ep" "$m")
    echo "${v:-0}" > "$TMPDIR/${inst}.${m}"
  done
done

# loop 每 10s 打印 delta 并更新保存值
while true; do
  ts=$(date +%s)
  for ep in "${ENDPOINTS[@]}"; do
    inst=$(echo "$ep" | sed -E 's|https?://||')
    declare -A cur
    for m in "${METRICS[@]}"; do
      cur[$m]=$(get_metric_val "$ep" "$m")
      cur_val=${cur[$m]:-0}
      prev_file="$TMPDIR/${inst}.${m}"
      prev_val=0
      if [ -f "$prev_file" ]; then
        prev_val=$(cat "$prev_file")
      fi
      delta=$(awk -v c="$cur_val" -v p="$prev_val" 'BEGIN{printf("%.0f", (c - p))}')
      # 输出格式: timestamp instance metric delta
      echo "$ts $inst $m $delta"
      # 更新
      echo "$cur_val" > "$prev_file"
    done
  done
  sleep 10
done

