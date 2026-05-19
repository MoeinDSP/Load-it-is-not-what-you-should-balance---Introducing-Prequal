#!/usr/bin/env bash
# compare.sh — replicates Figure 6 of the Prequal paper (NSDI '24).
#
# Backend capacity model (matches docker-compose.yml settings):
#   server1/2  CPU_LOAD=60  MAX_CONCURRENCY=1  latency≈35ms  → ~28 req/s each
#   server3    CPU_LOAD=0   MAX_CONCURRENCY=5  latency≈8ms   → ~625 req/s
#
# WRR capacity (equal distribution, bottlenecked at server1/2):
#   1 concurrent / 0.035 s × 3 servers ≈ 86 req/s  ← this is BASELINE
#
# Prequal capacity (routes ~95% to server3):
#   5 concurrent / 0.008 s ≈ 625 req/s
#
# Expected paper results (Figure 6):
#   ≤100% load : both algorithms fine, ~8ms p99 (Prequal steers to server3)
#   ~103%      : WRR p99 starts climbing; Prequal unchanged
#   ~114%+     : WRR returns 503 errors; Prequal zero errors
#
# Requirements:
#   hey   — brew install hey
#   Both load balancers running: docker compose up -d
#   bc, awk — pre-installed on macOS/Linux

set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
DURATION=30          # seconds per load level
PREQUAL_URL="http://localhost:8080"
WRR_URL="http://localhost:8081"
WORKERS=15           # concurrent hey workers (fixed)

# WRR capacity = MAX_CONCURRENCY_server12 / latency_server12 × 3_servers
# = 1 / 0.035 × 3 ≈ 86 req/s.  Override with --baseline if you change backends.
BASELINE=86

# ── Argument parsing ───────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $0 [OPTIONS]

Replicate Figure 6: Prequal vs WRR load-ramp experiment.

OPTIONS:
    -d, --duration SEC    Duration per load level in seconds (default: $DURATION)
    -b, --baseline QPS    WRR 100%% capacity in req/s (default: $BASELINE)
    -h, --help            Show this help

EXAMPLE:
    ./scripts/compare.sh --duration 60
EOF
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -d|--duration) DURATION="$2"; shift 2 ;;
        -b|--baseline) BASELINE="$2"; shift 2 ;;
        -h|--help)     usage; exit 0 ;;
        *) echo "Unknown option: $1"; usage; exit 1 ;;
    esac
done

# ── Preflight checks ───────────────────────────────────────────────────────────
check_deps() {
    local missing=0
    for cmd in hey curl bc awk; do
        if ! command -v "$cmd" &>/dev/null; then
            echo "Missing: $cmd"
            [[ "$cmd" == "hey" ]] && echo "   Install: brew install hey"
            missing=1
        fi
    done
    [[ $missing -eq 0 ]] || exit 1
}

check_services() {
    echo "Checking services..."
    for url in "$PREQUAL_URL/healthz" "$WRR_URL/healthz"; do
        if ! curl -sf "$url" >/dev/null 2>&1; then
            echo "Not responding: $url"
            echo "   Start with: docker compose up -d"
            exit 1
        fi
    done
    echo "Both load balancers are up."
}

check_deps
check_services

# ── Warmup (fills Prequal probe pool; WRR intentionally NOT warmed up) ────────
# Only Prequal gets a warmup so its probe pool has fresh RIF/latency entries.
# WRR starts the experiment with equal weights (EWMALatency=20ms initial for all
# servers) — this mirrors the paper's WRR whose CPU-utilization weights haven't
# yet adapted to the antagonist load, reproducing the trailing-signal weakness.
echo ""
echo "Warming up Prequal probe pool (10s)..."
hey -z 10s -q 1 -c "$WORKERS" "$PREQUAL_URL" >/dev/null 2>&1
echo "Warmup done.  Baseline WRR capacity: ${BASELINE} req/s"

# ── Load levels (×10/9 each step, matching paper §5.1) ────────────────────────
# hey -q is PER WORKER; divide target total QPS by number of workers.
LEVELS=(0.75 0.83 0.93 1.03 1.14 1.27 1.41 1.57 1.74)
NAMES=("75%" "83%" "93%" "103%" "114%" "127%" "141%" "157%" "174%")
RESULTS_DIR="/tmp/prequal_compare_$(date +%s)"
mkdir -p "$RESULTS_DIR"

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║         Prequal vs WRR — Load Ramp (Figure 6 Replication)          ║"
echo "╠══════════════════════════════════════════════════════════════════════╣"
printf "║  %-8s  %-8s  %-18s  %-18s  ║\n" "Load" "QPS" "Prequal p99 (ms)" "WRR p99 (ms)"
echo "╠══════════════════════════════════════════════════════════════════════╣"

for i in "${!LEVELS[@]}"; do
    level="${LEVELS[$i]}"
    name="${NAMES[$i]}"
    total_qps=$(echo "$BASELINE * $level" | bc -l | awk '{printf "%.0f", $1}')
    per_worker=$(echo "scale=2; $total_qps / $WORKERS" | bc -l)

    pfile="$RESULTS_DIR/prequal_step${i}.txt"
    wfile="$RESULTS_DIR/wrr_step${i}.txt"

    # Run both algorithms simultaneously (parallel background jobs).
    hey -z "${DURATION}s" -q "$per_worker" -c "$WORKERS" "$PREQUAL_URL" > "$pfile" 2>&1 &
    PID_P=$!
    hey -z "${DURATION}s" -q "$per_worker" -c "$WORKERS" "$WRR_URL"    > "$wfile" 2>&1 &
    PID_W=$!

    wait $PID_P $PID_W

    # hey outputs "99%% in X secs" (double-percent in terminal).
    p99_prequal=$(grep "99%%" "$pfile" | awk '{printf "%.0f", $3*1000}')
    p99_wrr=$(    grep "99%%" "$wfile" | awk '{printf "%.0f", $3*1000}')

    # Count non-2xx responses (hey formats as "  [503]	N responses" with leading spaces).
    err_p=$(awk '/\[/ && !/\[200\]/ && /responses/ {for(i=1;i<=NF;i++) if($i~/^[0-9]+$/ && $i+0>0) sum+=$i+0} END{print sum+0}' "$pfile" 2>/dev/null || echo 0)
    err_w=$(awk '/\[/ && !/\[200\]/ && /responses/ {for(i=1;i<=NF;i++) if($i~/^[0-9]+$/ && $i+0>0) sum+=$i+0} END{print sum+0}' "$wfile" 2>/dev/null || echo 0)

    [[ -z "$p99_prequal" ]] && p99_prequal="timeout"
    [[ -z "$p99_wrr"     ]] && p99_wrr="timeout"

    p_label="${p99_prequal}ms"
    w_label="${p99_wrr}ms"
    [[ "$err_p" -gt 0 ]] && p_label="${p99_prequal}ms (${err_p} err)"
    [[ "$err_w" -gt 0 ]] && w_label="${p99_wrr}ms (${err_w} err)"

    printf "║  %-8s  %-8s  %-18s  %-18s  ║\n" "$name" "${total_qps}/s" "$p_label" "$w_label"

    [[ $i -lt $((${#LEVELS[@]} - 1)) ]] && sleep 3
done

echo "╚══════════════════════════════════════════════════════════════════════╝"
echo ""
echo "Detailed hey output saved in: $RESULTS_DIR"
echo ""
echo "View live metrics in Grafana: http://localhost:3001  (admin/admin)"
echo "  - 'Request Latency'   → Fig. 5 (tail latency)"
echo "  - 'Error Rate'        → Fig. 6b (WRR errors above 103%)"
echo "  - 'RIF per Server'    → Fig. 4 (Prequal avoids server1/2)"
echo "  - 'Traffic Steering'  → Prequal routes ~95%+ to server3"
