#!/usr/bin/env bash
# setup.sh — build images, start all services, and wait until healthy.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "Building Docker images..."
docker compose build --parallel

echo "Starting services..."
docker compose up -d

echo "Waiting for load balancers to be ready..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:8080/healthz >/dev/null 2>&1 && \
       curl -sf http://localhost:8081/healthz >/dev/null 2>&1; then
        echo "✓  Both load balancers are up."
        break
    fi
    echo "  waiting... ($i/30)"
    sleep 2
done

echo ""
echo "Services running:"
echo "  Prequal LB   →  http://localhost:8080"
echo "  WRR LB       →  http://localhost:8081"
echo "  Prometheus   →  http://localhost:9090"
echo "  Grafana      →  http://localhost:3001  (admin/admin)"
echo ""
echo "Quick smoke test:"
echo "  curl http://localhost:8080/"
echo "  curl http://localhost:8081/"
echo ""
echo "Run Figure-6 replication:"
echo "  ./scripts/compare.sh --duration 60"
