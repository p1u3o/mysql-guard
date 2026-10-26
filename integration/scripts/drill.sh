#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/integration"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker not available; running compile and unit tests only"
  (cd "$ROOT" && go build -o /tmp/mysql-guard ./cmd/mysql-guard)
  (cd "$ROOT" && go test ./internal/decision/... ./internal/errant/... ./internal/rejoin/... -count=1)
  echo "Drill complete (offline mode)."
  exit 0
fi

echo "Starting integration stack..."
docker compose up -d --wait consul mysql-primary mysql-replica

echo "Consul leader:"
curl -sf http://127.0.0.1:8500/v1/status/leader | head -c 80 || true
echo

echo "MySQL primary ping:"
docker compose exec -T mysql-primary mysqladmin ping -h 127.0.0.1 -pplaceholder

echo "Building mysql-guard..."
(cd "$ROOT" && go build -o /tmp/mysql-guard ./cmd/mysql-guard)

echo "Running decision unit subset..."
(cd "$ROOT" && go test ./internal/decision/... ./internal/errant/... ./internal/rejoin/... -count=1)

echo "Drill placeholder: kill primary container (observe only in draft)"
docker compose kill mysql-primary
sleep 3
docker compose start mysql-primary

echo "Drill complete."
docker compose ps
