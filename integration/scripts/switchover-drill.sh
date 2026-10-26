#!/usr/bin/env bash
# Integration drills for mysql-guard (requires Docker).
# If Docker is unavailable, prints notice and runs unit tests only.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/integration"

if ! command -v docker >/dev/null 2>&1; then
  echo "NOTICE: Docker not available on this host — integration drills were NOT executed."
  echo "The script below is complete; run it locally with Docker to exercise the stack."
  (cd "$ROOT" && go test ./internal/actions/... ./internal/mysql/... -count=1)
  exit 0
fi

echo "Starting stack..."
docker compose build guard-a guard-b guard-witness
docker compose up -d consul db-a db-b guard-a guard-b guard-witness

echo "Wait for guards..."
sleep 15

assert_status() {
  local url="$1"
  curl -sf "$url/status" | grep -q '"cluster"\|"primary"\|"node"' || curl -sf "$url/status" | grep -q '"node"'
}

assert_status "http://127.0.0.1:9471"
assert_status "http://127.0.0.1:9473"

echo "Configure replication db-b -> db-a (if not already)"
docker compose exec -T db-b mysql -uroot -pplaceholder -e "CHANGE REPLICATION SOURCE TO SOURCE_HOST='db-a', SOURCE_USER='repl', SOURCE_PASSWORD='placeholder', SOURCE_AUTO_POSITION=1; START REPLICA;" 2>/dev/null || true

echo "Action log endpoint"
curl -sf "http://127.0.0.1:9471/api/actions" | grep -q entries

echo "Switchover (observe) from guard-a"
docker compose exec -T guard-a mysql-guard switchover db-b --cluster=example --peers=db-a,db-b --node=db-a --mode=observe --mysql-user=root --mysql-password=placeholder --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306

echo "Switch back (observe)"
docker compose exec -T guard-b mysql-guard switchover db-a --cluster=example --peers=db-a,db-b --node=db-b --mode=observe --mysql-user=root --mysql-password=placeholder --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306

echo "Kill primary drill (observe mode — logs only)"
docker compose stop db-a
sleep 5
curl -sf "http://127.0.0.1:9472/status" >/dev/null || true
docker compose start db-a
sleep 10

echo "Rejoin drill (observe)"
docker compose exec -T guard-a mysql-guard rejoin db-b --cluster=example --peers=db-a,db-b --node=db-a --mode=observe --mysql-user=root --mysql-password=placeholder --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306

echo "Integration drill finished OK"
docker compose ps
