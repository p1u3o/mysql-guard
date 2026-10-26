#!/usr/bin/env bash
# Act-mode integration drills for mysql-guard (requires Docker, curl, jq).
# Runs: leader/seed check, switchover, switch back, kill-primary failover,
# fence of the returning old primary, and rejoin. Every stage is asserted;
# the script exits non-zero on the first failed assertion.
# Use COMPOSE_PROJECT_NAME to isolate the stack; SNAPDIR for snapshots.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/integration"

for bin in docker curl jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "NOTICE: $bin not available; drills NOT executed"; exit 2; }
done

SNAPDIR="${SNAPDIR:-$ROOT/integration/snaps}"
mkdir -p "$SNAPDIR"
GUARDS=(9471 9472 9473)
FLAGS=(--cluster=example --peers=db-a,db-b --mode=act --mysql-user=root --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306)

log() { echo "[$(date -u +%H:%M:%SZ)] $*"; }
fail() { log "FAIL: $*"; snap "zz-failure" || true; exit 1; }
pass() { log "PASS: $*"; }
sqlq() { docker compose exec -T "$1" mysql -uroot -pplaceholder -N -B -e "$2" 2>/dev/null; }
status() { curl -sf -m 5 "http://127.0.0.1:$1/status"; }
actions() { curl -sf -m 5 "http://127.0.0.1:9471/api/actions"; }
kv_primary() { status 9473 | jq -r '.primary'; }
kv_epoch() { status 9473 | jq -r '.epoch'; }
ro() { sqlq "$1" "SELECT CONCAT(@@global.read_only, @@global.super_read_only)"; }
gtid() { sqlq "$1" "SELECT REPLACE(@@global.gtid_executed, '\n', '')"; }
count_ok() { actions | jq --arg a "$1" --arg s "${2:-end}" '[.entries[] | select(.action==$a and .step==$s and .result=="ok")] | length'; }
count_skipped() { actions | jq '[.entries[] | select(.result=="skipped")] | length'; }

snap() {
  local s="$1"
  for p in "${GUARDS[@]}"; do
    curl -s -m 5 "http://127.0.0.1:$p/status" > "$SNAPDIR/$s-$p-status.json" || true
  done
  curl -s -m 5 "http://127.0.0.1:9471/api/actions" > "$SNAPDIR/$s-actions.json" || true
  for d in db-a db-b; do
    docker compose exec -T "$d" mysql -uroot -pplaceholder -e "SELECT @@hostname,@@read_only,@@super_read_only,@@gtid_executed\\G SHOW REPLICA STATUS\\G" > "$SNAPDIR/$s-$d-mysql.txt" 2>/dev/null || true
  done
  docker compose exec -T consul consul kv export > "$SNAPDIR/$s-consul-kv.json" 2>/dev/null || true
}

# wait_for <seconds> <description> <command...>
wait_for() {
  local t="$1" d="$2"; shift 2
  for _ in $(seq 1 "$t"); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  fail "timeout after ${t}s waiting for: $d"
}

one_leader() {
  local n=0 lead=""
  for p in "${GUARDS[@]}"; do
    if [ "$(status "$p" | jq -r '.leader')" = "true" ]; then n=$((n+1)); lead=$p; fi
  done
  [ "$n" = 1 ] && [ "$lead" != 9473 ]
}
primary_is() { for p in "${GUARDS[@]}"; do [ "$(status "$p" | jq -r '.primary')" = "$1" ] || return 1; done; }
gtids_match() { local a b; a=$(gtid db-a); b=$(gtid db-b); [ -n "$a" ] && [ "$a" = "$b" ]; }
replica_running() {
  [ "$(sqlq "$1" "SELECT CONCAT((SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1), '/', (SELECT SERVICE_STATE FROM performance_schema.replication_applier_status LIMIT 1))")" = "ON/ON" ]
}
row_on() { [ "$(sqlq "$1" "SELECT COUNT(*) FROM drill.t WHERE v='$2'")" = 1 ]; }
write_row() { sqlq "$1" "CREATE DATABASE IF NOT EXISTS drill; CREATE TABLE IF NOT EXISTS drill.t (v VARCHAR(64) PRIMARY KEY); INSERT INTO drill.t VALUES ('$2')" || fail "write $2 on $1"; }

log "Building and starting stack (project ${COMPOSE_PROJECT_NAME:-default})"
docker compose build guard-a guard-b guard-witness
docker compose up -d consul db-a db-b
wait_for 120 "db-a healthy" sh -c "docker compose exec -T db-a mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"
wait_for 120 "db-b ready" sh -c "docker compose exec -T db-b mysql -uroot -pplaceholder -e 'SELECT 1'"

log "Configure replication db-b -> db-a (clear db-b init GTIDs first)"
sqlq db-b "STOP REPLICA; RESET BINARY LOGS AND GTIDS; CHANGE REPLICATION SOURCE TO SOURCE_HOST='db-a', SOURCE_USER='repl', SOURCE_PASSWORD='placeholder', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA; SET GLOBAL super_read_only=ON;" || fail "configure replication"
wait_for 60 "db-b replicating" replica_running db-b
wait_for 60 "GTID sets match after bootstrap" gtids_match

docker compose up -d guard-a guard-b guard-witness
snap 00-start

log "=== Drill 1: leader election and primary seed ==="
wait_for 60 "exactly one DB guard leader" one_leader
wait_for 60 "primary seeded as db-a on all guards" primary_is db-a
E0=$(kv_epoch); [ "$E0" -ge 1 ] || fail "epoch after seed is $E0"
[ "$(count_ok seed_primary seed)" -ge 1 ] || fail "seed_primary not in action log"
[ "$(status 9473 | jq -r '.leader')" = false ] || fail "witness became leader"
sleep 6; one_leader || fail "leadership not stable"
snap 01-leader
pass "drill 1: one leader ($(for p in "${GUARDS[@]}"; do echo -n "$p=$(status $p | jq -r .leader) "; done)), primary=db-a, epoch=$E0"

check_roles() { # <primary> <replica>
  [ "$(ro "$1")" = "00" ] || fail "$1 read_only/super_read_only=$(ro "$1"), want 00"
  [ "$(ro "$2")" = "11" ] || fail "$2 read_only/super_read_only=$(ro "$2"), want 11"
}

log "=== Drill 2: switchover db-a -> db-b (act) ==="
check_roles db-a db-b
write_row db-a pre-switch
wait_for 30 "pre-switch row on db-b" row_on db-b pre-switch
N=$(count_ok switchover); E=$(kv_epoch)
docker compose exec -T guard-a mysql-guard switchover db-b --node=db-a --mysql-host=db-a "${FLAGS[@]}" || fail "switchover command exit"
wait_for 20 "primary db-b on all guards" primary_is db-b
check_roles db-b db-a
[ "$(kv_epoch)" = $((E+1)) ] || fail "epoch $(kv_epoch) want $((E+1))"
[ "$(count_ok switchover)" = $((N+1)) ] || fail "switchover end/ok not logged"
wait_for 30 "db-a replicating from db-b" replica_running db-a
write_row db-b post-switch
wait_for 30 "post-switch row on db-a" row_on db-a post-switch
wait_for 30 "GTIDs match" gtids_match
snap 02-switchover
pass "drill 2: switchover db-a->db-b epoch $E->$(kv_epoch), gtid=$(gtid db-a)"

log "=== Drill 3: switch back db-b -> db-a (act) ==="
N=$(count_ok switchover); E=$(kv_epoch)
docker compose exec -T guard-b mysql-guard switchover db-a --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "switchback command exit"
wait_for 20 "primary db-a on all guards" primary_is db-a
check_roles db-a db-b
[ "$(kv_epoch)" = $((E+1)) ] || fail "epoch $(kv_epoch) want $((E+1))"
[ "$(count_ok switchover)" = $((N+1)) ] || fail "switchback end/ok not logged"
wait_for 30 "db-b replicating from db-a" replica_running db-b
write_row db-a post-switchback
wait_for 30 "post-switchback row on db-b" row_on db-b post-switchback
wait_for 30 "GTIDs match" gtids_match
snap 03-switchback
pass "drill 3: switchback db-b->db-a epoch $E->$(kv_epoch), gtid=$(gtid db-a)"

log "=== Drill 4: kill primary db-a, automatic failover (act) ==="
write_row db-a pre-kill
wait_for 30 "pre-kill row on db-b" row_on db-b pre-kill
wait_for 30 "GTIDs match before kill" gtids_match
GT_BEFORE=$(gtid db-a)
N=$(count_ok failover); E=$(kv_epoch)
docker compose stop db-a
T0=$(date +%s)
wait_for 90 "failover to db-b" primary_is db-b
log "failover observed after $(( $(date +%s) - T0 ))s"
[ "$(ro db-b)" = "00" ] || fail "db-b not writable after failover: $(ro db-b)"
[ "$(kv_epoch)" = $((E+1)) ] || fail "epoch $(kv_epoch) want $((E+1))"
[ "$(count_ok failover)" = $((N+1)) ] || fail "failover end/ok not logged"
GT_B=$(gtid db-b)
[ "$GT_B" = "$GT_BEFORE" ] || fail "db-b gtid $GT_B != pre-kill primary gtid $GT_BEFORE"
write_row db-b during-outage
snap 04-failover
pass "drill 4: failover db-a->db-b epoch $E->$(kv_epoch), no lost GTIDs ($GT_BEFORE)"

log "=== Drill 5: old primary returns and is fenced ==="
N=$(count_ok fence super_read_only)
docker compose start db-a
wait_for 120 "db-a back" sh -c "docker compose exec -T db-a mysql -uroot -pplaceholder -e 'SELECT 1'"
wait_for 60 "db-a fenced (super_read_only)" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT @@global.super_read_only' 2>/dev/null)\" = 1 ]"
[ "$(count_ok fence super_read_only)" -ge $((N+1)) ] || fail "fence ok not logged"
primary_is db-b || fail "primary changed after old primary returned"
snap 05-fenced
pass "drill 5: returning db-a fenced (ro=$(ro db-a)), primary stays db-b"

log "=== Drill 6: rejoin db-a under db-b (act) ==="
N=$(count_ok rejoin)
docker compose exec -T guard-a mysql-guard rejoin db-b --node=db-a --mysql-host=db-a "${FLAGS[@]}" || fail "rejoin command exit"
[ "$(count_ok rejoin)" = $((N+1)) ] || fail "rejoin end/ok not logged"
wait_for 30 "db-a replicating from db-b" replica_running db-a
wait_for 30 "outage row on db-a" row_on db-a during-outage
write_row db-b post-rejoin
wait_for 30 "post-rejoin row on db-a" row_on db-a post-rejoin
wait_for 30 "GTIDs match" gtids_match
check_roles db-b db-a
snap 06-rejoin
pass "drill 6: rejoin ok, gtid=$(gtid db-a)"

SK=$(count_skipped)
[ "$SK" = 0 ] || fail "$SK action log entries with result=skipped"
one_leader || fail "leadership not single at end"
pass "action log: no skipped entries; $(actions | jq '.entries | length') entries total"
log "ALL DRILLS PASSED"
docker compose ps
