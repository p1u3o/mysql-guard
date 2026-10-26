#!/usr/bin/env bash
# v0.3.0 integration drills (requires Docker, curl, jq): observe writes nothing,
# legacy master key + key migration, writer service sync, ProxySQL writer
# verification, ntfy alerts, aborted switchover after the epoch bump
# (fault-injected build), foreign master-key write detection.
# Exits non-zero on the first failed assertion. Set COMPOSE_PROJECT_NAME.
set -euo pipefail
export COMPOSE_FILE=docker-compose.yml:docker-compose.v030.yml
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/integration"

for bin in docker curl jq; do
  command -v "$bin" >/dev/null 2>&1 || { echo "NOTICE: $bin not available; drills NOT executed"; exit 2; }
done

SNAPDIR="${SNAPDIR:-$ROOT/integration/snaps}"
mkdir -p "$SNAPDIR"
GUARDS=(9471 9472 9473)
FLAGS=(--cluster=example --peers=db-a,db-b --mode=act --mysql-user=root --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306 --master-format=legacy --writer-service=example-writer --ntfy-url=http://ntfy/mg-drill)

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
docker compose build guard-a guard-b guard-witness; docker compose up -d ntfy
docker compose up -d consul db-a db-b
wait_for 120 "db-a healthy" sh -c "docker compose exec -T db-a mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"
wait_for 120 "db-b ready" sh -c "docker compose exec -T db-b mysql -uroot -pplaceholder -e 'SELECT 1'"

log "Configure replication db-b -> db-a (clear db-b init GTIDs first)"
cfg_repl() { sqlq db-b "STOP REPLICA; RESET BINARY LOGS AND GTIDS; CHANGE REPLICATION SOURCE TO SOURCE_HOST='db-a', SOURCE_USER='repl', SOURCE_PASSWORD='placeholder', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA; SET GLOBAL super_read_only=ON;"; }
# db-b may answer from its temporary init server and then restart: retry.
wait_for 60 "replication configured on db-b" cfg_repl
wait_for 60 "db-b replicating" replica_running db-b
wait_for 60 "GTID sets match after bootstrap" gtids_match

kv() { docker compose exec -T consul consul kv get "$1" 2>/dev/null || true; }
master() { kv mysql/master/example; }
svc() { curl -sf -m 5 "http://127.0.0.1:8500/v1/catalog/service/example-writer" | jq -r '.[] | select(.Node=="example-writer-vnode") | "\(.ServiceAddress):\(.ServicePort)"'; }
svc_is() { [ "$(svc)" = "$1" ]; }
alerts() { curl -sf -m 5 "http://127.0.0.1:18093/mg-drill/json?poll=1&since=all" | jq -r '.title // empty'; }
alert_n() { alerts | grep -cx "mysql-guard example: $1" || true; }
alert_gt() { [ "$(alert_n "$1")" -gt "$2" ]; }
px() { docker compose exec -T db-b mysql -h proxysql -P6033 -uroot -pplaceholder -N -B -e "$1" 2>/dev/null; }
px_write() { px "INSERT INTO drill.t VALUES ('$1')"; }
pxa() { docker compose exec -T db-b mysql -h proxysql -P6032 -uradmin -pplaceholder -N -B -e "$1" 2>/dev/null; }
px_writer() { pxa "SELECT hostname FROM runtime_mysql_servers WHERE hostgroup_id=10 AND status='ONLINE'"; }
px_writer_is() { [ "$(px_writer)" = "$1" ]; }
mk() { docker compose exec -T guard-a mysql-guard migrate-key "$@" --node=db-a --mysql-host=db-a "${FLAGS[@]}"; }
master_is() { [ "$(master)" = "$1" ]; }
cur_is() { [ "$(kv mysql/example/writer/current)" = "$1" ]; }
writes_through_proxysql_land_on() { # <writer> <tag>
  wait_for 30 "ProxySQL writer hostgroup = $1" px_writer_is "$1"
  wait_for 30 "write $2 through ProxySQL" px_write "$2"
  row_on "$1" "$2" || fail "ProxySQL write $2 did not land on $1"
}
two_writers() { [ "$(ro db-a 2>/dev/null)" = 00 ] && [ "$(ro db-b 2>/dev/null)" = 00 ]; }

log "=== V1: observe mode writes nothing (empty key, then a legacy key) ==="
sqlq db-a "CREATE DATABASE IF NOT EXISTS drill; CREATE TABLE IF NOT EXISTS drill.t (v VARCHAR(64) PRIMARY KEY)"
docker compose up -d proxysql
GMODE=observe docker compose up -d guard-a guard-b guard-witness
wait_for 60 "exactly one DB guard leader" one_leader
wait_for 30 "observe decision 'would seed'" sh -c "$(declare -f status); for p in 9471 9472; do status \$p | jq -r .last_decision; done | grep -q 'observe: would seed'"
sleep 10
[ -z "$(master)" ] || fail "observe wrote master key: $(master)"
[ -z "$(kv mysql/example/writer/epoch)" ] || fail "observe wrote epoch"
[ -z "$(svc)" ] || fail "observe registered writer service"
docker compose exec -T consul consul kv put mysql/master/example db-a:3306 >/dev/null
sleep 12
master_is db-a:3306 || fail "observe changed legacy key: $(master)"
[ -z "$(kv mysql/example/writer/epoch)$(kv mysql/example/writer/current)" ] || fail "observe wrote epoch/current"
[ -z "$(svc)" ] || fail "observe registered writer service with legacy key"
docker compose exec -T guard-a mysql-guard migrate-key -to=epoch --node=db-a --mysql-host=db-a "${FLAGS[@]}" --mode=observe >/dev/null 2>&1 && fail "migrate-key allowed with --mode=observe"
master_is db-a:3306 || fail "observe migrate-key changed key"
[ "$(alert_n switchover)$(alert_n failover)" = 00 ] || fail "observe sent action alerts"
snap v1-observe
pass "V1: observe wrote no master key, epoch, writer/current or writer service; migrate-key refused"

log "=== V2: act with the legacy key (prod mirror), writer service, key migration ==="
GMODE=act docker compose up -d guard-a guard-b guard-witness
wait_for 60 "exactly one DB guard leader (act)" one_leader
wait_for 60 "primary db-a on all guards" primary_is db-a
wait_for 30 "writer service -> db-a:3306" svc_is db-a:3306
master_is db-a:3306 || fail "act rewrote legacy key: $(master)"
mk -to=legacy --dry-run | grep -q 'target_value=db-a:3306' || fail "dry-run output"
mk -to=legacy | grep -q migrated || fail "migrate to legacy (init epoch)"
master_is db-a:3306 || fail "legacy migrate changed key"
[ "$(kv mysql/example/writer/epoch)" = 1 ] || fail "epoch not initialised to 1: $(kv mysql/example/writer/epoch)"
cur_is "db-a|1" || fail "writer/current $(kv mysql/example/writer/current)"
mk -to=epoch | grep -q migrated || fail "migrate to epoch"
master_is "db-a|1" || fail "epoch-format key $(master)"
sleep 8; primary_is db-a || fail "primary changed after migrate to epoch"
[ "$(alert_n foreign_master_write)" = 0 ] || fail "foreign alert after epoch migration"
mk -to=legacy | grep -q migrated || fail "migrate back to legacy"
master_is db-a:3306 || fail "legacy key after round trip $(master)"
sleep 8; primary_is db-a || fail "primary changed after round trip"
[ "$(alert_n foreign_master_write)" = 0 ] || fail "foreign alert after legacy round trip"
[ "$(alert_n failover)$(alert_n switchover)" = 00 ] || fail "unexpected action during migration"
writes_through_proxysql_land_on db-a v2
snap v2-legacy
pass "V2: legacy key read in act mode, service=db-a:3306, migrate legacy->epoch->legacy with no action, ProxySQL write ok"

log "=== V3: switchover db-a -> db-b (legacy key, service, ProxySQL, ntfy) ==="
E=$(kv_epoch); A=$(alert_n switchover)
docker compose exec -T guard-a mysql-guard switchover db-b --node=db-a --mysql-host=db-a "${FLAGS[@]}" || fail "switchover exit"
wait_for 20 "primary db-b" primary_is db-b
check_roles() { [ "$(ro "$1")" = 00 ] || fail "$1 ro=$(ro "$1")"; [ "$(ro "$2")" = 11 ] || fail "$2 ro=$(ro "$2")"; }
check_roles db-b db-a
master_is db-b:3306 || fail "key $(master)"
cur_is "db-b|$((E+1))" || fail "writer/current $(kv mysql/example/writer/current) want db-b|$((E+1))"
wait_for 20 "writer service -> db-b:3306" svc_is db-b:3306
wait_for 20 "switchover alert" alert_gt switchover "$A"
writes_through_proxysql_land_on db-b v3
wait_for 30 "v3 on db-a" row_on db-a v3
snap v3-switchover
pass "V3: switchover epoch $E->$(kv_epoch), key=$(master), service=$(svc), ProxySQL writer=$(px_writer), alert delivered"

log "=== V4: aborted switchover after the epoch bump (fault-injected SetMaster) ==="
E=$(kv_epoch); A=$(alert_n switchover-aborted)
for g in guard-a guard-b; do docker compose exec -T $g touch /tmp/fault-setmaster; done
docker compose exec -T guard-b mysql-guard switchover db-a --node=db-b --mysql-host=db-b "${FLAGS[@]}" && fail "faulted switchover reported success"
for g in guard-a guard-b; do docker compose exec -T $g rm -f /tmp/fault-setmaster; done
two_writers && fail "TWO WRITERS after aborted switchover"
check_roles db-b db-a
master_is db-b:3306 || fail "key changed by aborted switchover: $(master)"
[ "$(kv_epoch)" -ge "$((E+1))" ] || fail "epoch not bumped ($E -> $(kv_epoch))"
wait_for 30 "db-a replicating from db-b again" replica_running db-a
[ -z "$(kv mysql/example/guard/needs-rebuild/db-a)" ] || fail "needs-rebuild set on db-a"
wait_for 20 "switchover-aborted alert" alert_gt switchover-aborted "$A"
sleep 6; primary_is db-b || fail "primary moved after abort"
wait_for 30 "writer service still db-b" svc_is db-b:3306
writes_through_proxysql_land_on db-b v4
wait_for 30 "v4 on db-a" row_on db-a v4; wait_for 30 "GTIDs match after abort" gtids_match
snap v4-abort
pass "V4: aborted switchover rolled back: db-b writer, db-a replica, epoch $E->$(kv_epoch), alert delivered"

log "=== V5: switch back db-b -> db-a ==="
E=$(kv_epoch)
docker compose exec -T guard-b mysql-guard switchover db-a --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "switchback exit"
wait_for 20 "primary db-a" primary_is db-a
check_roles db-a db-b
master_is db-a:3306 || fail "key $(master)"
wait_for 20 "writer service -> db-a:3306" svc_is db-a:3306
writes_through_proxysql_land_on db-a v5
wait_for 30 "v5 on db-b" row_on db-b v5
pass "V5: switchback epoch $E->$(kv_epoch), service=$(svc)"

log "=== V6: kill primary db-a -> failover (service, ProxySQL, ntfy) ==="
wait_for 30 "GTIDs match before kill" gtids_match
E=$(kv_epoch); A=$(alert_n failover); M=$(alert_n proxysql_writer_mismatch)
docker compose stop db-a
T0=$(date +%s)
wait_for 90 "failover to db-b" primary_is db-b
log "failover after $(( $(date +%s) - T0 ))s"
[ "$(ro db-b)" = 00 ] || fail "db-b not writable"
master_is db-b:3306 || fail "key $(master)"
wait_for 20 "writer service -> db-b:3306" svc_is db-b:3306
wait_for 20 "failover alert" alert_gt failover "$A"
writes_through_proxysql_land_on db-b v6
docker compose start db-a
wait_for 120 "db-a back" sh -c "docker compose exec -T db-a mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"
wait_for 60 "db-a fenced read-only" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
docker compose exec -T guard-b mysql-guard rejoin db-b --node=db-a --mysql-host=db-a "${FLAGS[@]}" || fail "rejoin db-a"
wait_for 30 "db-a replicating" replica_running db-a
wait_for 30 "v6 on db-a" row_on db-a v6; wait_for 30 "GTIDs match" gtids_match
[ "$(alert_n proxysql_writer_mismatch)" = "$M" ] || fail "ProxySQL mismatch alert during normal failover (grace too short?)"
sleep 22
docker compose exec -T guard-b mysql-guard switchover db-a --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "switch back to db-a"
wait_for 20 "primary db-a" primary_is db-a
wait_for 20 "writer service -> db-a:3306" svc_is db-a:3306
writes_through_proxysql_land_on db-a v6b
snap v6-failover
pass "V6: failover in <=90s, key/service followed, ProxySQL writes ok, alert delivered, back on db-a"

log "=== V7: ProxySQL writer mismatch detected (read-only verify) ==="
M=$(alert_n proxysql_writer_mismatch); O=$(alert_n proxysql_writer_ok)
pxa "UPDATE mysql_servers SET status='OFFLINE_HARD' WHERE hostname='db-a'; LOAD MYSQL SERVERS TO RUNTIME"
wait_for 45 "proxysql_writer_mismatch alert" alert_gt proxysql_writer_mismatch "$M"
pxa "UPDATE mysql_servers SET status='ONLINE' WHERE hostname='db-a'; LOAD MYSQL SERVERS TO RUNTIME"
wait_for 45 "proxysql_writer_ok alert" alert_gt proxysql_writer_ok "$O"
primary_is db-a || fail "guard acted on a ProxySQL mismatch"
writes_through_proxysql_land_on db-a v7
pass "V7: mismatch alerted after grace and cleared; guard did not write ProxySQL or move the primary"

log "=== V8: foreign master-key write (repman/bash post-script) alerted ==="
F=$(alert_n foreign_master_write); C=$(alert_n foreign_master_write_cleared)
docker compose exec -T consul consul kv put mysql/example/guard/maintenance drill >/dev/null
docker compose exec -T consul consul kv put mysql/master/example db-b:3306 >/dev/null
wait_for 30 "foreign_master_write alert" alert_gt foreign_master_write "$F"
two_writers && fail "TWO WRITERS after foreign write"
docker compose exec -T consul consul kv put mysql/master/example db-a:3306 >/dev/null
wait_for 30 "foreign_master_write_cleared alert" alert_gt foreign_master_write_cleared "$C"
docker compose exec -T consul consul kv delete mysql/example/guard/maintenance >/dev/null
sleep 8; primary_is db-a || fail "primary not db-a after foreign write drill"
[ "$(ro db-b)" = 11 ] || fail "db-b writable after foreign write"
pass "V8: foreign write alerted and cleared, no action taken under maintenance"

SK=$(count_skipped); [ "$SK" = 0 ] || fail "$SK skipped action entries"
two_writers && fail "TWO WRITERS at end"
log "alerts delivered: $(alerts | sort | uniq -c | tr '\n' ';')"
log "ALL V0.3.0 DRILLS PASSED"
docker compose ps
