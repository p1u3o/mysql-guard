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
count_ok() { actions | jq --arg a "$1" --arg s "${2:-end}" '[.[] | select(.action==$a and .step==$s and .result=="ok")] | length'; }
count_skipped() { actions | jq '[.[] | select(.result=="skipped")] | length'; }

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
# per-event alert accounting: ev_begin before an event; ev_check "<expected titles>" waits past the
# transition grace + alert delay (70s) and requires the new alerts to be exactly the expected list.
ev_begin() { EVN=$(alerts | wc -l | tr -d ' '); }
ev_check() { # <label> <expected codes, space-separated>
  local lbl=$1; shift; sleep "${EV_WAIT:-70}"
  local got want; got=$(alerts | tail -n +$((EVN+1)) | sed 's/^mysql-guard example: //' | sort | tr '\n' ' ' | sed 's/ $//')
  want=$(printf '%s\n' "$@" | sort | tr '\n' ' ' | sed 's/ $//')
  [ "$got" = "$want" ] || fail "$lbl: alerts [$got], want exactly [$want]"
  log "$lbl: alerts exactly [$got]"
}
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
ev_begin
E=$(kv_epoch); A=$(alert_n switchover)
docker compose exec -T guard-a mysql-guard switchover db-b --dry-run=false --node=db-a --mysql-host=db-a "${FLAGS[@]}" || fail "switchover exit"
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
ev_check V3 switchover
pass "V3: switchover epoch $E->$(kv_epoch), key=$(master), service=$(svc), ProxySQL writer=$(px_writer), alert delivered"

log "=== V4: aborted switchover after the epoch bump (fault-injected SetMaster) ==="
ev_begin
E=$(kv_epoch); A=$(alert_n switchover-aborted)
for g in guard-a guard-b; do docker compose exec -T $g touch /tmp/fault-setmaster; done
docker compose exec -T guard-b mysql-guard switchover db-a --dry-run=false --node=db-b --mysql-host=db-b "${FLAGS[@]}" && fail "faulted switchover reported success"
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
key_repaired() { [ "$(kv mysql/example/writer/current)" = "db-b|$(kv_epoch)" ] && [ -z "$(kv mysql/example/guard/key-repair)" ]; }
wait_for 30 "writer/current repaired to the new epoch" key_repaired
wait_for 30 "v4 on db-a" row_on db-a v4; wait_for 30 "GTIDs match after abort" gtids_match
snap v4-abort
ev_check V4 switchover-aborted
pass "V4: aborted switchover rolled back: db-b writer, db-a replica, epoch $E->$(kv_epoch), alert delivered"

log "=== V5: switch back db-b -> db-a ==="
ev_begin
E=$(kv_epoch)
docker compose exec -T guard-b mysql-guard switchover db-a --dry-run=false --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "switchback exit"
wait_for 20 "primary db-a" primary_is db-a
check_roles db-a db-b
master_is db-a:3306 || fail "key $(master)"
wait_for 20 "writer service -> db-a:3306" svc_is db-a:3306
writes_through_proxysql_land_on db-a v5
wait_for 30 "v5 on db-b" row_on db-b v5
ev_check V5 switchover
pass "V5: switchback epoch $E->$(kv_epoch), service=$(svc)"

log "=== V6: kill primary db-a -> failover (service, ProxySQL, ntfy) ==="
wait_for 30 "GTIDs match before kill" gtids_match
ev_begin
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
docker compose exec -T guard-b mysql-guard switchover db-a --dry-run=false --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "switch back to db-a"
wait_for 20 "primary db-a" primary_is db-a
wait_for 20 "writer service -> db-a:3306" svc_is db-a:3306
writes_through_proxysql_land_on db-a v6b
snap v6-failover
ev_check "V6 (kill+rejoin+switchback)" failover switchover
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

log "=== V9: switchover under continuous writes (both ways, v0.3.1 fence/catch-up) ==="
LOADTAB="CREATE TABLE IF NOT EXISTS drill.load (id BIGINT AUTO_INCREMENT PRIMARY KEY, tag VARCHAR(16), ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
px "$LOADTAB" || sqlq db-a "$LOADTAB"
load_start() { # 3 writer loops, single-row commits through ProxySQL (target 20-50/s total)
  for i in 1 2 3 4; do
    docker compose exec -T -d db-b bash -c "echo \$\$ >> /tmp/load.pid; while :; do mysql -h proxysql -P6033 -uroot -pplaceholder -N -B -e \"INSERT INTO drill.load (tag) VALUES ('$1')\" 2>/dev/null; sleep 0.1; done"
  done
  sleep 1
}
load_stop() { docker compose exec -T db-b bash -c 'kill $(cat /tmp/load.pid) 2>/dev/null; rm -f /tmp/load.pid' || true; }
load_rate() { sqlq "$1" "SELECT ROUND(COUNT(*)/GREATEST(TIMESTAMPDIFF(MICROSECOND,MIN(ts),MAX(ts))/1e6,1),1) FROM drill.load WHERE tag='$2'"; }
for dir in "db-a db-b guard-a" "db-b db-a guard-b"; do
  set -- $dir; FROM=$1; TO=$2; G=$3; TAG="v9${TO#db-}"
  ev_begin
  E=$(kv_epoch); A=$(alert_n switchover); AB=$(alert_n switchover-aborted); ER=$(alert_n errant_gtid)
  load_start "$TAG"; sleep 5
  docker compose exec -T "$G" mysql-guard switchover "$TO" --dry-run=false --node="$FROM" --mysql-host="$FROM" "${FLAGS[@]}" || { load_stop; fail "switchover $FROM->$TO under load"; }
  sleep 5; load_stop
  wait_for 20 "primary $TO" primary_is "$TO"
  check_roles "$TO" "$FROM"
  two_writers && fail "TWO WRITERS after switchover under load"
  master_is "$TO:3306" || fail "key $(master)"; cur_is "$TO|$((E+1))" || fail "writer/current $(kv mysql/example/writer/current)"
  wait_for 20 "writer service -> $TO:3306" svc_is "$TO:3306"
  wait_for 20 "switchover alert" alert_gt switchover "$A"
  [ "$(alert_n switchover-aborted)$(alert_n errant_gtid)" = "$AB$ER" ] || fail "abort/errant alert under load"
  wait_for 30 "GTIDs match after load" gtids_match
  RATE=$(load_rate "$TO" "$TAG"); N=$(sqlq "$TO" "SELECT COUNT(*) FROM drill.load WHERE tag='$TAG'")
  [ "$(sqlq "$FROM" "SELECT COUNT(*) FROM drill.load WHERE tag='$TAG'")" = "$N" ] || fail "row count differs"
  awk -v r="$RATE" 'BEGIN{exit !(r>=20 && r<=50)}' || fail "load rate $RATE/s outside 20-50"
  writes_through_proxysql_land_on "$TO" "$TAG-after"
  snap "v9-$FROM-$TO"
  ev_check "V9 $FROM->$TO" switchover
  pass "V9: switchover $FROM->$TO under ~${RATE} writes/s ($N rows, identical on both), epoch $E->$(kv_epoch), no errant/abort alert"
done

log "=== V11a: no transient alerts from planned switchovers / failover (V3-V9) ==="
for c in repl_io_stopped repl_sql_stopped primary_read_only read_only_drift repl_state_unknown repl_lag repl_stalled fence; do
  [ "$(alert_n $c)" = 0 ] || fail "transient alert $c sent during V3-V9: $(alert_n $c)"
done
log "alerts so far: $(alerts | sort | uniq -c | tr '\n' ';')"
pass "V11a: each switchover/failover sent exactly one alert; 0 thread/read_only/lag/unknown/fence alerts"

log "=== V10: catch-up timeout -> clean abort, source writable, epoch unchanged ==="
primary_is db-a || fail "precondition primary db-a"
E=$(kv_epoch); AB=$(alert_n switchover-aborted); S=$(alert_n switchover)
sqlq db-b "STOP REPLICA SQL_THREAD"
write_row db-a v10-lag
docker compose exec -T guard-a mysql-guard switchover db-b --dry-run=false --node=db-a --mysql-host=db-a "${FLAGS[@]}" --switchover-catchup-timeout=3s && fail "switchover with lagging target reported success"
check_roles db-a db-b
two_writers && fail "TWO WRITERS after catch-up abort"
[ "$(kv_epoch)" = "$E" ] || fail "epoch bumped by catch-up abort ($E -> $(kv_epoch))"
master_is db-a:3306 || fail "key changed $(master)"; cur_is "db-a|$E" || fail "writer/current $(kv mysql/example/writer/current)"
wait_for 20 "switchover-aborted alert" alert_gt switchover-aborted "$AB"
[ "$(alert_n switchover-aborted)" = "$((AB+1))" ] || fail "expected exactly one abort alert"
[ "$(alert_n switchover)" = "$S" ] || fail "switchover alert sent for aborted switchover"
wait_for 20 "ProxySQL writer back to db-a" px_writer_is db-a
writes_through_proxysql_land_on db-a v10-after
sqlq db-b "START REPLICA SQL_THREAD"
wait_for 30 "v10 rows on db-b" row_on db-b v10-after; wait_for 30 "GTIDs match" gtids_match
sleep 8; primary_is db-a || fail "primary moved after abort"
snap v10-catchup-abort
pass "V10: catch-up timeout aborted cleanly: db-a writable, epoch $E unchanged, ProxySQL restored, one abort alert"

log "=== V11b: a real replica IO stop still alerts exactly once (after the hold delay) ==="
primary_is db-a || fail "precondition primary db-a"
RIO=$(alert_n repl_io_stopped)
sqlq db-b "STOP REPLICA IO_THREAD"
sleep 20; [ "$(alert_n repl_io_stopped)" = "$RIO" ] || fail "IO alert before hold delay"
wait_for 90 "repl_io_stopped alert" alert_gt repl_io_stopped "$RIO"
sleep 15; [ "$(alert_n repl_io_stopped)" = "$((RIO+1))" ] || fail "expected exactly one repl_io_stopped alert, got $(( $(alert_n repl_io_stopped) - RIO ))"
sqlq db-b "START REPLICA IO_THREAD"; wait_for 30 "db-b replicating" replica_running db-b; wait_for 30 "GTIDs match" gtids_match
pass "V11b: persistent IO stop alerted once after the delay"

log "=== V11c: a lasting read-only primary still alerts exactly once (maintenance on, no action) ==="
docker compose exec -T consul consul kv put mysql/example/guard/maintenance "v11c" >/dev/null
sleep 6; ev_begin
sqlq db-a "SET GLOBAL super_read_only=ON"
sleep 20; [ "$(alerts | wc -l | tr -d ' ')" = "$EVN" ] || fail "alert before hold delay"
EV_WAIT=55 ev_check V11c primary_read_only
sqlq db-a "SET GLOBAL super_read_only=OFF; SET GLOBAL read_only=OFF"
docker compose exec -T consul consul kv delete mysql/example/guard/maintenance >/dev/null
writes_through_proxysql_land_on db-a v11c; wait_for 30 "GTIDs match" gtids_match
pass "V11c: persistent read-only primary alerted once after the delay, no action under maintenance"

log "=== V12 (v0.3.4): kill primary, old primary left fenced: one failover alert, held in the window, then alerts once ==="
primary_is db-a || fail "precondition primary db-a"
wait_for 30 "GTIDs match before V12 kill" gtids_match
FFW_S=180
ev_begin
docker compose stop db-a
wait_for 90 "V12 failover to db-b" primary_is db-b
two_writers && fail "TWO WRITERS after V12 failover"
master_is db-b:3306 || fail "V12 key $(master)"
wait_for 20 "V12 writer service -> db-b:3306" svc_is db-b:3306
writes_through_proxysql_land_on db-b v12
docker compose start db-a
wait_for 120 "V12 db-a back" sh -c "docker compose exec -T db-a mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"
wait_for 60 "V12 db-a fenced read-only" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
replica_running db-a && fail "V12 precondition: db-a should not be replicating before rejoin"
TEND=$(kv mysql/example/guard/transition | jq -r '.ended | sub("\\.[0-9]+";"") | fromdate')
[ "$(kv mysql/example/guard/transition | jq -r .action)" = failover ] || fail "V12 transition marker not failover"
# V12a: well past delay+grace (70s) but inside the window: exactly one failover alert, nothing for db-a.
W=$(( TEND + 90 - $(date +%s) )); [ "$W" -gt 0 ] && sleep "$W"
[ "$(date +%s)" -lt $(( TEND + FFW_S - 10 )) ] || fail "V12a check ran outside the window"
EV_WAIT=0 ev_check "V12a (kill, fenced, in window)" failover
two_writers && fail "TWO WRITERS in V12 window"
# V12b: past window + delay + margin: the fenced node's replication alerts arrive once.
ev_begin
W=$(( TEND + FFW_S + 45 + 25 - $(date +%s) )); [ "$W" -gt 0 ] && sleep "$W"
NEW=$(alerts | tail -n +$((EVN+1)) | sed 's/^mysql-guard example: //' | sort)
[ -n "$NEW" ] || fail "V12b: fenced node past the window did not alert"
[ -z "$(echo "$NEW" | grep -vxE 'repl_io_stopped|repl_sql_stopped|read_only_drift|repl_state_unknown|repl_lag|repl_stalled')" ] || fail "V12b: unexpected alerts [$NEW]"
[ -z "$(echo "$NEW" | uniq -d)" ] || fail "V12b: duplicate alerts [$NEW]"
N1=$(alerts | wc -l | tr -d ' '); sleep 30
[ "$(alerts | wc -l | tr -d ' ')" = "$N1" ] || fail "V12b: fenced node alerted more than once"
log "V12b: past-window alerts exactly once: [$(echo $NEW)]"
# restore: rejoin db-a, switch back, no extra alert from the rejoin
ev_begin
docker compose exec -T guard-b mysql-guard rejoin db-b --node=db-a --mysql-host=db-a "${FLAGS[@]}" || fail "V12 rejoin db-a"
wait_for 30 "V12 db-a replicating" replica_running db-a
wait_for 30 "v12 on db-a" row_on db-a v12; wait_for 30 "V12 GTIDs match" gtids_match
sleep 22
docker compose exec -T guard-b mysql-guard switchover db-a --dry-run=false --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "V12 switch back to db-a"
wait_for 20 "V12 primary db-a" primary_is db-a
wait_for 20 "V12 writer service -> db-a:3306" svc_is db-a:3306
writes_through_proxysql_land_on db-a v12b
ev_check "V12c (rejoin+switchback)" switchover
pass "V12: kill-primary gave one failover alert and nothing for the fenced node in the window; past the window it alerted once; rejoin silent"

SK=$(count_skipped); [ "$SK" = 0 ] || fail "$SK skipped action entries"
two_writers && fail "TWO WRITERS at end"
log "alerts delivered: $(alerts | sort | uniq -c | tr '\n' ';')"
log "ALL V0.3.x DRILLS PASSED"
docker compose ps
