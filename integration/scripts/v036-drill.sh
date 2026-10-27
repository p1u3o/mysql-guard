#!/usr/bin/env bash
# v0.3.6 drills (requires Docker, curl, jq). Act mode, semi-sync, member service.
#  S0 baseline: semi-sync sync, writer/reader DNS
#  D3 replica isolated (db-b + guard-b off the network) -> no failover
#  L  replica SQL stopped, then lagging -> dropped from reader DNS, back after
#  D1 whole-host loss of the primary (db-a + guard-a killed) -> failover, rejoin, switch back
#  D2 primary partitioned (db-a + guard-a off the network) -> promote, fenced on return, clean rejoin
#  D4 witness lost + mysqld killed -> failover on the two db probe votes
#  D5 semi-sync failover under writes -> every acknowledged write is on the new primary; semi-sync restored
#  D5b semi-sync fell back to async -> semisync_async alert; host loss then blocked by the gate
# Every event: exactly the expected alerts, and writer./reader.<svc>.service.consul checked.
set -euo pipefail
export COMPOSE_FILE=docker-compose.yml:docker-compose.v030.yml:docker-compose.v036.yml
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT/integration"
for bin in docker curl jq; do command -v "$bin" >/dev/null 2>&1 || { echo "NOTICE: $bin missing; drills NOT executed"; exit 2; }; done
PROJ=${COMPOSE_PROJECT_NAME:?set COMPOSE_PROJECT_NAME}
NET="${PROJ}_default"
SNAPDIR="${SNAPDIR:-$ROOT/integration/snaps}"; mkdir -p "$SNAPDIR"
GUARDS=(9471 9472 9473)
FLAGS=(--cluster=example --peers=db-a,db-b --mode=act --mysql-user=root --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306 --master-format=legacy --writer-service=example-writer --ntfy-url=http://ntfy/mg-drill)
SVC=example-mysql

log() { echo "[$(date -u +%H:%M:%SZ)] $*"; }
fail() { log "FAIL: $*"; snap "zz-failure" || true; exit 1; }
pass() { log "PASS: $*"; }
sqlq() { docker compose exec -T "$1" mysql -uroot -pplaceholder -N -B -e "$2" 2>/dev/null; }
status() { curl -sf -m 5 "http://127.0.0.1:$1/status"; }
anystatus() { for p in 9473 9472 9471; do status "$p" && return 0; done; return 1; }
kv() { docker compose exec -T consul consul kv get "$1" 2>/dev/null || true; }
kv_epoch() { anystatus | jq -r .epoch; }
master() { kv mysql/master/example; }
primary_key_is() { [ "$(master)" = "$1:3306" ]; }
ro() { sqlq "$1" "SELECT CONCAT(@@global.read_only, @@global.super_read_only)"; }
gtid() { sqlq "$1" "SELECT REPLACE(@@global.gtid_executed, '\n', '')"; }
gtids_match() { local a b; a=$(gtid db-a); b=$(gtid db-b); [ -n "$a" ] && [ "$a" = "$b" ]; }
replica_running() { [ "$(sqlq "$1" "SELECT CONCAT((SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1),'/',(SELECT SERVICE_STATE FROM performance_schema.replication_applier_status LIMIT 1))")" = "ON/ON" ]; }
two_writers() { [ "$(ro db-a 2>/dev/null)" = 00 ] && [ "$(ro db-b 2>/dev/null)" = 00 ]; }
row_on() { [ "$(sqlq "$1" "SELECT COUNT(*) FROM drill.t WHERE v='$2'")" = 1 ]; }
px() { docker compose exec -T loader mysql -h proxysql -P6033 -uroot -pplaceholder -N -B -e "$1" 2>/dev/null; }
px_write() { px "INSERT INTO drill.t VALUES ('$1')"; }
pxa() { docker compose exec -T loader mysql -h proxysql -P6032 -uradmin -pplaceholder -N -B -e "$1" 2>/dev/null; }
px_writer_is() { [ "$(pxa "SELECT hostname FROM runtime_mysql_servers WHERE hostgroup_id=10 AND status='ONLINE'")" = "$1" ]; }
writes_land_on() { wait_for 60 "ProxySQL writer = $1" px_writer_is "$1"; wait_for 30 "write $2 via ProxySQL" px_write "$2"; row_on "$1" "$2" || fail "write $2 not on $1"; }
alerts() { curl -sf -m 5 "http://127.0.0.1:18093/mg-drill/json?poll=1&since=all" | jq -r '.title // empty'; }
alert_msgs() { curl -sf -m 5 "http://127.0.0.1:18093/mg-drill/json?poll=1&since=all" | jq -r 'select(.title=="mysql-guard example: '"$1"'") | .message'; }
alert_n() { alerts | grep -cx "mysql-guard example: $1" || true; }
alert_gt() { [ "$(alert_n "$1")" -gt "$2" ]; }
ev_begin() { EVN=$(alerts | wc -l | tr -d ' '); }
ev_check() { local lbl=$1; shift; sleep "${EV_WAIT:-70}"
  local got want; got=$(alerts | tail -n +$((EVN+1)) | sed 's/^mysql-guard example: //' | sort | tr '\n' ' ' | sed 's/ $//')
  want=$(printf '%s\n' "$@" | sed '/^$/d' | sort | tr '\n' ' ' | sed 's/ $//')
  [ "$got" = "$want" ] || fail "$lbl: alerts [$got], want exactly [$want]"
  log "$lbl: alerts exactly [$got]"; }
wait_for() { local t="$1" d="$2"; shift 2; for _ in $(seq 1 "$t"); do if "$@" >/dev/null 2>&1; then return 0; fi; sleep 1; done; fail "timeout after ${t}s waiting for: $d"; }
snap() { local s="$1"
  for p in "${GUARDS[@]}"; do curl -s -m 5 "http://127.0.0.1:$p/status" > "$SNAPDIR/$s-$p-status.json" || true; done
  for d in db-a db-b; do sqlq "$d" "SELECT @@hostname,@@read_only,@@super_read_only,@@gtid_executed; SHOW GLOBAL STATUS LIKE 'Rpl_semi%'; SHOW REPLICA STATUS\\G" > "$SNAPDIR/$s-$d-mysql.txt" || true; done
  curl -s -m 5 "http://127.0.0.1:8500/v1/health/service/$SVC" > "$SNAPDIR/$s-members.json" || true
  docker compose exec -T consul consul kv export > "$SNAPDIR/$s-consul-kv.json" 2>/dev/null || true; }
# DNS: SRV targets (service addresses are host names) for <tag>.<svc>.service.consul
dns() { docker compose exec -T loader dig +short +time=2 +tries=1 @consul -p 8600 SRV "$1.$SVC.service.consul" 2>/dev/null | awk '{print $4}' | sed 's/\.$//; s/\..*$//' | sort | tr '\n' ' ' | sed 's/ $//'; }
dns_is() { [ "$(dns "$1")" = "$2" ]; }
dns_check() { # <label> <writer> <readers, space-separated or empty>
  wait_for 45 "$1: writer.$SVC -> [$2]" dns_is writer "$2"
  wait_for 45 "$1: reader.$SVC -> [$3]" dns_is reader "$3"
  [ "$(curl -sf "http://127.0.0.1:8500/v1/catalog/service/example-writer" | jq -r '.[0].ServiceAddress')" = "$2" ] || fail "$1: example-writer service not $2"
  log "$1: DNS writer=[$(dns writer)] reader=[$(dns reader)]"; }
member() { curl -sf "http://127.0.0.1:8500/v1/health/service/$SVC" | jq -r --arg n "$1" '.[] | select(.Service.Meta.node==$n) | "\(.Service.Tags|join(","))/\(.Checks[]|select(.CheckID|endswith(":mysql"))|.Status)/\(.Service.Meta.epoch)"'; }
member_is() { [ "$(member "$1")" = "$2" ]; }
semi() { sqlq "$1" "SELECT CONCAT(@@global.rpl_semi_sync_source_enabled,'/',@@global.rpl_semi_sync_replica_enabled,'/',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Rpl_semi_sync_source_status'),'/',(SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Rpl_semi_sync_source_clients'))"; }
semi_sync_on() { [ "$(semi "$1")" = "1/0/ON/1" ]; }  # source enabled, sync, one replica acking
host_kill() { docker compose kill "db-$1" "guard-$1" >/dev/null; }
host_start() { docker compose start "db-$1" "guard-$1" >/dev/null; wait_for 120 "db-$1 up" sh -c "docker compose exec -T db-$1 mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"; }
net_cut() { for c in "db-$1" "guard-$1"; do docker network disconnect "$NET" "${PROJ}-$c-1"; done; }
net_heal() { for c in "db-$1" "guard-$1"; do docker network connect --alias "$c" "$NET" "${PROJ}-$c-1"; done; }
primary_status_is() { [ "$(anystatus | jq -r .primary)" = "$1" ]; }
rejoin() { docker compose exec -T "guard-$2" mysql-guard rejoin "db-$2" --node="db-$1" --mysql-host="db-$1" "${FLAGS[@]}" || fail "rejoin db-$1 to db-$2"; wait_for 60 "db-$1 replicating" replica_running "db-$1"; wait_for 60 "GTIDs match" gtids_match; }
switch_to() { docker compose exec -T "guard-$2" mysql-guard switchover "db-$1" --node="db-$2" --mysql-host="db-$2" "${FLAGS[@]}" || fail "switchover to db-$1"; wait_for 30 "primary db-$1" primary_key_is "db-$1"; }
# writes every second through ProxySQL; acknowledged ids are logged (id per line)
load_start() { docker compose exec -T -d loader bash -c "echo \$\$ > /tmp/load.pid; : > /tmp/acked-$1; while :; do id=\$(mysql -h proxysql -P6033 -uroot -pplaceholder -N -B -e \"INSERT INTO drill.load (tag) VALUES ('$1'); SELECT LAST_INSERT_ID()\" 2>/dev/null) && echo \$id >> /tmp/acked-$1; sleep 1; done"; sleep 2; }
load_stop() { docker compose exec -T loader bash -c 'kill $(cat /tmp/load.pid) 2>/dev/null; rm -f /tmp/load.pid' || true; }
acked_missing_on() { # <node> <tag> -> prints count of acknowledged ids missing on node
  local ids; ids=$(docker compose exec -T loader cat "/tmp/acked-$2" | tr '\n' ',' | sed 's/,$//')
  [ -n "$ids" ] || { echo none; return; }
  local n; n=$(docker compose exec -T loader cat "/tmp/acked-$2" | wc -l | tr -d ' ')
  echo $(( n - $(sqlq "$1" "SELECT COUNT(*) FROM drill.load WHERE id IN ($ids)") )); }
back_to_a() { # rejoin db-a under db-b and switch back; one switchover alert, DNS restored
  ev_begin
  rejoin a b
  dns_check "$1 rejoined" db-b db-a
  semi_sync_on db-b || fail "$1: semi-sync not restored on db-b after rejoin ($(semi db-b))"
  sleep 25
  switch_to a b
  writes_land_on db-a "$1-back"
  dns_check "$1 switched back" db-a db-b
  wait_for 30 "$1: semi-sync on db-a" semi_sync_on db-a
  ev_check "$1 rejoin+switchback" switchover; }

log "Build and start (project $PROJ)"
docker compose build guard-a guard-b guard-witness loader
docker compose up -d ntfy consul db-a db-b proxysql loader
wait_for 120 "db-a healthy" sh -c "docker compose exec -T db-a mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"
wait_for 120 "db-b ready" sh -c "docker compose exec -T db-b mysql -uroot -pplaceholder -e 'SELECT 1'"
cfg_repl() { sqlq db-b "STOP REPLICA; RESET BINARY LOGS AND GTIDS; SET GLOBAL rpl_semi_sync_replica_enabled=ON; CHANGE REPLICATION SOURCE TO SOURCE_HOST='db-a', SOURCE_USER='repl', SOURCE_PASSWORD='placeholder', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1, SOURCE_SSL=1, SOURCE_HEARTBEAT_PERIOD=2; START REPLICA; SET GLOBAL super_read_only=ON;"; }
wait_for 60 "replication on db-b" cfg_repl
wait_for 60 "db-b replicating" replica_running db-b
sqlq db-a "SET GLOBAL rpl_semi_sync_source_timeout=10000; SET GLOBAL rpl_semi_sync_source_enabled=ON; CREATE DATABASE IF NOT EXISTS drill; CREATE TABLE IF NOT EXISTS drill.t (v VARCHAR(64) PRIMARY KEY); CREATE TABLE IF NOT EXISTS drill.load (id BIGINT AUTO_INCREMENT PRIMARY KEY, tag VARCHAR(16), ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
sqlq db-b "SET GLOBAL rpl_semi_sync_source_timeout=10000"
wait_for 60 "GTIDs match" gtids_match
docker compose exec -T consul consul kv put mysql/master/example db-a:3306 >/dev/null
docker compose up -d guard-a guard-b guard-witness
wait_for 90 "guards up, primary db-a" primary_status_is db-a

log "=== S0: baseline ==="
ev_begin
wait_for 30 "semi-sync sync on db-a" semi_sync_on db-a
writes_land_on db-a s0
dns_check S0 db-a db-b
member_is db-a "writer/passing/$(kv_epoch)" || fail "S0 member db-a: $(member db-a)"
member_is db-b "reader/passing/$(kv_epoch)" || fail "S0 member db-b: $(member db-b)"
[ "$(anystatus | jq -r '.nodes[]? | select(.node=="witness-1") | .primary_probe // empty')" != dead ] || fail "witness says dead"
EV_WAIT=10 ev_check S0
snap s0; pass "S0: act mode, semi-sync sync, writer/reader DNS correct"

log "=== D3: replica isolated -> no failover ==="
ev_begin; E=$(kv_epoch); load_start d3
net_cut b
wait_for 30 "D3: db-b out of reader DNS" dns_is reader ""
sleep 60
primary_key_is db-a || fail "D3: primary moved to $(master)"; [ "$(kv_epoch)" = "$E" ] || fail "D3: epoch moved"
[ "$(ro db-a)" = 00 ] || fail "D3: db-a not writable"
dns_check "D3 isolated" db-a ""
net_heal b
wait_for 60 "D3 db-b replicating" replica_running db-b; wait_for 60 "GTIDs match" gtids_match
load_stop
dns_check "D3 healed" db-a db-b
wait_for 60 "D3: semi-sync back to sync" semi_sync_on db-a
[ "$(acked_missing_on db-b d3)" = 0 ] || fail "D3: acked writes missing on db-b"
EV_WAIT=20 ev_check D3 semisync_async
snap d3; pass "D3: replica isolated 85s+: no failover, epoch $E, db-b left and rejoined reader DNS"

log "=== L: stopped and lagging replica leave reader DNS, return after ==="
ev_begin
sqlq db-b "STOP REPLICA SQL_THREAD"
wait_for 30 "L: stopped db-b out of reader DNS" dns_is reader ""
member_is db-b "fenced/critical/$(kv_epoch)" || fail "L: member db-b $(member db-b)"
sqlq db-b "START REPLICA SQL_THREAD"
wait_for 60 "L: db-b back to reader" member_is db-b "reader/passing/$(kv_epoch)"
# lag without a stall: continuous writes, delayed apply (the gap stays ~constant, so the stall
# detector fires only after its window; lagging must show before that)
sqlq db-b "STOP REPLICA SQL_THREAD; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=15; START REPLICA SQL_THREAD"
load_start lag
wait_for 40 "L: lagging db-b warning" member_is db-b "lagging/warning/$(kv_epoch)"
dns_is reader "" || fail "L: lagging replica still in reader DNS"
load_stop
sqlq db-b "STOP REPLICA SQL_THREAD; CHANGE REPLICATION SOURCE TO SOURCE_DELAY=0; START REPLICA SQL_THREAD"
wait_for 60 "GTIDs match" gtids_match
dns_check "L recovered" db-a db-b
EV_WAIT=10 ev_check L
snap lag; pass "L: stopped (critical) and lagging (warning) replica left reader DNS and came back, no alerts below the delays"

log "=== D1: whole-host loss of the primary (db-a + guard-a) ==="
ev_begin; E=$(kv_epoch)
host_kill a
wait_for 120 "D1 failover to db-b" primary_key_is db-b
epoch_gt() { [ "$(kv_epoch)" -gt "$1" ]; }; wait_for 20 "D1 epoch bumped" epoch_gt "$E"
writes_land_on db-b d1
dns_check "D1 failed over" db-b ""
alert_msgs failover | tail -1 | grep -q 'votes: .*witness' || fail "D1: failover alert lacks witness vote: $(alert_msgs failover | tail -1)"
alert_msgs failover | tail -1 | grep -q 'semi-sync at loss: sync' || fail "D1: failover alert lacks semi-sync state"
[ "$(semi db-b | cut -d/ -f1)" = 0 ] || fail "D1: new primary kept semi-sync source ON without a replica ($(semi db-b))"
host_start a
wait_for 60 "D1 db-a fenced" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
two_writers && fail "D1 TWO WRITERS"
dns_check "D1 old primary back, fenced" db-b ""
member_is db-a "fenced/critical/$(kv_epoch)" || fail "D1 member db-a $(member db-a)"
EV_WAIT=20 ev_check "D1 failover" failover
snap d1; back_to_a D1
pass "D1: whole-host loss failed over (witness+probe+io votes), old primary fenced and out of DNS, rejoined, semi-sync restored"

log "=== D2: primary partitioned -> promote, fenced on return, clean rejoin ==="
ev_begin; E=$(kv_epoch); load_start d2
net_cut a
wait_for 120 "D2 failover to db-b" primary_key_is db-b
writes_land_on db-b d2
dns_check "D2 partitioned" db-b ""
sleep 10; load_stop
net_heal a
wait_for 90 "D2 db-a fenced on return" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
two_writers && fail "D2 TWO WRITERS"
dns_check "D2 healed, db-a fenced" db-b ""
[ "$(acked_missing_on db-b d2)" = 0 ] || fail "D2: acked writes missing on db-b"
EV_WAIT=20 ev_check "D2 failover" failover
snap d2; back_to_a D2
pass "D2: partitioned primary replaced, fenced on return, rejoined cleanly"

log "=== D4: witness lost + mysqld killed ==="
docker compose stop guard-witness >/dev/null
sleep 10; ev_begin
docker compose kill db-a >/dev/null
wait_for 120 "D4 failover to db-b" primary_key_is db-b
alert_msgs failover | tail -1 | grep -q 'db-a:probe' || fail "D4: expected db-a probe vote: $(alert_msgs failover | tail -1)"
writes_land_on db-b d4
dns_check "D4 failed over" db-b ""
docker compose start db-a guard-witness >/dev/null
wait_for 120 "db-a up" sh -c "docker compose exec -T db-a mysqladmin ping -h 127.0.0.1 -pplaceholder --silent"
wait_for 60 "D4 db-a fenced" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
EV_WAIT=20 ev_check "D4 failover" failover
snap d4; back_to_a D4
pass "D4: no witness, mysqld killed: failed over on two db probe votes"

log "=== D5: semi-sync failover under writes, no acknowledged write lost ==="
wait_for 30 "semi-sync on db-a" semi_sync_on db-a
ev_begin; load_start d5; sleep 15
host_kill a
wait_for 120 "D5 failover to db-b" primary_key_is db-b
sleep 10; load_stop
M=$(acked_missing_on db-b d5); [ "$M" = 0 ] || fail "D5: $M acknowledged writes lost"
N=$(docker compose exec -T loader sh -c 'wc -l < /tmp/acked-d5')
alert_msgs failover | tail -1 | grep -q 'semi-sync at loss: sync' || fail "D5: alert lacks semi-sync state"
dns_check "D5 failed over" db-b ""
host_start a
wait_for 60 "D5 db-a fenced" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
EV_WAIT=20 ev_check "D5 failover" failover
snap d5; back_to_a D5
pass "D5: failover under ~1 write/s, all $N acknowledged writes on db-b, semi-sync restored after rejoin"

log "=== D5b: semi-sync fell back to async -> alert, then host loss blocked by the gate ==="
ev_begin; E=$(kv_epoch)
sqlq db-b "SET GLOBAL rpl_semi_sync_replica_enabled=OFF; STOP REPLICA IO_THREAD; START REPLICA IO_THREAD"
px_write d5b-async || true
wait_for 30 "db-a async" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e \"SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Rpl_semi_sync_source_status'\" 2>/dev/null)\" = OFF ]"
EV_WAIT=75 ev_check "D5b fallback" semisync_async
wait_for 30 "GTIDs match" gtids_match
ev_begin
host_kill a
sleep 75
primary_key_is db-a || fail "D5b: failover happened despite the semi-sync gate"
[ "$(kv_epoch)" = "$E" ] || fail "D5b epoch moved"
EV_WAIT=0 ev_check "D5b gate" failover_blocked
alert_msgs failover_blocked | tail -1 | grep -q 'semi-sync' || fail "D5b blocked alert text"
host_start a
# a restarted mysqld takes semi-sync from its config (my.cnf in prod); the drill runs it from flags, so set it as my.cnf would
sqlq db-a "SET GLOBAL rpl_semi_sync_source_enabled=ON"
sqlq db-b "SET GLOBAL rpl_semi_sync_replica_enabled=ON; STOP REPLICA IO_THREAD; START REPLICA IO_THREAD"
wait_for 60 "D5b semi-sync back" semi_sync_on db-a
writes_land_on db-a d5b-after
dns_check "D5b restored" db-a db-b
snap d5b; pass "D5b: async fallback alerted once; host loss while async was refused by the gate (one alert)"

log "=== D6 (v0.3.7): primary mysqld killed and restarted read-only at once -> restore_writer, one alert ==="
ev_begin; E=$(kv_epoch)
# the test pair's start.sh brings mysqld up read-only; emulate it for this one restart
sqlq db-a "SET PERSIST_ONLY read_only=ON; SET PERSIST_ONLY super_read_only=ON"
docker compose kill db-a >/dev/null; docker compose start db-a >/dev/null
wait_for 60 "D6 db-a up read-only" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT @@global.super_read_only' 2>/dev/null)\" = 1 ]"
wait_for 120 "D6 db-a writable again" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT @@global.super_read_only' 2>/dev/null)\" = 0 ]"
primary_key_is db-a || fail "D6: failed over instead of restoring db-a"
sqlq db-a "RESET PERSIST read_only; RESET PERSIST super_read_only"
sqlq db-a "SET GLOBAL rpl_semi_sync_source_enabled=ON"
sqlq db-b "STOP REPLICA IO_THREAD; START REPLICA IO_THREAD"
writes_land_on db-a d6
two_writers && fail "D6 TWO WRITERS"
dns_check "D6 restored" db-a db-b
EV_WAIT=40 ev_check "D6 restore" restore_writer
snap d6; pass "D6: restarted read-only primary made writable again; replica repl_* held (one alert)"

log "=== D7 (v0.3.7): primary mysqld stopped and kept down (guard-a up) -> failover, one alert ==="
ev_begin; E=$(kv_epoch)
docker compose stop db-a >/dev/null
wait_for 120 "D7 failover to db-b" primary_key_is db-b
writes_land_on db-b d7
dns_check "D7 failed over" db-b ""
two_writers && fail "D7 TWO WRITERS"
docker compose start db-a >/dev/null
wait_for 60 "D7 db-a fenced" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e 'SELECT CONCAT(@@global.read_only,@@global.super_read_only)' 2>/dev/null)\" = 11 ]"
EV_WAIT=20 ev_check "D7 failover" failover
snap d7; back_to_a D7
pass "D7: mysqld kept down failed over with one alert, rejoined, switched back"

two_writers && fail "TWO WRITERS at end"
SK=$(curl -sf "http://127.0.0.1:9471/api/actions" | jq '[.[] | select(.result=="skipped")] | length'); [ "$SK" = 0 ] || fail "$SK skipped actions"
log "alerts delivered: $(alerts | sort | uniq -c | tr '\n' ';')"
log "ALL V0.3.6 DRILLS PASSED (incl. v0.3.7 D6/D7)"
