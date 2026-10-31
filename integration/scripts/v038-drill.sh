#!/usr/bin/env bash
# v0.3.8 drills on the Docker pair (requires Docker, curl, jq): semi-sync stall handling with a real
# mysqld stop, and switchover failure paths. Same stack as v036-drill.sh with -semisync-stall on.
#  A1 replica mysqld stopped: semi-sync source turned off, commit stall measured
#  A2 replica mysqld started: source turned on again once caught up, a client registered
#  A7 replica started with its replica variable OFF at runtime: the guard keeps the source off
#  B7 switchover where the target STOP REPLICA is refused (privilege): rolled back, one writer
#  B8 switchover with a ProxySQL writer hostgroup that does not match
#  A8 primary host lost while the stall state holds: failover as before, fresh record
set -euo pipefail
export COMPOSE_FILE=docker-compose.yml:docker-compose.v030.yml:docker-compose.v036.yml:docker-compose.v038.yml
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
switch_to() { docker compose exec -T "guard-$2" mysql-guard switchover "db-$1" --dry-run=false --node="db-$2" --mysql-host="db-$2" "${FLAGS[@]}" || fail "switchover to db-$1"; wait_for 30 "primary db-$1" primary_key_is "db-$1"; }
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
cfg_repl() { sqlq db-b "STOP REPLICA; RESET BINARY LOGS AND GTIDS; SET GLOBAL rpl_semi_sync_replica_enabled=ON; CHANGE REPLICATION SOURCE TO SOURCE_HOST='db-a', SOURCE_USER='repl', SOURCE_PASSWORD='placeholder', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1, SOURCE_SSL=1, SOURCE_HEARTBEAT_PERIOD=2; START REPLICA; SET PERSIST read_only=ON; SET PERSIST super_read_only=ON;"; }
wait_for 60 "replication on db-b" cfg_repl
wait_for 60 "db-b replicating" replica_running db-b
sqlq db-a "SET GLOBAL rpl_semi_sync_source_timeout=10000; SET GLOBAL rpl_semi_sync_source_enabled=ON; CREATE DATABASE IF NOT EXISTS drill; CREATE TABLE IF NOT EXISTS drill.t (v VARCHAR(64) PRIMARY KEY); CREATE TABLE IF NOT EXISTS drill.load (id BIGINT AUTO_INCREMENT PRIMARY KEY, tag VARCHAR(16), ts TIMESTAMP(3) DEFAULT CURRENT_TIMESTAMP(3))"
sqlq db-b "SET GLOBAL rpl_semi_sync_source_timeout=10000"
wait_for 60 "GTIDs match" gtids_match
docker compose exec -T consul consul kv put mysql/master/example db-a:3306 >/dev/null
docker compose up -d guard-a guard-b guard-witness
wait_for 90 "guards up, primary db-a" primary_status_is db-a


# ---- helpers for the v0.3.8 cases ----
src_is() { [ "$(sqlq "$1" "SELECT @@global.rpl_semi_sync_source_enabled")" = "$2" ]; }
clients() { sqlq "$1" "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME='Rpl_semi_sync_source_clients'"; }
clients_is() { [ "$(clients "$1")" = "$2" ]; }
ssrec() { kv mysql/example/guard/semisync; }
actions_api() { for p in 9471 9472; do curl -sf -m 5 "http://127.0.0.1:$p/api/actions" && return 0; done; }
leader_metric() { for p in 9471 9472; do m=$(curl -sf -m 5 "http://127.0.0.1:$p/metrics" || true); echo "$m" | grep -q '^mysql_guard_leader 1' && { echo "$m" | grep "^$1 " | awk '{print $2}'; return 0; }; done; }
mysql_up() { docker compose exec -T "$1" mysqladmin ping -h 127.0.0.1 -pplaceholder --silent >/dev/null 2>&1; }
lat_start() { docker compose exec -T loader bash -c 'cat > /tmp/lat.sh; chmod +x /tmp/lat.sh' <<'EOF'
echo $$ > /tmp/lat.pid; : > /tmp/lat.log
while :; do s=$(date +%s.%N)
  if mysql -h proxysql -P6033 -uroot -pplaceholder -N -B -e "INSERT INTO drill.load (tag) VALUES ('lat')" >/dev/null 2>&1; then r=ok; else r=FAIL; fi
  e=$(date +%s.%N); echo "$s $r $(echo "$e $s" | awk '{printf "%.3f", $1-$2}')" >> /tmp/lat.log; sleep 0.5; done
EOF
  docker compose exec -T -d loader bash /tmp/lat.sh; sleep 2; }
lat_stop() { docker compose exec -T loader bash -c 'kill $(cat /tmp/lat.pid) 2>/dev/null; rm -f /tmp/lat.pid' || true; }
lat_max() { docker compose exec -T loader awk -v a="$1" -v b="${2:-9999999999}" '$1>=a && $1<=b && $3>m{m=$3} END{printf "%.3f", m+0}' /tmp/lat.log; }
lat_fails() { docker compose exec -T loader awk -v a="$1" -v b="${2:-9999999999}" '$1>=a && $1<=b && $2=="FAIL"{n++} END{print n+0}' /tmp/lat.log; }
now() { date +%s; }
RES=()
res() { RES+=("$*"); log "RESULT: $*"; }

log "=== S0: baseline, stall handling on ==="
wait_for 30 "semi-sync on, one client" clients_is db-a 1
src_is db-a 1 || fail "S0: source not enabled"
writes_land_on db-a s0
[ "$(ssrec | jq -r '.stall_disabled // false')" = false ] || fail "S0: stall_disabled set: $(ssrec)"
pass "S0"

log "=== A1: replica mysqld stopped -> source turned off ==="
lat_start; sleep 6
ev_begin; T0=$(now); docker compose stop db-b >/dev/null
wait_for 90 "A1 source OFF on db-a" src_is db-a 0
T1=$(now)
res "A1 source turned OFF $((T1-T0))s after the replica mysqld stop (probes 3 x 2s + witness)"
sleep 20; T2=$(now)
res "A1 worst commit during the stop..off window: $(lat_max $T0 $T1)s; worst commit after off: $(lat_max $T1 $T2)s; failed writes since stop: $(lat_fails $T0 $T2)"
[ "$(ssrec | jq -r .stall_disabled)" = true ] || fail "A1: no stall_disabled in $(ssrec)"
[ "$(leader_metric mysql_guard_semisync_stall_off)" = 1 ] || fail "A1: gauge stall_off != 1"
actions_api | jq -e '[.[]|select(.action=="semisync_stall" or (.msg|tostring|test("semi-sync source turned off")))]|length>=1' >/dev/null || fail "A1: no action-log entry: $(actions_api | jq -c '.[-3:]')"
primary_key_is db-a || fail "A1: primary moved"
EV_WAIT=10 ev_check A1 semisync_stall_off
snap a1; pass "A1"

log "=== A2: replica mysqld started -> source on again after 5 healthy ticks ==="
ev_begin; T0=$(now); docker compose start db-b >/dev/null
wait_for 120 "db-b up" mysql_up db-b
wait_for 120 "A2 source ON on db-a" src_is db-a 1
T1=$(now)
wait_for 30 "A2 client registered" clients_is db-a 1
res "A2 source back ON $((T1-T0))s after the replica start; clients=$(clients db-a)"
[ "$(ssrec | jq -r '.stall_disabled // false')" = false ] || fail "A2: stall_disabled still set"
wait_for 30 "A2 gauge 0" sh -c "true"; [ "$(leader_metric mysql_guard_semisync_stall_off)" = 0 ] || fail "A2: gauge != 0"
sleep 10; res "A2 worst commit after on-again: $(lat_max $T1)s"
EV_WAIT=10 ev_check A2 semisync_stall_on
snap a2; pass "A2"

log "=== A7: replica back with its replica variable OFF -> guard keeps the source off ==="
ev_begin
docker compose stop db-b >/dev/null
wait_for 90 "A7 source OFF" src_is db-a 0
docker compose start db-b >/dev/null
wait_for 120 "db-b up" mysql_up db-b
sqlq db-b "SET GLOBAL rpl_semi_sync_replica_enabled=OFF; STOP REPLICA IO_THREAD; START REPLICA IO_THREAD"
wait_for 30 "db-b replicating" replica_running db-b
sleep 40
src_is db-a 0 || fail "A7: guard turned the source on with the replica side OFF"
[ "$(ssrec | jq -r .stall_disabled)" = true ] || fail "A7: record cleared: $(ssrec)"
res "A7 replica variable OFF, threads running, caught up: source stays OFF after 40s (no re-enable attempt)"
LOGHIT=$(docker compose logs guard-a guard-b 2>&1 | grep -c "semi-sync stays off: replica" || true); res "A7 log lines naming the replica side: $LOGHIT"
sqlq db-b "SET GLOBAL rpl_semi_sync_replica_enabled=ON; STOP REPLICA IO_THREAD; START REPLICA IO_THREAD"
wait_for 90 "A7 source ON again" src_is db-a 1
wait_for 30 "client" clients_is db-a 1
snap a7; pass "A7"
lat_stop

log "=== B7: target STOP REPLICA refused -> rollback, one writer ==="
sqlq db-a "CREATE USER IF NOT EXISTS mgsw IDENTIFIED BY 'placeholder'; GRANT ALL ON *.* TO mgsw WITH GRANT OPTION; REVOKE SUPER, REPLICATION_SLAVE_ADMIN ON *.* FROM mgsw"
wait_for 30 "GTIDs match" gtids_match
sqlq db-b "SHOW GRANTS FOR mgsw" | grep -ci "REPLICATION_SLAVE_ADMIN" || true
E=$(kv_epoch); ev_begin; load_start b7
FL7=("${FLAGS[@]/--mysql-user=root/--mysql-user=mgsw}")
set +e; docker compose exec -T guard-a mysql-guard switchover db-b --dry-run=false --node=db-a --mysql-host=db-a "${FL7[@]}" > /tmp/b7.out 2>&1; RC=$?; set -e
cat /tmp/b7.out | tail -12
res "B7 exit code $RC"
[ "$RC" != 0 ] || fail "B7: switchover succeeded with a refused STOP REPLICA"
sleep 5; load_stop
primary_key_is db-a || fail "B7: key moved to $(master)"
[ "$(kv_epoch)" = "$E" ] || fail "B7: epoch moved"
[ "$(ro db-a)" = 00 ] || fail "B7: db-a not writable ($(ro db-a))"
two_writers && fail "B7 TWO WRITERS"
writes_land_on db-a b7-after
replica_running db-b || fail "B7: db-b not replicating"
res "B7 after: key=$(master) epoch=$(kv_epoch) ro(a)=$(ro db-a) ro(b)=$(ro db-b); acked writes missing on db-a: $(acked_missing_on db-a b7)"
EV_WAIT=15 ev_check B7 switchover-aborted
snap b7; pass "B7"

log "=== B8: ProxySQL writer hostgroup that does not match ==="
ev_begin
set +e; docker compose exec -T guard-a mysql-guard switchover db-b --dry-run=true --node=db-a --mysql-host=db-a "${FLAGS[@]}" --proxysql-verify --proxysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306 --proxysql-writer-hg=99 > /tmp/b8.out 2>&1; RC=$?; set -e
grep -E "FAIL|ProxySQL|exit|preflight" /tmp/b8.out | head; res "B8 dry run with writer-hg=99: exit $RC; $(grep -c 'FAIL' /tmp/b8.out) blocking checks"
snap b8; pass "B8 (observed; see RESULT lines)"

log "=== A8: primary host lost while the stall state holds ==="
ev_begin
docker compose stop db-b >/dev/null
wait_for 90 "A8 source OFF" src_is db-a 0
docker compose start db-b >/dev/null
wait_for 120 "db-b up" mysql_up db-b
sqlq db-b "SET GLOBAL rpl_semi_sync_replica_enabled=OFF; STOP REPLICA IO_THREAD; START REPLICA IO_THREAD"
wait_for 30 "db-b replicating" replica_running db-b
sleep 12
[ "$(ssrec | jq -r .stall_disabled)" = true ] || fail "A8: stall state not holding: $(ssrec)"
E=$(kv_epoch); log "record before: $(ssrec)"
docker compose kill db-a guard-a >/dev/null
wait_for 150 "A8 failover to db-b" primary_key_is db-b
writes_land_on db-b a8
two_writers && fail "A8 TWO WRITERS"
res "A8 failover epoch $E -> $(kv_epoch); record after: $(ssrec | jq -c .)"
[ "$(ssrec | jq -r '.stall_disabled // false')" = false ] || fail "A8: new record still has stall_disabled: $(ssrec)"
snap a8; pass "A8"

log "=== results ==="
printf '%s\n' "${RES[@]}"
log "ALL V0.3.8 DRILLS PASSED"
