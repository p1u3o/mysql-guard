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
cfg_repl() { sqlq db-b "STOP REPLICA; RESET BINARY LOGS AND GTIDS; CHANGE REPLICATION SOURCE TO SOURCE_HOST='db-a', SOURCE_USER='repl', SOURCE_PASSWORD='placeholder', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1; START REPLICA; SET GLOBAL super_read_only=ON;"; }
# db-b may answer from its temporary init server and then restart: retry.
wait_for 60 "replication configured on db-b" cfg_repl
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
repl_state_on() { status "$1" | jq -e --arg n "$2" --arg s "$3" '[.nodes[] | select(.node==$n and .repl_state==$s)] | length == 1' >/dev/null; }
wait_for 30 "db-b reported running on all DB guards" sh -c "$(declare -f status repl_state_on); repl_state_on 9471 db-b running && repl_state_on 9472 db-b running"
[ "$(status 9471 | jq '[.nodes[] | select(has("proxysql_status"))] | length')" = 0 ] || fail "proxysql_status shown without ProxySQL"
[ "$(status 9473 | jq '.loops.renew_age_sec < 30')" = true ] || fail "witness renew age $(status 9473 | jq .loops.renew_age_sec)"
for p in "${GUARDS[@]}"; do [ "$(status $p | jq -r '.version')" = drill ] || fail "guard $p version=$(status $p | jq -r .version)"; done
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

log "=== Drill 7: guard user without performance_schema grant -> replication state unknown ==="
sqlq db-b "SELECT 1" >/dev/null || fail "db-b down"
sqlq db-b "CREATE USER IF NOT EXISTS 'nops'@'%' IDENTIFIED BY 'placeholder'; GRANT SUPER, REPLICATION_SLAVE_ADMIN, REPLICATION CLIENT, PROCESS, SYSTEM_VARIABLES_ADMIN, RELOAD ON *.* TO 'nops'@'%'" || fail "create nops user on primary db-b"
wait_for 30 "nops user on db-a" sh -c "[ \"\$(docker compose exec -T db-a mysql -uroot -pplaceholder -N -B -e \"SELECT COUNT(*) FROM mysql.user WHERE user='nops'\" 2>/dev/null)\" = 1 ]"
RO_A=$(ro db-a); RO_B=$(ro db-b); GT7=$(gtid db-b)
docker compose run -d --rm --no-deps --name "${COMPOSE_PROJECT_NAME:-integration}-nops" -p 9474:9474 -e MYSQL_PASSWORD=placeholder guard-a \
  --node=db-a --mysql-host=db-a --role=db --mode=observe --cluster=nops --peers=db-a,db-b --mysql-user=nops \
  --mysql-peer-endpoints=db-a=db-a:3306,db-b=db-b:3306 --interval=2s --session-ttl=10s --lock-delay=1s --http=:9474 >/dev/null || fail "start nops guard"
wait_for 60 "nops guard reports db-a (replica) unknown" sh -c "curl -sf -m5 http://127.0.0.1:9474/status | jq -e '[.nodes[] | select(.node==\"db-a\" and .repl_state==\"unknown\" and .io_thread==null and .sql_thread==null and (.repl_error|test(\"denied\")))] | length == 1'"
sleep 8
NL=$(docker logs "${COMPOSE_PROJECT_NAME:-integration}-nops" 2>&1)
echo "$NL" | grep -q '"code":"repl_state_unknown"' || fail "nops guard did not log repl_state_unknown"
echo "$NL" | grep -qE 'repl_(io|sql)_stopped' && fail "nops guard reported stopped on a permission error"
# logged only when the alert set changes (seeding may change it once), never per 2s tick
[ "$(echo "$NL" | grep -c '"code":"repl_state_unknown"')" -le 2 ] || fail "repl_state_unknown repeated per tick (spam)"
[ "$(curl -sf -m5 http://127.0.0.1:9474/api/actions | jq '[.entries[]? | select(.action!="seed_primary")] | length')" = 0 ] || fail "nops guard took actions"
[ "$(ro db-a)" = "$RO_A" ] && [ "$(ro db-b)" = "$RO_B" ] || fail "read_only changed during nops drill"
curl -sf -m5 http://127.0.0.1:9474/status > "$SNAPDIR/07-nops-status.json" || true
docker rm -f "${COMPOSE_PROJECT_NAME:-integration}-nops" >/dev/null
sqlq db-b "DROP USER 'nops'@'%'" || fail "drop nops"
primary_is db-b || fail "primary moved during nops drill"
wait_for 30 "GTIDs match after nops drill" gtids_match
pass "drill 7: no performance_schema grant -> db-a repl_state=unknown, no stopped alert, logged once, no actions, read_only unchanged ($RO_A/$RO_B)"

src_user() { sqlq "$1" "SELECT USER FROM performance_schema.replication_connection_configuration LIMIT 1"; }
count_res() { actions | jq --arg a "$1" --arg r "$2" '[.entries[] | select(.action==$a and .step=="end" and .result==$r)] | length'; }

log "=== Drill 8: switchover verifies the new replica is connected via the repl account ==="
N=$(count_ok switchover); E=$(kv_epoch)
docker compose exec -T guard-b mysql-guard switchover db-a --node=db-b --mysql-host=db-b "${FLAGS[@]}" || fail "drill 8 switchover exit"
wait_for 20 "primary db-a on all guards" primary_is db-a
check_roles db-a db-b
replica_running db-b || fail "db-b threads not ON at switchover return"
[ "$(src_user db-b)" = repl ] || fail "db-b source user $(src_user db-b), want repl"
[ "$(count_ok switchover)" = $((N+1)) ] || fail "switchover ok not logged"
actions | jq -e '[.entries[] | select(.action=="switchover" and .step=="end" and .result=="ok")][-1].reason | test("connected \\(user repl\\)")' >/dev/null || fail "ok message lacks connected/repl"
write_row db-a d8-ok; wait_for 30 "d8-ok row on db-b" row_on db-b d8-ok
pass "drill 8a: replica db-b ON/ON as user repl at switchover return, epoch $E->$(kv_epoch)"

log "Drill 8b: broken repl password -> switchover logged degraded, never ok"
sqlq db-a "ALTER USER 'repl'@'%' IDENTIFIED BY 'wrong-on-purpose'" || fail "break repl pw"
wait_for 30 "GTIDs match" gtids_match
N=$(count_ok switchover); D=$(count_res switchover degraded)
if docker compose exec -T guard-a mysql-guard switchover db-b --node=db-a --mysql-host=db-a "${FLAGS[@]}"; then fail "degraded switchover exited 0"; fi
wait_for 20 "primary db-b on all guards after degraded switchover" primary_is db-b
check_roles db-b db-a
[ "$(count_ok switchover)" = "$N" ] || fail "false ok logged for degraded switchover"
[ "$(count_res switchover degraded)" = $((D+1)) ] || fail "degraded not logged"
snap 08-degraded
sqlq db-b "ALTER USER 'repl'@'%' IDENTIFIED BY 'placeholder'" || fail "restore repl pw"
sqlq db-a "STOP REPLICA; START REPLICA" || fail "restart db-a replica"
wait_for 60 "db-a replicating after pw restore" replica_running db-a
wait_for 30 "GTIDs match" gtids_match
write_row db-b d8-restored; wait_for 30 "d8-restored on db-a" row_on db-a d8-restored
pass "drill 8b: degraded logged + non-zero exit, no false ok; recovered, gtid=$(gtid db-a)"

log "=== Drill 9: slow START REPLICA on the old primary (3s driver readTimeout would fail) ==="
# CLI runs in guard-b; its self connection to db-b (old primary) goes through slowproxy.
PFLAGS=("${FLAGS[@]}")
run_proxy() { docker compose exec -T -d guard-b sh -c "exec slowproxy -listen 127.0.0.1:3399 -target db-b:3306 -mode $1 -delay 6s > /tmp/slowproxy-$1.log 2>&1"; sleep 1; }
stop_proxy() { docker compose exec -T guard-b sh -c 'kill $(pidof slowproxy) 2>/dev/null; true'; }
nr_keys() { docker compose exec -T consul consul kv get -recurse mysql/example/guard/needs-rebuild/ 2>/dev/null | wc -l | tr -d ' '; }
[ "$(nr_keys)" = 0 ] || fail "needs-rebuild keys present before drill 9"

# 9a: START REPLICA response delayed 6s (> old 3s readTimeout, < 30s repl-stmt-timeout)
primary_is db-b || fail "drill 9 expects primary db-b"
run_proxy delay
N=$(count_ok switchover); E=$(kv_epoch); T0=$(date +%s)
docker compose exec -T guard-b mysql-guard switchover db-a --node=db-b --mysql-host=127.0.0.1 --mysql-port=3399 "${PFLAGS[@]}" || { docker compose exec -T guard-b cat /tmp/slowproxy-delay.log; fail "drill 9a switchover exit"; }
DT=$(( $(date +%s) - T0 ))
docker compose exec -T guard-b cat /tmp/slowproxy-delay.log | grep -q 'DELAYING "START REPLICA" by 6s' || fail "proxy did not delay START REPLICA"
stop_proxy
wait_for 20 "primary db-a on all guards" primary_is db-a
check_roles db-a db-b
replica_running db-b || fail "db-b threads not ON"
[ "$(src_user db-b)" = repl ] || fail "db-b source user"
[ "$(count_ok switchover)" = $((N+1)) ] || fail "9a switchover ok not logged"
[ "$(nr_keys)" = 0 ] || fail "needs-rebuild set in 9a"
[ "$DT" -ge 6 ] || fail "9a took ${DT}s, delay not applied"
write_row db-a d9a; wait_for 30 "d9a on db-b" row_on db-b d9a; wait_for 30 "GTIDs match" gtids_match
pass "drill 9a: START REPLICA delayed 6s -> switchover ok in ${DT}s, epoch $E->$(kv_epoch), no needs-rebuild"

# 9b: START REPLICA forwarded, then the connection drops: must reconnect, verify, not needs-rebuild
log "Drill 9b: connection dropped right after START REPLICA"
docker compose exec -T -d guard-a sh -c "exec slowproxy -listen 127.0.0.1:3399 -target db-a:3306 -mode drop > /tmp/slowproxy-drop.log 2>&1"; sleep 1
N=$(count_ok switchover); E=$(kv_epoch)
docker compose exec -T guard-a mysql-guard switchover db-b --node=db-a --mysql-host=127.0.0.1 --mysql-port=3399 "${PFLAGS[@]}" || { docker compose exec -T guard-a cat /tmp/slowproxy-drop.log; fail "drill 9b switchover exit"; }
docker compose exec -T guard-a cat /tmp/slowproxy-drop.log | grep -q 'DROPPING client after "START REPLICA"' || fail "proxy did not drop after START REPLICA"
docker compose exec -T guard-a sh -c 'kill $(pidof slowproxy) 2>/dev/null; true'
wait_for 20 "primary db-b on all guards" primary_is db-b
check_roles db-b db-a
replica_running db-a || fail "db-a threads not ON"
[ "$(count_ok switchover)" = $((N+1)) ] || fail "9b switchover ok not logged"
[ "$(nr_keys)" = 0 ] || fail "needs-rebuild set in 9b"
actions | jq -e '[.entries[] | select(.action=="switchover" and (.step=="start_replica_retry" or .step=="start_replica"))] | length >= 1' >/dev/null || fail "9b retry/verify not logged"
write_row db-b d9b; wait_for 30 "d9b on db-a" row_on db-a d9b; wait_for 30 "GTIDs match" gtids_match
snap 09-slow-start
pass "drill 9b: dropped connection after START REPLICA -> state verified, ok, epoch $E->$(kv_epoch), no needs-rebuild, gtid=$(gtid db-a)"

log "=== Drill 10: replica with duplicate server_id -> stalled, flagged, never promoted ==="
P=$(kv_primary); R=db-b; [ "$P" = db-b ] && R=db-a
PSID=$(sqlq "$P" "SELECT @@server_id"); RSID=$(sqlq "$R" "SELECT @@server_id")
[ -n "$PSID" ] && [ "$PSID" != "$RSID" ] || fail "server ids P=$PSID R=$RSID"
wait_for 30 "GTIDs match before drill 10" gtids_match
# Reproduce the 2026-10-26 incident: the replica's server_id equals the originating
# server_id of events it must apply (not the source's own id, which MySQL rejects at
# connect). The IO thread silently skips them: threads ON, no error, no progress.
DUP=4242
sqlq "$R" "STOP REPLICA; SET GLOBAL server_id=$DUP; START REPLICA" || fail "set server_id $DUP on $R"
wait_for 30 "$R threads ON" replica_running "$R"
sqlq "$P" "SET GLOBAL server_id=$DUP" || fail "set origin id on $P"
write_row "$P" d10-a; write_row "$P" d10-b
sqlq "$P" "SET GLOBAL server_id=$PSID" || fail "restore $P server_id"
sleep 3
row_on "$R" d10-a && fail "$R applied d10-a despite duplicate server_id (repro failed)"
stalled_on() { for p in 9471 9472; do [ "$(status "$p" | jq -r --arg n "$R" '.nodes[] | select(.node==$n) | .stalled')" = true ] || return 1; done; }
T0=$(date +%s)
wait_for 40 "$R flagged stalled on both DB guards (witness has no topology)" stalled_on
log "stalled flagged after $(( $(date +%s) - T0 ))s: $(status 9471 | jq -c --arg n "$R" '.nodes[] | select(.node==$n) | {stalled,stall_reason,gtid_gap_to_writer,gtid_retrieved}')"
replica_running "$R" || fail "$R threads not ON (stall must be threads ON)"
docker compose logs --no-color guard-a guard-b 2>/dev/null | grep -q 'repl_stalled' || fail "repl_stalled alert not logged"
E=$(kv_epoch); NF=$(count_ok failover)
docker compose stop "$P"
sleep 25
[ "$(kv_primary)" = "$P" ] || fail "primary moved to stalled $R"
[ "$(kv_epoch)" = "$E" ] || fail "epoch bumped $E->$(kv_epoch)"
[ "$(count_ok failover)" = "$NF" ] || fail "failover ok logged with stalled candidate"
actions | jq -e '[.entries[] | select(.step=="stalled_candidate" and .result=="blocked")] | length >= 1' >/dev/null || fail "stalled_candidate block not logged"
[ "$(ro "$R")" != "00" ] || fail "$R became writable"
docker compose start "$P"
wait_for 120 "$P back" sh -c "docker compose exec -T $P mysql -uroot -pplaceholder -e 'SELECT 1'"
sqlq "$R" "STOP REPLICA; SET GLOBAL server_id=$RSID; START REPLICA" || fail "restore server_id on $R"
wait_for 60 "$R replicating" replica_running "$R"
wait_for 60 "d10-b on $R" row_on "$R" d10-b
wait_for 30 "GTIDs match after drill 10" gtids_match
unstalled() { for p in 9471 9472; do [ "$(status "$p" | jq -r --arg n "$R" '.nodes[] | select(.node==$n) | .stalled')" = false ] || return 1; done; }
wait_for 30 "$R stall cleared" unstalled
snap 10-dup-server-id
pass "drill 10: $R with server_id=$DUP (= origin id of new events from $P) flagged stalled, failover refused while $P down (epoch $E), recovered, gtid=$(gtid "$P")"

SK=$(count_skipped)
[ "$SK" = 0 ] || fail "$SK action log entries with result=skipped"
one_leader || fail "leadership not single at end"
pass "action log: no skipped entries; $(actions | jq '.entries | length') entries total"
log "ALL DRILLS PASSED"
docker compose ps
