# Semi-sync stall handling and the guarded switchover command

Status: implemented on branch `feat/semisync-stall-switchover`, not deployed.
Both features are off or inert by default (`-semisync-stall` is `false`; `switchover` prints a plan unless `-dry-run=false`).

## 1. Semi-sync stall handling (`-semisync-stall`)

### Problem
With `rpl_semi_sync_source_enabled=ON`, `AFTER_SYNC` and a 10s `rpl_semi_sync_source_timeout`, every commit waits up to 10s for an acknowledgement when the only replica is dead. MySQL falls back to async after the first timeout, but the first writers stall and any later ack-less window repeats it.

### Rule
The leader guard (act mode only) turns `rpl_semi_sync_source_enabled` OFF on the primary when the replica is confirmed dead, and ON again when the replica is back and caught up.

Confirmed dead means all of these for `-semisync-stall-probes` (3) consecutive leader ticks, uninterrupted:

1. the leader's own probe of the replica failed;
2. a fresh witness guard reports its own connect check of the replica as `dead` (a primary-to-replica partition alone never counts; with no witness data nothing happens);
3. the primary reports `Rpl_semi_sync_source_clients = 0`.

The witness only probes the replica when it runs with `-semisync-stall` too (it publishes `peer_probes` in its guard record). Run the flag on the DB guards and the witness.

Re-enable needs, for `-semisync-reenable-ticks` (5) consecutive ticks: replica reachable, read-only, IO and SQL threads running, IO not lost, not stalled, lag <= `-semisync-reenable-max-lag` (1s), no GTID gap to the writer, and replica-side semi-sync active (`rpl_semi_sync_replica_enabled` and status ON; it is read when the IO thread starts, so a replica whose IO thread started before the variable was set is never re-enabled automatically).
After `SET ... ON` the guard waits up to 3s for a registered semi-sync client. If none shows up it turns the switch OFF again and backs off for 1 minute, so commits cannot stall on a broken re-enable.

### Safety
- Only a disable the guard made itself is ever undone: `SemiSyncRecord.StallDisabled` in Consul (`mysql/<cluster>/guard/semisync`), tied to node and epoch. An operator's own OFF is left alone.
- Never acts during maintenance, a failover/fence/switchover marker (plus its grace window), a pending failover vote, a recovering or read-only primary, or while the semi-sync failover gate is holding. Any of these resets the confirmation counters.
- Right before touching MySQL the executor re-checks mode, maintenance, the transition marker, and that writer key and epoch still equal what the decision was made for, and the leader lock is verified.
- If the Consul write after the OFF fails, the guard turns the switch back ON (an unowned OFF would never be undone).
- If someone else turned the switch back on, the flag is cleared (`clear`).
- In observe and fence mode the decision is logged and counted as `would_act`; nothing is written.
- A failover or switchover writes a fresh semi-sync record, which drops the flag.

### Observability
Logs `semi-sync stall` (decision changes only), action-log entries `semisync_stall` with step `off`, `on`, `clear`, ntfy alerts `semisync_stall_off` and `semisync_stall_on`.
Metrics: `mysql_guard_semisync_stall_off` (gauge, 1 while the guard holds the switch off), `mysql_guard_semisync_stall_disabled_total`, `mysql_guard_semisync_stall_reenabled_total`, `mysql_guard_semisync_stall_skipped_total{reason}`.

### Code
`internal/semistall` (pure decision, unit tested), `internal/actions/semistall.go` (guarded executor methods), `internal/guard/semistall.go` (per-tick glue, witness view), `internal/guard/v036.go` (witness probes the replica), `mysql.Admin.SetSemiSyncSource`.

## 2. `mysql-guard switchover --to <node>`

### Behaviour
```
mysql-guard switchover --to db-b <usual flags>                 # prints preflight + plan, changes nothing
mysql-guard switchover --to db-b --dry-run=false <usual flags> # runs it
```
`--dry-run` defaults to true. The positional target still works (`switchover db-b`), also with `--dry-run=false`. There is no HTTP endpoint: the dashboard has no authentication, and a state-changing endpoint behind no auth is not acceptable.

### Preflight (blocks the run when any check fails; sampled up to 3x because the old primary keeps taking writes)
no maintenance; no transition marker; old primary is the current writer (Consul key); old primary reachable and writable; target reachable, read-only, replication source configured, IO and SQL running without error, lag 0, holds every GTID of the old primary; semi-sync (when the source has it enabled): source status ON, a registered client, target replica status ON - and not "off by the stall handler".
Advisory only (warning): errant GTIDs of the target at read time (the authoritative check runs after the fence, as before), ProxySQL writer hostgroup state.

### Execution
Unchanged from v0.3.7: `super_read_only` on the old primary, drain, ProxySQL offline (admin mode only), wait for `GTID_EXECUTED` to match within `-switchover-catchup-timeout`, gate (missing or errant GTIDs abort), stop and reset replica on the target, `read_only=OFF`, CAS epoch bump, write the master key / writer service, `CHANGE REPLICATION SOURCE` of the old primary with the existing repl user and SSL, `START REPLICA`, wait until connected, restore semi-sync. Any failure before promotion rolls back (old primary writable, epoch unchanged); after the epoch bump the existing post-epoch handling applies.

New step at the end: read-only ProxySQL check that the writer hostgroup ONLINE rows are exactly the new primary, polled for `-switchover-proxysql-wait` (30s; needs `-proxysql-verify`). In monitor mode ProxySQL moves the hostgroup itself from its read_only checks. A miss logs `proxysql_verify` warn and sends the alert `switchover-proxysql`; it never rolls back a completed promotion.

### Behaviour change
Existing scripts and runbooks that call `switchover` now get a dry run unless they pass `--dry-run=false`. The integration drill scripts were updated. The preflight now also stops a switchover that previously started and aborted after fencing (lagging target, stopped replication).
