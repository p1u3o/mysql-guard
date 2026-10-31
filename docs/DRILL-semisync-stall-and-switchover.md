# Drill plan: semi-sync stall handling and switchover

Run on a disposable pair only (mxdev, or the test pair `kv01-par`/`kv02-par`, cluster `mysql-guard-test`), never on `mysql-prod-async`.
Build: `make build`, install the binary on both nodes and the witness, add `-semisync-stall` to all three guards, keep `-mode=act`.
Baseline: semi-sync ON both sides (`rpl_semi_sync_source_timeout=10000`), IO thread started after the replica variable was ON, `sysbench oltp_write_only` (or a 20 tx/s insert loop) against the writer, recording p99 commit latency.

## A. Stall handling
| # | Action | Expect |
|---|---|---|
| A1 | Replica `systemctl stop mysql` | within ~3 ticks + 1 witness heartbeat: log `semi-sync stall ... action=disable`, `rpl_semi_sync_source_enabled=OFF` on primary, action-log `semisync_stall/off`, ntfy `semisync_stall_off`, gauge `mysql_guard_semisync_stall_off 1`. Writer stall at most the first timeout window, then no 10s commits. The stall tracker steps only on the guard that holds the leader lock (`mysql/<cluster>/guard/leader`); for the isolation in a real-pair run the leader must be the guard whose own probe of the replica fails, i.e. the one on the primary's node. Check the lock holder first. |
| A2 | Start the replica | after >=5 healthy ticks: switch ON, a semi-sync client registered, `semisync_stall/on`, gauge 0, record `stall_disabled` gone. Write latency back to sync level. |
| A3 | Partition primary<->replica only (iptables on the primary, witness still reaches the replica) | no disable (`witness probe ok`); skip metric `reason="blocked"` not raised; nothing in the action log. Remove the rule. |
| A4 | Stop replica, then `consul kv put mysql/<cluster>/guard/maintenance x` before 3 ticks | counters reset, no action; clear maintenance. |
| A5 | Operator sets OFF by hand with the replica healthy | guard leaves it OFF (no `stall_disabled`), `semisync_async`/off alert behaviour unchanged. |
| A6 | Guard disabled it (A1), operator sets ON by hand | next tick: log `semi-sync stall ... action=clear` and action-log `semisync_stall/clear`; the record loses `stall_disabled`. |
| A7 | Start the replica with `rpl_semi_sync_replica_enabled=OFF` | guard does not re-enable (reason names replica side); with it ON but without restarting the IO thread: guard tries, no client within 3s, turns OFF again, warn + 1 min backoff, no commit stall longer than ~3s. Record the actual stall seen. The "tries" path needs the replica-side pre-check to pass, which a real pair with the IO thread running never lets it do; it is covered by the unit tests and code review only. The Docker drill (`integration/scripts/v038-drill.sh`) covers the variable-OFF case. |
| A8 | Kill the primary while A1 state holds (flag set) | failover proceeds as before; new record has no `stall_disabled`. |
| A9 | Same run with the flag omitted on the witness | never disables (no witness data). |
| A10 | Observe mode with A1 | logs `would act`, `would_act_total` increments, MySQL untouched. |

## B. Switchover
| # | Action | Expect |
|---|---|---|
| B1 | `switchover --to <replica>` (dry run) | preflight all `ok`, plan printed, exit 0, no change anywhere. |
| B2 | B1 with the replica `STOP REPLICA` | `FAIL` on IO/SQL threads, exit 1, nothing changed, action log `blocked/preflight`. |
| B3 | B1 with target lag (`SET GLOBAL replica_parallel_workers` low plus a big write, or `STOP REPLICA SQL_THREAD` for a few seconds) | `FAIL` lag or GTID, exit 1. |
| B4 | `--dry-run=false` under write load | writer moves, epoch +1, old primary replica connected, `proxysql_verify ok`, alert `switchover`, write gap = fence-to-promote only. Key, writer service and ProxySQL agree. |
| B5 | Switch back with `switchover db-a --dry-run=false` (positional form) | same. |
| B6 | Preflight while stall-disabled (A1 state, replica up but not yet re-enabled) | blocked with the stall-handler message. |
| B7 | Inject failure before promotion (make the target's `STOP REPLICA` fail, e.g. revoke a privilege) | rollback: old primary writable, epoch unchanged, alert `switchover-aborted` (also for failures of `STOP REPLICA`, `RESET REPLICA ALL` and promotion). |
| B8 | ProxySQL in monitor mode with a wrong writer hostgroup config | a wrong writer hostgroup only gives a `[warn] ProxySQL shows the old primary as writer` preflight line, never a block; the post-promotion check alerts and never rolls back. |
| B9 | Maintenance flag set | preflight blocks. |
| B10 | `--to` equal to the current writer / unknown node | refused with a clear message. |

Pass criteria: no run leaves two writers; every abort leaves exactly one writer; no secrets in logs or alerts.
Before the prod run: B1 only (dry run on prod is read-only), then A1-A3 on the test pair, then B4 on the test pair.

## Notes from the first run
- A Nomad template with `change_mode=restart` restarts the guard task now and then; that resets the in-memory tracker and the Prometheus counters (`would_act_total` reads 0 after it). Compare against the log, not only the counter.
- The mysqld stop/start cases (A1, A2, A7, A8, B7, B8) can be run without touching a real MySQL with `integration/scripts/v038-drill.sh` (Docker, project name via `COMPOSE_PROJECT_NAME`).
