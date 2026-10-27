# mysql-guard

Draft Go service for MySQL GTID async-replication failover guarding: Consul session leadership, epoch fencing, GTID promotion gate, and separate health / renewal / leader-watch loops. **Not production-ready** — intended for review and test-env drills.

## Build and test

```bash
go build -o mysql-guard ./cmd/mysql-guard
go vet ./...
go test ./...
make release   # static linux/amd64 tarball + .sha256 in dist/
./integration/scripts/drill.sh   # docker-compose + smoke drill
```

Required flags (or env): `--cluster` / `GUARD_CLUSTER`, `--peers` / `GUARD_PEERS`, `--node` / `GUARD_NODE`.

Subcommands (`switchover`, `handover`, `rejoin`) accept the same flags in any position; an optional positional argument is the target peer name.

### MySQL credentials

Either set `-mysql-dsn` (no password in the DSN when using `-mysql-password-file`), or set `-mysql-user`, `-mysql-host`, `-mysql-port`, and `-mysql-password-file` / `MYSQL_PASSWORD`. The guard builds the DSN from host, port, user, and file contents.

### Consul agent service

Set `-consul-service-id` (and optionally `-consul-service-name`, default `mysql-guard`) to register the local agent service with a TTL check (`<id>:guard`). Replication and fence alerts update that check; if registration fails, TTL updates log once and are disabled.

### Initial writer KV

On a fresh cluster, the **leader** seeds `mysql/master/<cluster>` from the observed topology: only when the key is empty, every peer is reachable, exactly one peer is writable, and no other peer has GTIDs missing from that writer. Seeding is logged to the action log (`seed_primary`).

Only `db` guards compete for the leader lock (non-blocking session acquire, retried every `session-ttl/3`). `witness` guards take part in Consul coordination only and skip MySQL and replication checks.

Automatic failover (mode `act`): the leader promotes the most advanced reachable replica when at least `--votes-needed` DB guards report the primary unreachable for three consecutive ticks. A non-primary peer that stays writable for two ticks is fenced (`super_read_only`).

### Web UI

Open `http://<listen>/` on the same `-http` address. The dashboard polls `/status` and `GET /api/actions` (read-only). Action history is stored in Consul KV `mysql/<cluster>/actions` (cap `--action-log-max`, default 500) via CAS append.

### Remote peer MySQL

`-mysql-peer-endpoints` / `MYSQL_PEER_ENDPOINTS` (`db-a=host:port,db-b=host:port`) with shared `-mysql-user` and password file. Optional `-mysql-tls` and `-mysql-ca-file`. Switchover/rejoin and `/status` cluster view use these connections (local DSN remains fallback for this node).

### Replica-thread alerts during transitions (v0.3.2)

A switchover or failover writes a transition marker to `mysql/<cluster>/guard/transition` (action, from, to, started, ended). Every guard holds back `repl_io_stopped` and `repl_sql_stopped` while the marker is active (not ended and under 2 minutes old) and for 20s after it ends, and in any case raises them only after the condition has lasted `-repl-thread-alert-delay` (default 45s). A planned switchover or a failover therefore sends one alert (`switchover` or `failover`), not three. A replica thread that really stays stopped still alerts once after the delay. Other replication alerts (lag, stall, read_only drift) are unchanged. Drilled in `v030-drill.sh` V11a/V11b.

### Switchover ordering: fence, catch up, then check (v0.3.1)

GTID sets are compared only after the old primary is fenced. Order: (1) `super_read_only=ON` on the old primary and offline it in ProxySQL; open transactions get `-fence-drain-timeout` (default 3s) to finish, then their sessions are killed; (2) read the old primary's final `gtid_executed`; (3) wait for the target to apply it within `-switchover-catchup-timeout` (default 2x `-max-apply-lag`); (4) errant check: target minus source must be empty, and source minus target must be empty. If catch-up times out or the check fails, the switchover aborts before the epoch bump: the old primary is made writable again, ProxySQL is restored, the epoch and master key are unchanged, and `switchover-aborted` (or `errant_gtid`) is alerted. v0.3.0 compared snapshots read before the fence, so under writes a replica that applied a newer source transaction between the two reads was wrongly reported as having extra GTIDs. Failover is not affected: its gate reads only the candidate (received vs executed) and the dead primary takes no writes; the superset gate against the last published writer GTIDs can only fail safe (block until the next tick). Drilled under ~20 writes/s both ways and with a forced catch-up timeout (`v030-drill.sh` V9, V10).

### Switchover rollback after epoch bump (v0.3.0)

Before the epoch CAS and master KV update, failed steps roll back (old primary writable, ProxySQL restored). If writing the master key fails **after** the epoch bump (3 attempts), the guard now aborts cleanly instead of leaving a half-done switchover: the new node is demoted (refused if it already has transactions the old primary lacks), re-pointed at the old primary, the old primary is made writable again and re-published at the bumped epoch, and `switchover-aborted` is alerted. If only the writer-service sync failed after the key was written, the switchover completes and the next leader tick retries the service. Drilled with a `-tags faultinject` build (`integration/scripts/v030-drill.sh` V4); release builds contain no fault hook.

### ProxySQL admin

Admin updates match `mysql_servers` rows by **hostname and port**. Map logical peer names to ProxySQL backends with `-proxysql-peer-endpoints` / `PROXYSQL_PEER_ENDPOINTS`, e.g. `db-a=10.0.0.1:3306,db-b=10.0.0.2:3306`. Unmapped peers use the peer name as hostname and port `3306`.

## Binary

- **`mysql-guard`** — long-running guard (`--mode observe` by default)
- **`mysql-guard switchover|handover <node>`** — planned handover (fence, catch-up, then errant check; see above)
- **`mysql-guard rejoin <source>`** — errant check + auto-position vs needs-rebuild plan

## Modes

| Mode | Behaviour |
|------|-----------|
| `observe` | Full decision loop; log and metrics only (`mysql_guard_would_act_total`). Never writes cluster state (master key, epoch, `writer/current`, `writer/last-gtid`, writer service, MySQL `read_only`); still writes its own bookkeeping (instance heartbeats, actions log, leader lock) |
| `fence` | Fence stray writers; no promotion |
| `act` | Fence + promote + ProxySQL admin (when enabled) |

## Key flags (see `--help`)

| Flag | Default | Notes |
|------|---------|--------|
| `--mode` | `observe` | |
| `--node` / `GUARD_NODE` | required | |
| `--role` | `db` | `db` (promotable) or `witness` |
| `--lag-gate-max-wait` | `0` | Never promote with missing GTIDs |
| `--proxysql-admin-enable` | `false` | Admin hostgroup updates on promote/fence |
| `--ntfy-url` | | Replication / errant / fence alerts |
| `--repl-lag-alert-sec` | `30` | IO/SQL/lag/read_only drift alerts |
| `--repl-thread-alert-delay` | `45s` | IO/SQL thread-stopped alerts only after persisting this long and outside a switchover/failover window (`REPL_THREAD_ALERT_DELAY`) |
| `--min-between-failovers` | `1h` | Rate limit between real failovers (writer moves to another node) |
| `--read-only-grace` | `10s` | A reachable primary that stays read-only this long counts as failed (see below) |
| Maintenance KV | | `mysql/<cluster>/guard/maintenance` — any value pauses actions |

## v0.3.0: prod-compatibility features

### Master key format and migration

`-master-format` / `MASTER_FORMAT`:

* `epoch` (default): `mysql/master/<cluster>` = `node|epoch`.
* `legacy`: `mysql/master/<cluster>` = `host:port` exactly as the bash guard and the repman post-script write it; the epoch is kept in `mysql/<cluster>/writer/epoch` and `mysql/<cluster>/writer/current` = `node|epoch`.

Both formats are always **read**. A legacy value is mapped to a node through `-mysql-peer-endpoints` (an unknown endpoint is an error, never a guess). Migration (always run with the guards' own flags):

1. Deploy the guards in `-mode=observe` with `-master-format=legacy` next to the bash guard: they read the existing key and never write cluster state (master key, epoch, writer/current, writer/last-gtid, writer service, MySQL read_only). They do write their own bookkeeping under `mysql/<cluster>/guard/` and `mysql/<cluster>/actions` (instance heartbeats, actions log, leader lock), so the guard Consul policy needs write there even in observe.
2. Stop the bash guard, then `mysql-guard migrate-key -to=legacy` once: initialises `writer/epoch` (1) and `writer/current` without changing the key value consumers read.
3. Switch the guards to `-mode=act`, still legacy. Consumers keep reading `host:port`.
4. Optional, after every consumer of the key understands `node|epoch`: `migrate-key -to=epoch`, then set `-master-format=epoch`. `migrate-key -to=legacy` reverts. `--dry-run` prints the target value; migration is a CAS on the key and is refused in observe mode.

### Writer service

`-writer-service=<name>` (e.g. `mysql-prod-writer`), `-writer-service-tags` (default `mysql,writer`). The leader registers a catalog service on node `<name>-vnode` pointing at the primary's endpoint, with Meta `cluster`, `node`, `epoch`, `promoted_by=mysql-guard` — the same shape as the bash guard's `mysql-prod-writer`. Synced after every SetMaster and every leader tick (idempotent); never in observe mode or under maintenance. Failure alerts `writer_service_sync_failed`. The guard's Consul policy needs `service "<name>" { policy = "write" }` and `node "<name>-vnode" { policy = "write" }`.

### Repman: the guard is the sole promoter

Run replication-manager in `failover-mode = "manual"` and replace its `failover-post-script` (`repman-failover-consul.sh`) with a no-op (`examples/repman-failover-post-script-noop.sh`), so only the guard writes `mysql/master/<cluster>` and the writer service. Repman stays useful for monitoring. Any write to the master key that the guard did not make (wrong epoch/node vs `writer/epoch` / `writer/current`, or a legacy value while in epoch mode) is alerted as `foreign_master_write` after 2 ticks (`-foreign-write-alert`, default on), and `foreign_master_write_cleared` once consistent.

### ProxySQL verification

Default unchanged: ProxySQL follows `read_only` (`mysql_replication_hostgroups`) and the guard never writes ProxySQL. `-proxysql-verify` with `-proxysql-admin-dsn` (the guard only runs `SELECT`s; ProxySQL has no read-only admin role, so give it a dedicated admin user) compares `runtime_mysql_servers` ONLINE rows in the writer hostgroup (`-proxysql-writer-hg`, 10) with the primary's `-proxysql-peer-endpoints` entry. A mismatch persisting `-proxysql-verify-grace` (20s) alerts `proxysql_writer_mismatch`, then `proxysql_writer_ok` when fixed. It only alerts; it never changes ProxySQL or the primary.

### ntfy alerts

`-ntfy-url` (`https://ntfy.sh/<topic>` or a self-hosted server), optional `-ntfy-token-file` (Bearer). Title `mysql-guard <cluster>: <code>`. Codes: `switchover`, `switchover-aborted`, `failover`, `restore_writer`, `fence`, `needs-rebuild`, `errant_gtid`, `*-degraded`, `foreign_master_write[_cleared]`, `proxysql_writer_mismatch` / `proxysql_writer_ok`, `writer_service_sync_failed`. Delivery failures are logged (`alert delivery failed`).

### Remaining before production

* Prod Consul policies: add the writer service/vnode write rules and `writer/` keys for the prod guard tokens.
* Repman: switch the prod post-script to the no-op and confirm `failover-mode=manual` (needs Daniel).
* ntfy: dedicated prod topic in prod Vault (exists for the bash guard) and a subscribed device; consider a token-protected self-hosted topic.
* Shadow run: guards in observe + legacy beside the bash guard for an agreed period, comparing decisions, then the cut-over (stop bash guard, `migrate-key -to=legacy`, act).
* Prod ProxySQL: a dedicated admin user (remote-capable, not `admin`) for `-proxysql-verify`.
* Runbook for `switchover-aborted`, `foreign_master_write`, `needs-rebuild`.

## Read-only primary (v0.2.8)

A primary that is reachable but stays read-only for longer than `--read-only-grace`
(for example after a restart whose init waits for the guard) counts as failed:

- If its `gtid_executed` includes every reachable peer's set (and the last published
  writer set for any unreachable peer), the leader makes it writable again:
  STOP/RESET REPLICA, CAS epoch bump, master key `<same node>|<epoch+1>`, then
  `super_read_only=OFF, read_only=OFF`. Action log: `restore_writer`.
- Otherwise it fails over to a complete replica through the normal failover path
  (stall and superset gates apply).

Rate limit decision: `--min-between-failovers` applies only between real failovers.
A writer restore is not a failover (the writer does not move, so it cannot flap
between nodes), is not rate limited, and does not reset the failover timer. A
failover caused by an incomplete read-only primary is a real failover and is rate
limited. Maintenance mode blocks both.

## License

MIT — see [LICENSE](LICENSE).

Credentials: `MYSQL_DSN`, `MYSQL_PASSWORD` / `MYSQL_PASSWORD_FILE`, `CONSUL_HTTP_TOKEN` / `CONSUL_TOKEN_FILE` (never logged).

## Features in this draft

| Area | Status |
|------|--------|
| Consul leader lock + renewal goroutine | Implemented (non-blocking session acquire) |
| Epoch CAS + fenced KV markers | Implemented |
| GTID gate + peer offset election | Implemented (GTID parser is simplified) |
| Vote streaks (V1 probe wired; V2/V3 stubbed in health loop) | Partial |
| ProxySQL admin on promote/fence | Implemented behind `--proxysql-admin-enable` |
| Errant GTID before promote/rejoin | Implemented + unit tests |
| Rejoin plan (auto-position vs needs-rebuild) | Implemented; SQL execution TODO |
| Replication alerts → ntfy + Consul TTL checks | Implemented (needs `CONSUL_SERVICE_ID`) |
| Maintenance mode KV | Implemented |
| `/status` topology (primary, epoch, nodes, guards) | Implemented (guard discovery TODO) |
| Embedded ops Web UI (`/`, `/api/actions`) | Implemented (read-only) |
| Consul action log KV (`mysql/<cluster>/actions`) | Implemented |
| Planned switchover / rejoin CLI | Implemented (local MySQL; multi-host orchestration limited) |
| Consul events, full V2/V3 votes, rate-limit KV | TODO |
| Nomad + Vault wiring | Example job only (`examples/nomad/mysql-guard.hcl`) |

## Architecture (loops)

- **Renewal** — Consul session / lock (`internal/consul/lock.go`), independent of MySQL probes
- **Health** — probe interval, feeds vote streaks and replication alerts
- **Leader-watch** — blocking KV watch on `mysql/<cluster>/guard/leader`

## Rollout phases (from plan)

0. Bash guard + optional V3 heartbeat fix in prod  
1. **This repo** — unit tests + docker-compose drills  
2. Nomad test namespace + G1–Gn drills  
3. Prod observe beside bash guard  
4. Act on staging  
5. Prod act; retire bash guard and replication-manager  

## Out of scope

Backups, log collection, automatic replication error skipping.

## Repository name

The Go module path is `mysql-guard`. Rename the git remote/repository to `mysql-guard` in your forge when you create the repo (cloud agents cannot rename the remote).

- `-repl-stmt-timeout` / `REPL_STMT_TIMEOUT` (default 30s): timeout for replication statements, with START REPLICA retry/backoff and post-drop state verification.
- `-stall-after` / `STALL_AFTER` (default 30s, v0.2.6): a replica whose IO/SQL threads are ON but whose GTID gap to the writer has not shrunk for this long (including Retrieved_Gtid_Set empty while the writer advanced, e.g. a server_id equal to the events' originating server_id) is flagged `stalled` in `/status`, raises a `repl_stalled` critical alert, sets the guard TTL check to warning, and is never a failover candidate (action log step `stalled_candidate`). Automatic failover also refuses any candidate whose executed+retrieved set is not a superset of the last-known writer's executed set (`mysql/<cluster>/writer/last-gtid`, step `superset_gate`). GTID sets with a UUID repeated are now merged, not overwritten. Drill 10 covers this.
- v0.2.7: the last-writer GTID key (`mysql/<cluster>/writer/last-gtid`) is republished by the leader until a write succeeds, instead of only on the tick that saw the change (which usually preceded leadership, so the key was never written). Drills 1 and 2 assert it.
