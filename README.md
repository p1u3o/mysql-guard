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

### Switchover rollback after epoch bump

Before the epoch CAS and master KV update, failed steps roll back (old primary writable, ProxySQL restored). **After epoch bump**, failures do not demote the new primary: the guard runs a **post-epoch abort** — logs `switchover_abort`, sets `mysql/<cluster>/guard/needs-rebuild/<old>`, and alerts. Manual rebuild of the old node is required.

### ProxySQL admin

Admin updates match `mysql_servers` rows by **hostname and port**. Map logical peer names to ProxySQL backends with `-proxysql-peer-endpoints` / `PROXYSQL_PEER_ENDPOINTS`, e.g. `db-a=10.0.0.1:3306,db-b=10.0.0.2:3306`. Unmapped peers use the peer name as hostname and port `3306`.

## Binary

- **`mysql-guard`** — long-running guard (`--mode observe` by default)
- **`mysql-guard switchover|handover <node>`** — planned handover (GTID-equal + errant checks; full SQL steps TODO)
- **`mysql-guard rejoin <source>`** — errant check + auto-position vs needs-rebuild plan

## Modes

| Mode | Behaviour |
|------|-----------|
| `observe` | Full decision loop; log and metrics only (`mysql_guard_would_act_total`) |
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
| Maintenance KV | | `mysql/<cluster>/guard/maintenance` — any value pauses actions |

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
