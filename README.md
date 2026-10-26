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

On a fresh cluster, the **leader** running on the **first peer** (`--peers` order) seeds `mysql/master/<cluster>` when the key is absent, the local instance is writable, and it has no replication source. A log line is emitted when seeding runs.

### ProxySQL admin

Admin updates match `mysql_servers` rows by **hostname and port**. Map logical peer names to ProxySQL backends with `-proxysql-peer-endpoints` / `PROXYSQL_PEER_ENDPOINTS`, e.g. `db-a=10.0.0.1:3306,db-b=10.0.0.2:3306`. Unmapped peers use the peer name as hostname and port `3306`.

## Binary

- **`mysql-guard`** — long-running guard (`--mode observe` by default)
- **`mysql-guard switchover|handover <node>`** — planned handover (GTID-equal + errant checks; full SQL steps TODO)
- **`mysql-guard rejoin <source>`** — errant check + auto-position vs needs-rebuild plan

## Modes

| Mode | Behaviour |
|------|-----------|
| `observe` | Full decision loop; log and metrics only (`mysqlguard_would_act_total`) |
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
| Consul leader lock + renewal goroutine | Implemented (simplified lock acquire) |
| Epoch CAS + fenced KV markers | Implemented |
| GTID gate + peer offset election | Implemented (GTID parser is simplified) |
| Vote streaks (V1 probe wired; V2/V3 stubbed in health loop) | Partial |
| ProxySQL admin on promote/fence | Implemented behind `--proxysql-admin-enable` |
| Errant GTID before promote/rejoin | Implemented + unit tests |
| Rejoin plan (auto-position vs needs-rebuild) | Implemented; SQL execution TODO |
| Replication alerts → ntfy + Consul TTL checks | Implemented (needs `CONSUL_SERVICE_ID`) |
| Maintenance mode KV | Implemented |
| `/status` topology (primary, epoch, nodes, guards) | Implemented (guard discovery TODO) |
| Planned switchover / rejoin CLI | Partial |
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
