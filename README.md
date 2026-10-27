# mysql-guard

A failover guard for a two-node MySQL pair using asynchronous GTID replication. It watches the pair, and when the primary fails it promotes the replica. Consul handles coordination.

Latest release: **v0.3.4**. Releases are listed under [Releases](#releases).

## What it is for

- **Setup:** one primary and one replica (MySQL 8, GTID, `SOURCE_AUTO_POSITION`, async replication), with ProxySQL in front and Consul for coordination.
- **Sole promoter:** the guard is the only component allowed to promote a node or change the writer. Every other tool (replication-manager, old scripts) must be set to manual or no-op. If anything else writes the writer key, the guard raises an alert.
- **One guard per node, plus a witness:** each DB host runs a `db` guard, and a third host runs a `witness`. Only `db` guards compete for the Consul leader lock. The current leader is the only guard that acts.
- **Safety:** the guard never promotes a node that is missing GTIDs. A writer change bumps an epoch with a Consul CAS write, so a stale writer can be recognised and fenced.

## What it does

| Area | Behaviour |
|---|---|
| Monitoring | Every `-interval` (default 2s), each `db` guard probes MySQL and reads replication state (IO/SQL threads, lag, `read_only`, GTID sets). It publishes a heartbeat to Consul and serves `/status`, `/metrics` and a read-only web UI. |
| Failover | Applies only in `act` mode, on the leader. Failover starts when at least `-votes-needed` guards report the primary dead for 3 consecutive ticks. It also starts when a reachable primary stays read-only for longer than `-read-only-grace` and lacks data. The leader then promotes the most advanced replica that passes all gates: not stalled, no errant GTIDs, executed+retrieved is a superset of the last published writer GTIDs, and retrieved GTIDs applied (`-lag-gate-max-wait`, 0 = never promote with missing GTIDs). Failovers are rate-limited by `-min-between-failovers` (default 1h). |
| Restore writer | A primary that is read-only but has complete data is made writable again at epoch+1, with no failover. This is not rate-limited. |
| Switchover | Run as `mysql-guard switchover <target>`. Steps: fence the old primary (`super_read_only`, ProxySQL offline, drain open transactions for `-fence-drain-timeout`, then kill them), wait for the target to catch up (`-switchover-catchup-timeout`), run an errant-GTID check both ways, then bump the epoch and write the key. Any failure before the epoch bump rolls back. A key-write failure after the bump also rolls back, and the alert is `switchover-aborted`. |
| Rejoin | Run as `mysql-guard rejoin <source>`. It runs an errant-GTID check, then either re-points this node with auto-position or marks it `needs-rebuild` (`mysql/<cluster>/guard/needs-rebuild/<node>`). |
| Fencing | Applies in `fence` and `act` modes. Any non-primary that stays writable for 2 ticks is set to `super_read_only=ON`. |
| Writer key | Writes `mysql/master/<cluster>` (format chosen by `-master-format`). On an empty cluster, the leader seeds the key when every peer is reachable, exactly one peer is writable, and no other peer has GTIDs the writer lacks. |
| Writer service | When `-writer-service` is set, the leader keeps a Consul catalog service on node `<name>-vnode` pointing at the primary, with Meta `cluster`, `node`, `epoch` and `promoted_by=mysql-guard`. |
| ProxySQL | By default ProxySQL follows `read_only` and the guard never writes to ProxySQL. With `-proxysql-admin-enable` (or `-proxysql-mode=admin`), it updates `mysql_servers` on promote and fence. With `-proxysql-verify`, it only reads `runtime_mysql_servers` and alerts on a writer mismatch. |
| Alerts | Sends ntfy notifications (`-ntfy-url`) and updates the Consul TTL check `<service-id>:guard`. You get one alert per event. See [Reading alerts](#reading-alerts). |
| Maintenance | While `mysql/<cluster>/guard/maintenance` exists (any value), the guard takes no fence, failover, restore or writer-service actions. |

## What it does NOT do

- No Group Replication, InnoDB Cluster, semi-sync management, or setups with more than one writer. It handles async GTID replication only.
- No backups, rebuilds or clones. A `needs-rebuild` node has to be rebuilt by hand.
- It does not skip replication errors automatically.
- In **`observe` mode** (the default), it never writes cluster state: no master key, epoch, `writer/current`, `writer/last-gtid`, writer service, MySQL `read_only`, or ProxySQL. It still runs the full decision loop and logs what it would do (`mysql_guard_would_act_total`). It does write its own bookkeeping: the leader lock/session, instance heartbeats and the action log.
- The **witness** never connects to MySQL: no probes, no replication alerts, no leader lock. It only takes part in Consul coordination (`ChecksFor(witness)` has no checks).
- `fence` mode fences stray writers but never promotes.

## How it works

```
             +-------------- Consul KV / sessions --------------+
             |  leader lock, epoch, writer key, heartbeats, log |
             +----^------------------^------------------^-------+
                  |                  |                  |
          guard (db, node A)  guard (db, node B)  guard (witness)
                  |                  |          (connect check only)
              MySQL A  ==async GTID==>  MySQL B
                  ^                  ^
                  +---- ProxySQL ----+   (follows read_only, or admin mode)
```

Roles, as set by `ChecksFor` in `internal/guard/runtime.go`:

| Role | MySQL probes | Replication alerts | Competes for leader | Promotable |
|---|---|---|---|---|
| `db` | yes | yes | yes | yes |
| `witness` | no | no | no | no |

Each guard runs three independent loops: lock renewal (Consul session), health (probes, votes, alerts, leader decisions) and leader-watch.

### Failover quorum

A failover tick counts only with at least `-votes-needed` (default 2) votes; `-dead-after` must also pass and the primary must look dead on 3 consecutive ticks. Every voter needs a fresh heartbeat (at most 3 x `-interval` + 2 x `-probe-timeout` + 1s old, so a guard slowed by a dead primary still votes) and must see the same primary. One vote each:

| Voter | Vote "dead" when |
|---|---|
| each `db` guard (`<node>:probe`) | its MySQL probe of the primary fails |
| the witness (`<witness>:witness`) | its MySQL connect check of the primary fails. A server that answers with an error (e.g. access denied) counts as alive. |
| one replica IO vote (`<node>:io`) | a replica's IO thread lost the primary **and** the primary's own guard heartbeat is stale or missing **and** no fresh witness reports the primary reachable |

The IO vote only counts when the whole primary host looks gone (its guard is silent too), so a replica that is merely cut off from the primary cannot fail it over: the fresh primary guard, or the witness, vetoes it. The IO vote may come from the same replica as a probe vote; that is what lets a two-node pair survive whole-host loss when the witness cannot probe MySQL. The voters behind each decision are in `/status` (`voters`) and in the failover alert (`votes: ...`).

Then the existing gates still apply (lag/GTID gate, errant GTIDs, stall, `-min-between-failovers`, maintenance), plus the semi-sync gate.

### Semi-sync

Each guard reads the semi-sync variables and status (`rpl_semi_sync_source_*`, or the legacy `master`/`slave` names). The leader keeps the primary's last state in `mysql/<cluster>/guard/semisync`.

- **Fallback to async** on the primary raises a `semisync_async` alert (after `-repl-alert-delay`; held during a failover follow-up).
- **Failover alert** includes `semi-sync at loss: <sync|async|off|unknown> (last sample Ns before)`.
- **Gate:** if semi-sync was enabled on the primary and it was last seen async, automatic failover is refused (logged `semisync_gate`, one alert per primary and epoch): acknowledged writes may exist only on the lost primary. Disable with `-semisync-gate=false`.
- **Restore:** the promoted node gets semi-sync source OFF (no replica to acknowledge yet) and the record is marked pending. When a replica (re)joins, it gets replica ON before `START REPLICA`, and the source gets source ON once the replica is connected. The same happens at switchover whenever the cluster runs semi-sync (the record says enabled or pending, or either node has source enabled).

### Member service and health checks

With `-member-service <svc>` (act mode only), the leader keeps one catalog entry per member, each on its own external node `<svc>-<node>-vnode`, pointing at the member's `host:port` from `-mysql-peer-endpoints`:

| Tag | When | Check `<svc>:<node>:mysql` |
|---|---|---|
| `writer` | current primary | passing when writable; critical when unreachable or read-only |
| `reader` | replica with both threads ON and lag <= `-member-max-lag` | passing |
| `lagging` | replica lagging more than `-member-max-lag` (`Seconds_Behind_Source`) | warning |
| `fenced` | unreachable, replication stopped or broken, stalled, writable while not primary, or marked needs-rebuild | critical |

Metadata: `cluster`, `node`, `epoch`, `role`. So `writer.<svc>.service.consul` and `reader.<svc>.service.consul` resolve to the writer and the healthy readers (DNS returns only passing and, by default, warning entries; `lagging`/`fenced` carry no `reader` tag). With `-writer-service`, the writer entry gets the same kind of check (`<writer-service>:mysql`); its behaviour is otherwise unchanged. In the tick that changes the writer (failover or seed), the writer and member entries are not rewritten; the next tick publishes the new topology, so the old primary is never re-published.

- **Ordering:** writes happen only on the elected leader, and each write first re-reads the epoch and is skipped if it moved on. A promotion rewrites all entries at once in the same step as the writer key (new writer passing, every other member fenced); members return to `reader` once they replicate from the new primary.
- **Debounce:** a role or status change must persist for `-member-check-delay` (10s) before it is written. Losing the primary, needs-rebuild and a writable non-primary are written immediately.
- **If the leader dies:** catalog checks have no TTL, so two things keep them from staying passing forever. (1) The leader rewrites every check at least every 30s, and each check output begins `as-of=<time> epoch=<n> by=<leader> stale-after=90s:`, so anything older than 90s is stale. (2) The leader lock is a Consul session (`-session-ttl`), so another db guard takes over within that TTL and rewrites the checks from its own view. If no guard is alive at all, the entries keep their last state; monitor the `as-of` age.


### Consul key layout

| Key | Written by | Meaning |
|---|---|---|
| `mysql/master/<cluster>` | leader (act/fence) | Current writer, as `node\|epoch` (epoch format) or `host:port` (legacy format) |
| `mysql/<cluster>/writer/epoch` | leader | Epoch counter, changed with CAS |
| `mysql/<cluster>/writer/current` | leader | `node\|epoch` (used with the legacy format) |
| `mysql/<cluster>/writer/last-gtid` | leader | Last published writer `gtid_executed`, used by the superset gate |
| `mysql/<cluster>/guard/leader` | db guards | Leader lock (session) |
| `mysql/<cluster>/guard/instance/<node>` | every guard | Heartbeat and vote |
| `mysql/<cluster>/guard/gtid/<node>` | guards | Per-node GTID state |
| `mysql/<cluster>/guard/fenced/<node>` | leader | Fence marker |
| `mysql/<cluster>/guard/last-failover` | leader | Rate-limit timestamp |
| `mysql/<cluster>/guard/transition` | leader / CLI | Switchover or failover in progress (used to hold alerts) |
| `mysql/<cluster>/guard/key-repair` | CLI | `node\|epoch` for the leader to rewrite after an aborted switchover |
| `mysql/<cluster>/guard/needs-rebuild/<node>` | rejoin | Node must be rebuilt |
| `mysql/<cluster>/guard/maintenance` | operator | Exists = pause all actions |
| `mysql/<cluster>/actions` | guards | Action log (CAS append, capped at `-action-log-max`) |

## Configuration

Every option is a flag. Where an env var column has a value, the env var sets the default and the flag overrides it. Single (`-x`) and double (`--x`) dashes both work. `-cluster`, `-node` and `-peers` are required.

### Identity and mode

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-cluster` | `GUARD_CLUSTER` | (required) | Cluster name used in KV keys |
| `-node` | `GUARD_NODE` | (required) | This guard's logical name. On a `db` guard it must match a peer name. |
| `-peers` | `GUARD_PEERS` | (required) | Comma-separated DB peer names, e.g. `db-a,db-b` |
| `-role` | `GUARD_ROLE` | `db` | `db` or `witness` |
| `-mode` | `GUARD_MODE` | `observe` | `observe`, `fence` or `act` |
| `-http` | `GUARD_HTTP` | `:9470` | Listen address for UI, `/status`, `/metrics`, `/healthz` |

### MySQL

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-mysql-dsn` | `MYSQL_DSN` | | Full local DSN. Leave the password out if you use a password file. |
| `-mysql-host` | `MYSQL_HOST` | `127.0.0.1` | Local host, used when the DSN is built from `-mysql-user` |
| `-mysql-port` | `MYSQL_PORT` | `3306` | Local port, and the default port for peers |
| `-mysql-user` | `MYSQL_USER` | | Guard (admin) user |
| `-mysql-password-file` | `MYSQL_PASSWORD_FILE` | | File containing the guard password. If unset, `MYSQL_PASSWORD` (env only) is used. |
| `-mysql-repl-user` | `MYSQL_REPL_USER` | (guard user) | Account used in `CHANGE REPLICATION SOURCE` |
| `-mysql-repl-password-file` | `MYSQL_REPL_PASSWORD_FILE` | | Replication password file. If unset, `MYSQL_REPL_PASSWORD` (env only) is used. |
| `-mysql-peer-endpoints` | `MYSQL_PEER_ENDPOINTS` | | `db-a=host:port,db-b=host:port`. Used for remote peer connections and to map `host:port` to a node in the legacy key format. |
| `-mysql-tls` | `MYSQL_TLS` | `false` | Use TLS for peer connections |
| `-mysql-ca-file` | `MYSQL_CA_FILE` | | CA PEM for peer TLS |
| `-repl-stmt-timeout` | `REPL_STMT_TIMEOUT` | `30s` | Timeout for replication statements (CHANGE SOURCE, START/STOP/RESET REPLICA) |
| `-repl-connect-timeout` | `REPL_CONNECT_TIMEOUT` | `30s` | Maximum wait for a re-pointed replica's IO and SQL threads to come up |

### Consul

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-consul-addr` | `CONSUL_HTTP_ADDR` | `127.0.0.1:8500` | Consul HTTP address |
| `-consul-token-file` | `CONSUL_TOKEN_FILE` | | ACL token file. If unset, `CONSUL_HTTP_TOKEN` (env only) is used. |
| `-consul-service-id` | `CONSUL_SERVICE_ID` | | Registers an agent service with the TTL check `<id>:guard`. Recommended. |
| `-consul-service-name` | `CONSUL_SERVICE_NAME` | `mysql-guard` | Service name for that registration |
| `-session-ttl` | | `15s` | Leader session TTL |
| `-lock-delay` | | `15s` | Consul lock delay |
| `-master-format` | `MASTER_FORMAT` | `epoch` | Write format of `mysql/master/<cluster>`: `epoch` or `legacy`. Reads always accept both. |
| `-writer-service` | `WRITER_SERVICE` | (off) | Name of the catalog service kept pointing at the writer |
| `-writer-service-tags` | `WRITER_SERVICE_TAGS` | `mysql,writer` | Tags for that service |
| `-member-service` | `MEMBER_SERVICE` | (off) | Catalog service with one entry per member (see *Member service*) |
| `-member-max-lag` | `MEMBER_MAX_LAG` | `30` | Seconds of lag above which a replica is `lagging` (check warning, out of reader DNS) |
| `-member-check-delay` | `MEMBER_CHECK_DELAY` | `10s` | A member role/check change must persist this long before it is written. Promotion and fencing are immediate. |
| `-action-log-max` | | `500` | Maximum entries in `mysql/<cluster>/actions` |

### Detection and failover

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-interval` | | `2s` | Health loop interval |
| `-probe-timeout` | | `3s` | Deadline for each probe |
| `-slow-threshold` | | `4` | Consecutive slow probes before a probe counts as failed |
| `-dead-after` | | `30s` | Time since the first failure before a vote counts as dead |
| `-votes-needed` | | `2` | Dead votes required before a failover tick counts |
| `-min-between-failovers` | | `1h` | Minimum time between real failovers |
| `-max-apply-lag` | | `30s` | Time allowed to apply retrieved GTIDs |
| `-lag-gate-max-wait` | | `0` | Maximum wait for missing GTIDs. `0` = never promote while GTIDs are missing. |
| `-fence-grace` | | | **Removed in v0.3.6.** It never had an effect; the flag is still accepted (and ignored) so old job files keep starting. |
| `-read-only-grace` | | `10s` | A reachable primary that is read-only for this long counts as failed (restored if its data is complete, otherwise failover) |
| `-replica-stale` | | `35s` | Replica IO vote: the IO thread has lost the primary when it is connecting/erroring, or has received no event or heartbeat for this long. Set `SOURCE_HEARTBEAT_PERIOD` well below it. |
| `-witness-probe` | `WITNESS_PROBE` | `true` | Witness: MySQL connect check of the primary with the `-mysql-user` login (a read-only `mg_*` user is enough). Without credentials the witness does not vote. |
| `-semisync-gate` | `SEMISYNC_GATE` | `true` | Refuse automatic failover when semi-sync was enabled on the primary but it was last seen running async |
| `-semisync-restore` | `SEMISYNC_RESTORE` | `true` | Re-enable semi-sync after a failover, switchover or rejoin when it was on before |
| `-repl-source-ssl` | `REPL_SOURCE_SSL` | `true` | `CHANGE REPLICATION SOURCE ... SOURCE_SSL=1` |
| `-stall-after` | `STALL_AFTER` | `30s` | A replica whose threads are ON but whose GTID gap does not shrink for this long is `stalled`. It raises an alert and is never a candidate. A replica that keeps applying transactions (a delayed or steadily lagging replica) is lag, not a stall. |
| `-fence-drain-timeout` | `FENCE_DRAIN_TIMEOUT` | `3s` | Switchover: wait for open transactions on the fenced source, then kill them |
| `-switchover-catchup-timeout` | `SWITCHOVER_CATCHUP_TIMEOUT` | `0` (= 2x `-max-apply-lag`) | Switchover: maximum wait for the target to catch up |

### ProxySQL

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-proxysql-mode` | `PROXYSQL_MODE` | `monitor` | `monitor` (no writes) or `admin` (same as `-proxysql-admin-enable`) |
| `-proxysql-admin-enable` | `PROXYSQL_ADMIN_ENABLE` | `false` | Update `mysql_servers` on promote and fence |
| `-proxysql-admin-dsn` | `PROXYSQL_ADMIN_DSN` | | ProxySQL admin DSN |
| `-proxysql-writer-hg` | | `10` | Writer hostgroup |
| `-proxysql-reader-hg` | | `11` | Reader hostgroup |
| `-proxysql-peer-endpoints` | `PROXYSQL_PEER_ENDPOINTS` | | `db-a=host:port,...` mapping to `mysql_servers` rows. Unmapped peers are matched as `<peer>:3306`. |
| `-proxysql-verify` | `PROXYSQL_VERIFY` | `false` | Read-only comparison of the runtime writer hostgroup with the writer key. Alerts only. |
| `-proxysql-verify-grace` | `PROXYSQL_VERIFY_GRACE` | `20s` | How long a mismatch must last before it alerts |

### Alerts

| Flag | Env | Default | Meaning |
|---|---|---|---|
| `-ntfy-url` | `NTFY_URL` | (off) | ntfy topic URL |
| `-ntfy-token-file` | `NTFY_TOKEN_FILE` | | Bearer token file |
| `-repl-lag-alert-sec` | | `30` | Lag alert threshold, in seconds |
| `-repl-alert-delay` | `REPL_ALERT_DELAY` | `45s` | A replication-health condition must persist this long before it alerts. Old names `-repl-thread-alert-delay` / `REPL_THREAD_ALERT_DELAY` still work. |
| `-fence-followup-window` | `FENCE_FOLLOWUP_WINDOW` | `30m` | After a failover, the old primary waiting to rejoin is fenced without a separate alert, and its replication alerts are held for this long |
| `-foreign-write-alert` | `FOREIGN_WRITE_ALERT` | `true` | Alert when something other than a guard writes the master key |

Boolean env vars accept `1`, `true` or `yes`.

## Required MySQL grants and Consul policy

### MySQL (guard user, on every DB node)

The witness needs only a login that can connect (`USAGE`, e.g. the read-only `mg_*` user) for its connect check; without one it does not vote.

The examples below use placeholders (`<guard-user>`, `<host>`).

**observe mode:** read-only access to replication and GTID state:

```sql
GRANT PROCESS, REPLICATION CLIENT ON *.* TO '<guard-user>'@'<host>';
GRANT SELECT ON performance_schema.* TO '<guard-user>'@'<host>';
```

**fence / act mode:** additionally needs to set `read_only` / `super_read_only`, run STOP/START/RESET REPLICA and CHANGE REPLICATION SOURCE, and kill sessions while fencing:

```sql
GRANT SYSTEM_VARIABLES_ADMIN, REPLICATION_SLAVE_ADMIN, CONNECTION_ADMIN ON *.* TO '<guard-user>'@'<host>';
-- SUPER also covers these, but is deprecated in MySQL 8
```

The replication account (`-mysql-repl-user`, or the guard user if that is unset) needs `REPLICATION SLAVE`.

For `-proxysql-verify`, use a dedicated ProxySQL admin user. The guard only runs `SELECT`s, but ProxySQL has no read-only admin role.

### Consul ACL

**observe mode:** the guard reads cluster state and writes only its own bookkeeping:

```hcl
key_prefix "mysql/master/<cluster>"  { policy = "read" }
key_prefix "mysql/<cluster>/writer/" { policy = "read" }
key_prefix "mysql/<cluster>/guard/"  { policy = "write" }  # leader lock, instance heartbeats
key_prefix "mysql/<cluster>/actions" { policy = "write" }
session_prefix "" { policy = "write" }
# only if -consul-service-id is set:
service "mysql-guard" { policy = "write" }
node_prefix "" { policy = "read" }
```

**act / fence mode:** add write access to the writer key and the writer service:

```hcl
key_prefix "mysql/master/<cluster>"  { policy = "write" }
key_prefix "mysql/<cluster>/writer/" { policy = "write" }
# only if -writer-service is set:
service "<writer-service>"      { policy = "write" }
node    "<writer-service>-vnode" { policy = "write" }   # also covers the <writer-service>:mysql check
# only if -member-service is set:
service     "<member-service>"  { policy = "write" }
node_prefix "<member-service>-" { policy = "write" }   # vnodes <member-service>-<node>-vnode and their checks
```

## Deploying with Nomad

Release assets are static Linux binaries: `mysql-guard-<version>-linux-amd64.tar.gz` and `mysql-guard-<version>-linux-arm64.tar.gz`, each with a `.sha256` file. The repository is private, so host the tarball where your Nomad clients can fetch it. Pass secrets as files rendered from your secret store, never as literals.

The job below is minimal and uses placeholders only. Run one `db` guard on each DB host and one witness on a third host. For arm64 hosts, swap the artifact for the arm64 asset and the `cpu.arch` constraint value.

```hcl
job "mysql-guard-<cluster>" {
  datacenters = ["<dc>"]

  group "db" {
    count = 2
    constraint { distinct_hosts = true }
    constraint {
      attribute = "${attr.cpu.arch}"
      value     = "amd64"                     # arm64 hosts: "arm64"
    }

    task "guard" {
      driver = "exec"

      artifact {
        # arm64: mysql-guard-v0.3.4-linux-arm64.tar.gz
        source      = "<artifact-base-url>/mysql-guard-v0.3.4-linux-amd64.tar.gz"
        destination = "local/"
        options { checksum = "sha256:<sha256-from-release>" }
      }

      config {
        command = "local/mysql-guard"
        args = [
          "-cluster=<cluster>",
          "-node=${node.unique.name}",        # must match a peer name
          "-peers=<db-a>,<db-b>",
          "-role=db",
          "-mode=observe",                    # act once the shadow run is clean
          "-mysql-user=<guard-user>",
          "-mysql-password-file=${NOMAD_SECRETS_DIR}/mysql_password",
          "-mysql-peer-endpoints=<db-a>=<host-a>:3306,<db-b>=<host-b>:3306",
          "-consul-token-file=${NOMAD_SECRETS_DIR}/consul_token",
          "-consul-service-id=mysql-guard-${node.unique.name}",
          "-ntfy-url=<ntfy-topic-url>",
        ]
      }

      # template blocks that render mysql_password / consul_token into secrets/

      resources {
        cpu    = 100
        memory = 64
      }
    }
  }

  group "witness" {
    count = 1
    constraint {
      attribute = "${attr.cpu.arch}"
      value     = "arm64"                     # or amd64
    }

    task "guard" {
      driver = "exec"
      artifact {
        source      = "<artifact-base-url>/mysql-guard-v0.3.4-linux-arm64.tar.gz"
        destination = "local/"
        options { checksum = "sha256:<sha256-from-release>" }
      }
      config {
        command = "local/mysql-guard"
        args = [
          "-cluster=<cluster>",
          "-node=witness-1",
          "-peers=<db-a>,<db-b>",
          "-role=witness",
          "-consul-token-file=${NOMAD_SECRETS_DIR}/consul_token",
        ]
      }
      resources {
        cpu    = 50
        memory = 32
      }
    }
  }
}
```

`examples/nomad/mysql-guard.hcl` is an older docker-based sketch.

### Migrating from the bash guard / replication-manager

1. Run the guards with `-mode=observe -master-format=legacy` next to the existing tooling, and compare their decisions.
2. Set replication-manager to `failover-mode = "manual"` and replace its post-script with `examples/repman-failover-post-script-noop.sh`. Stop the bash guard.
3. Run `mysql-guard migrate-key -to=legacy <guard flags>` once. This initialises `writer/epoch` and `writer/current` without changing the key value. `-dry-run` prints the result, and the command is refused in observe mode.
4. Switch to `-mode=act`. Later, once every consumer understands `node|epoch`, you can run `migrate-key -to=epoch` and set `-master-format=epoch`.

## Operations

All CLI commands take the same flags as the daemon, in any position. Run them with the guard's own config on a DB host. Boolean flags take no value (`-mysql-tls`); use `-flag=false` to turn one off.

**Maintenance mode** (pauses every action; monitoring and alerts continue):

```bash
consul kv put mysql/<cluster>/guard/maintenance "<reason>"
consul kv delete mysql/<cluster>/guard/maintenance
```

**Planned switchover:**

```bash
mysql-guard switchover <target-node> -cluster=<cluster> -peers=<db-a>,<db-b> -node=<this-node> -mode=act ...
# "handover" is an alias. With no target, the last peer in -peers is used.
```

**Rejoin** an old primary as a replica (run on that node):

```bash
mysql-guard rejoin <source-node> ...flags...
```

If there are errant GTIDs, the node is marked `needs-rebuild` rather than re-pointed.

**HTTP endpoints** (on `-http`, default `:9470`):

| Path | Content |
|---|---|
| `/` | Read-only web UI |
| `/status` | JSON: primary, epoch, nodes (GTID, lag, stalled), guards, leader, last decision |
| `/api/actions` | Action log as a JSON array (`[]` when empty) |
| `/metrics` | Prometheus metrics, e.g. `mysql_guard_would_act_total` |
| `/healthz` | Liveness, always 200 |

### Reading alerts

Each ntfy alert has the title `mysql-guard <cluster>: <code>`. Each event produces one alert.

| Code | When |
|---|---|
| `failover`, `switchover`, `switchover-aborted`, `restore_writer`, `errant_gtid`, `needs-rebuild` | Sent immediately, once per event |
| `repl_io_stopped`, `repl_sql_stopped`, `primary_read_only`, `read_only_drift`, `repl_lag`, `repl_stalled`, `repl_state_unknown` | Held while a switchover or failover is in progress (marker under 2 minutes old) and for 20s after it. After that, sent only once the condition has lasted `-repl-alert-delay`. For a fenced old primary waiting to rejoin, held for `-fence-followup-window` (30m), then sent once. |
| `fence` | Sent for an unexpected writer. Fencing a returning old primary is folded into `failover`. |
| `foreign_master_write` / `_cleared` | Something other than a guard wrote the master key (after 2 ticks) |
| `proxysql_writer_mismatch` / `proxysql_writer_ok` | `-proxysql-verify` mismatch that lasted the grace period |
| `writer_service_sync_failed` | The writer catalog service could not be updated (retried each leader tick) |

If an alert persists, check `/status` and `/api/actions` first. A `needs-rebuild` node must be re-cloned from the primary. A `foreign_master_write` means another tool is still writing the key.

## Testing and drills

```bash
go vet ./...
go test ./...
./integration/scripts/drill.sh             # docker-compose pair + witness, basic failover drills
./integration/scripts/switchover-drill.sh  # switchover under load
./integration/scripts/v030-drill.sh        # legacy key, writer service, ProxySQL verify, alert counts
./integration/scripts/v036-drill.sh        # D1-D5: host loss, partitions, witness loss, semi-sync, member DNS
```

The drills run MySQL, ProxySQL and Consul locally in Docker (`integration/docker-compose*.yml`). Fault injection for rollback paths needs `go build -tags faultinject`. Release builds contain no fault hooks.

## Releases

Releases are tagged `vX.Y.Z` on GitHub, with `linux-amd64` and `linux-arm64` tarballs plus `.sha256` files. The version is set with `-ldflags "-X main.version=<tag>"`. `make release` builds the linux amd64 and arm64 tarballs into `dist/` with a `SHA256SUMS` file. The release notes describe what changed in each version.

## License

MIT. See [LICENSE](LICENSE).
