# TODO

Open ideas for mysql-guard. Nothing here is implemented yet.

## Replica lifecycle

- [ ] **`mysql-guard join --node <new> --from <source>`**: add a new replica to a running cluster.
  - Dry-run by default (prints the plan, changes nothing); `--dry-run=false` executes.
  - Preflight: the new node is empty (no user schemas, no GTIDs executed), and its version and config match the source (version, `server_id` unique, GTID mode, binlog format, `default_table_encryption`, semi-sync plugins).
  - Seed from `--from` with the CLONE plugin (needs the clone plugin installed and `BACKUP_ADMIN` / `CLONE_ADMIN` for the guard user), with a wal-g restore as the fallback when CLONE is not possible.
  - Point the node at the primary with GTID auto-position and wait until it has caught up (lag 0, no GTID gap).
  - Only then add it as a cluster member and into the ProxySQL reader hostgroup.
- [ ] **`rejoin`**: bring a former primary or a long-down replica back.
  - GTID catch-up first.
  - Fall back to a CLONE reseed when the binlogs it needs are already purged on the primary.
- [ ] **Unrecoverable-replica detection**: compare the replica's executed GTIDs with the primary's `gtid_purged`. If the replica is missing GTIDs the primary no longer has, raise an alert `needs reseed` (and expose it on `/status`).
- [ ] **Binlog-runway warning**: warn before a replica's position gets close to the primary's oldest binlog (estimate from replication lag/stall and binlog purge rate), so a stopped replica is noticed before it becomes unrecoverable.
- [ ] **Lag-based removal from the ProxySQL read hostgroup**: take a lagging replica out of the reader hostgroup above a lag threshold and put it back once it has caught up (with hysteresis).

## Notes

- Much of the above becomes moot if we move to MySQL Group Replication (built-in distributed recovery with clone, membership and automatic expulsion). Revisit before building any of it.
