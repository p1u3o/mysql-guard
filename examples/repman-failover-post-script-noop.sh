#!/bin/sh
# replication-manager failover-post-script when mysql-guard is the sole promoter.
# mysql-guard owns mysql/master/<cluster> and the writer service; repman must not
# write them (keep failover-mode = "manual"). Log only.
logger -t repman-post-script "repman failover post-script called ($*); ignored: mysql-guard is the promoter" 2>/dev/null || true
exit 0
