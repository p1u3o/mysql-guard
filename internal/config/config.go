package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Role is where this guard instance runs.
type Role string

const (
	RoleDB      Role = "db"
	RoleWitness Role = "witness"
)

// Mode controls whether the guard acts on decisions.
type Mode string

const (
	ModeObserve Mode = "observe"
	ModeFence   Mode = "fence"
	ModeAct     Mode = "act"
)

// ProxySQLMode is how the guard interacts with ProxySQL.
type ProxySQLMode string

const (
	ProxySQLMonitor ProxySQLMode = "monitor"
	ProxySQLAdmin   ProxySQLMode = "admin"
)

// Config holds runtime settings (flags and environment).
type Config struct {
	NodeName string
	Role     Role
	Mode     Mode

	MySQLDSN           string
	MySQLHost          string
	MySQLPort          int
	MySQLAdminUser     string
	MySQLAdminPass     string
	MySQLAdminPassFile string

	// Replication account used in CHANGE REPLICATION SOURCE (switchover/rejoin).
	// Empty user falls back to the admin (guard) account.
	MySQLReplUser      string
	MySQLReplPass      string
	MySQLReplPassFile  string
	ReplConnectTimeout time.Duration
	// ReplStmtTimeout bounds replication statements (CHANGE REPLICATION SOURCE,
	// START/STOP/RESET REPLICA) and their state reads; it is also the driver
	// read/write timeout of the dedicated replication connection pool.
	ReplStmtTimeout time.Duration

	ConsulAddr      string
	ConsulTokenFile string
	ConsulToken     string

	ClusterName string
	Peers       []string // mysql node names in the pair

	HTTPListen string

	Interval       time.Duration
	ProbeTimeout   time.Duration
	SlowThreshold  int
	DeadAfter      time.Duration
	VotesNeeded    int
	SessionTTL     time.Duration
	LockDelay      time.Duration
	MinBetweenFail time.Duration
	MaxApplyLag    time.Duration
	// v0.3.1 switchover: drain wait for open transactions on the fenced source,
	// and max wait for the target to reach the source's final GTID set.
	FenceDrainTimeout        time.Duration
	SwitchoverCatchupTimeout time.Duration
	LagGateMaxWait           time.Duration
	ReadOnlyGrace            time.Duration
	// ReplicaStale: a replica whose IO thread received nothing (no event and no
	// heartbeat) for this long, or whose IO thread is connecting/erroring, has
	// "lost the primary" (v0.3.6 replica IO vote, see quorum.go).
	ReplicaStale        time.Duration
	ProxySQLMode        ProxySQLMode
	ProxySQLAdminDSN    string
	ProxySQLAdminEnable bool
	ProxySQLWriterHG    int
	ProxySQLReaderHG    int

	NtfyURL              string
	ReplLagAlertSec      float64
	ReplThreadAlertDelay time.Duration
	FenceFollowupWindow  time.Duration
	StallAfter           time.Duration
	ConsulServiceID      string
	ConsulServiceName    string

	ActionLogMax int

	ProxySQLPeerEndpoints map[string]string // peer name -> host:port for mysql_servers rows
	MySQLPeerEndpoints    map[string]string // peer name -> host:port for admin connections
	MySQLTLS              bool
	MySQLCAFile           string

	Promotable bool

	// v0.3.0
	MasterFormat        string // epoch | legacy
	WriterServiceName   string // Consul catalog service pointing at the writer ("" = off)
	WriterServiceTags   []string
	MemberServiceName   string        // v0.3.6: per-member catalog entries tagged writer/reader/lagging/fenced ("" = off)
	MemberMaxLag        float64       // reader lag limit (seconds) for the member checks
	MemberCheckDelay    time.Duration // debounce: a member state must persist this long before it is written
	ProxySQLVerify      bool          // read-only check of the ProxySQL writer hostgroup via admin
	ProxySQLVerifyGrace time.Duration // mismatch must persist this long before alerting
	ForeignWriteAlert   bool          // alert when the master key was written by something other than a guard
	NtfyTokenFile       string
	NtfyToken           string

	// v0.3.6
	WitnessProbe    bool // witness probes the primary with a MySQL connect check (needs -mysql-user and -mysql-peer-endpoints)
	SemiSyncGate    bool // refuse automatic failover when acknowledged writes could be lost (semi-sync enabled but last seen async)
	SemiSyncRestore bool // re-enable semi-sync on the new primary/replica after failover, switchover and rejoin
	ReplSourceSSL   bool // CHANGE REPLICATION SOURCE ... SOURCE_SSL=1

	// semi-sync stall handling (off by default, see README "Semi-sync stall handling")
	SemiSyncStall       bool    // leader turns semi-sync source off when the replica is confirmed dead, back on when it is caught up
	SemiSyncStallProbes int     // consecutive leader ticks the replica must be confirmed dead before semi-sync is turned off
	SemiSyncReenable    int     // consecutive leader ticks the replica must be healthy and caught up before semi-sync is turned on again
	SemiSyncMaxLag      float64 // re-enable only when the replica lags at most this many seconds

	// planned switchover
	SwitchoverProxyWait time.Duration // how long to wait for ProxySQL's writer hostgroup to show the new primary (monitor mode: read-only check)
	SwitchoverTo        string        // switchover --to <node>
	SwitchoverDryRun    bool          // switchover prints its plan and preflight and changes nothing (default true)
}

// ProxySQLConfig is passed to the ProxySQL client.
func (c *Config) ProxySQLConfig() ProxySQLSettings {
	admin := c.ProxySQLAdminEnable || c.ProxySQLMode == ProxySQLAdmin
	return ProxySQLSettings{
		AdminEnabled:    admin,
		Verify:          c.ProxySQLVerify,
		AdminDSN:        c.ProxySQLAdminDSN,
		WriterHostgroup: c.ProxySQLWriterHG,
		ReaderHostgroup: c.ProxySQLReaderHG,
		PeerEndpoints:   c.ProxySQLPeerEndpoints,
	}
}

// ProxySQLSettings for the proxysql package.
type ProxySQLSettings struct {
	AdminEnabled    bool
	Verify          bool
	AdminDSN        string
	WriterHostgroup int
	ReaderHostgroup int
	PeerEndpoints   map[string]string
}

// Load parses flags and environment. Credentials are never logged by callers.
func Load(args []string) (*Config, error) {
	fs := flag.NewFlagSet("mysql-guard", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)

	c := &Config{
		Mode:                 ModeObserve,
		Interval:             2 * time.Second,
		ProbeTimeout:         3 * time.Second,
		SlowThreshold:        4,
		DeadAfter:            30 * time.Second,
		VotesNeeded:          2,
		SessionTTL:           15 * time.Second,
		LockDelay:            15 * time.Second,
		MinBetweenFail:       time.Hour,
		MaxApplyLag:          30 * time.Second,
		FenceDrainTimeout:    3 * time.Second,
		LagGateMaxWait:       0,
		ReadOnlyGrace:        10 * time.Second,
		ReplicaStale:         35 * time.Second,
		ProxySQLMode:         ProxySQLMonitor,
		ProxySQLWriterHG:     10,
		ProxySQLReaderHG:     11,
		ReplLagAlertSec:      30,
		ReplThreadAlertDelay: 45 * time.Second,
		FenceFollowupWindow:  30 * time.Minute,
		StallAfter:           30 * time.Second,
		HTTPListen:           ":9470",
		ConsulAddr:           "127.0.0.1:8500",
		MySQLPort:            3306,
		ReplConnectTimeout:   30 * time.Second,
		ReplStmtTimeout:      30 * time.Second,
		ConsulServiceName:    "mysql-guard",
		ActionLogMax:         500,
	}

	raw := &rawFlags{}
	defineFlags(fs, c, raw)
	peers, writerTags, proxysqlPeers, mysqlPeers := "", "", "", ""
	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	peers, writerTags, proxysqlPeers, mysqlPeers = raw.peers, raw.writerTags, raw.proxysqlPeers, raw.mysqlPeers
	c.Peers = splitCSV(peers)
	c.WriterServiceTags = splitCSV(writerTags)
	if c.MasterFormat != "epoch" && c.MasterFormat != "legacy" {
		return nil, fmt.Errorf("-master-format must be epoch or legacy")
	}
	if c.NtfyTokenFile != "" {
		b, err := os.ReadFile(c.NtfyTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read ntfy token file: %w", err)
		}
		c.NtfyToken = strings.TrimSpace(string(b))
	}
	c.ProxySQLPeerEndpoints = parsePeerEndpoints(proxysqlPeers)
	c.MySQLPeerEndpoints = parsePeerEndpoints(mysqlPeers)
	if len(c.MySQLPeerEndpoints) == 0 && mysqlPeers == "" {
		c.MySQLPeerEndpoints = map[string]string{}
	}
	if c.ClusterName == "" {
		return nil, fmt.Errorf("--cluster or GUARD_CLUSTER is required")
	}
	if c.NodeName == "" {
		return nil, fmt.Errorf("--node or GUARD_NODE is required")
	}
	if len(c.Peers) == 0 {
		return nil, fmt.Errorf("--peers or GUARD_PEERS is required (comma-separated db host names)")
	}
	if c.Role != RoleDB && c.Role != RoleWitness {
		return nil, fmt.Errorf("invalid role %q", c.Role)
	}
	if c.Mode != ModeObserve && c.Mode != ModeFence && c.Mode != ModeAct {
		return nil, fmt.Errorf("invalid mode %q", c.Mode)
	}
	c.Promotable = c.Role == RoleDB

	if c.MySQLAdminPassFile != "" {
		b, err := os.ReadFile(c.MySQLAdminPassFile)
		if err != nil {
			return nil, fmt.Errorf("read mysql password file: %w", err)
		}
		c.MySQLAdminPass = strings.TrimSpace(string(b))
	} else {
		c.MySQLAdminPass = envOr("MYSQL_PASSWORD", "")
	}

	if c.MySQLReplUser != "" {
		if c.MySQLReplPassFile != "" {
			b, err := os.ReadFile(c.MySQLReplPassFile)
			if err != nil {
				return nil, fmt.Errorf("read mysql repl password file: %w", err)
			}
			c.MySQLReplPass = strings.TrimSpace(string(b))
		} else {
			c.MySQLReplPass = envOr("MYSQL_REPL_PASSWORD", "")
		}
	}

	if c.ConsulTokenFile != "" {
		b, err := os.ReadFile(c.ConsulTokenFile)
		if err != nil {
			return nil, fmt.Errorf("read consul token file: %w", err)
		}
		c.ConsulToken = strings.TrimSpace(string(b))
	} else {
		c.ConsulToken = envOr("CONSUL_HTTP_TOKEN", "")
	}

	if err := c.FinalizeMySQLDSN(); err != nil {
		return nil, err
	}

	return c, nil
}

func parsePeerEndpoints(s string) map[string]string {
	out := map[string]string{}
	for _, part := range splitCSV(s) {
		name, host, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		out[strings.TrimSpace(name)] = strings.TrimSpace(host)
	}
	return out
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v == "1" || strings.EqualFold(v, "true") || v == "yes"
}

// ReplCredentials returns the account used for CHANGE REPLICATION SOURCE:
// the dedicated replication user if set, otherwise the admin (guard) account.
func (c *Config) ReplCredentials() (user, pass string) {
	if c.MySQLReplUser != "" {
		return c.MySQLReplUser, c.MySQLReplPass
	}
	return c.MySQLAdminUser, c.MySQLAdminPass
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func splitCSV(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// CatchupTimeout is the switchover catch-up limit (default 2x MaxApplyLag).
func (c *Config) CatchupTimeout() time.Duration {
	if c.SwitchoverCatchupTimeout > 0 {
		return c.SwitchoverCatchupTimeout
	}
	if c.MaxApplyLag > 0 {
		return 2 * c.MaxApplyLag
	}
	return 60 * time.Second
}

// rawFlags holds flag values that Load post-processes.
type rawFlags struct {
	peers, writerTags, proxysqlPeers, mysqlPeers string
}

// defineFlags registers every guard flag on fs. It is shared by Load and by
// the subcommand argument splitter (to know which flags are booleans).
func defineFlags(fs *flag.FlagSet, c *Config, raw *rawFlags) {
	fs.StringVar(&c.NodeName, "node", envOr("GUARD_NODE", ""), "this node's logical name")
	fs.StringVar((*string)(&c.Role), "role", envOr("GUARD_ROLE", "db"), "db or witness")
	fs.StringVar((*string)(&c.Mode), "mode", envOr("GUARD_MODE", "observe"), "observe, fence, or act")
	fs.StringVar(&c.MySQLDSN, "mysql-dsn", envOr("MYSQL_DSN", ""), "full MySQL DSN override (must not include password if -mysql-password-file is set)")
	fs.StringVar(&c.MySQLHost, "mysql-host", envOr("MYSQL_HOST", "127.0.0.1"), "MySQL host when building DSN from -mysql-user")
	fs.IntVar(&c.MySQLPort, "mysql-port", envInt("MYSQL_PORT", c.MySQLPort), "MySQL port when building DSN (env MYSQL_PORT)")
	fs.StringVar(&c.MySQLReplUser, "mysql-repl-user", envOr("MYSQL_REPL_USER", ""), "replication account for CHANGE REPLICATION SOURCE (default: -mysql-user)")
	fs.StringVar(&c.MySQLReplPassFile, "mysql-repl-password-file", envOr("MYSQL_REPL_PASSWORD_FILE", ""), "file containing the replication account password")
	fs.DurationVar(&c.ReplStmtTimeout, "repl-stmt-timeout", envDuration("REPL_STMT_TIMEOUT", c.ReplStmtTimeout), "timeout for replication statements (CHANGE SOURCE, START/STOP/RESET REPLICA) and their connection pool")
	fs.DurationVar(&c.ReplConnectTimeout, "repl-connect-timeout", envDuration("REPL_CONNECT_TIMEOUT", c.ReplConnectTimeout), "max wait for re-pointed replica IO+SQL threads ON")
	fs.StringVar(&c.MySQLAdminUser, "mysql-user", envOr("MYSQL_USER", ""), "MySQL admin user")
	fs.StringVar(&c.MySQLAdminPassFile, "mysql-password-file", envOr("MYSQL_PASSWORD_FILE", ""), "file containing MySQL password")
	fs.StringVar(&c.ConsulAddr, "consul-addr", envOr("CONSUL_HTTP_ADDR", c.ConsulAddr), "Consul HTTP address")
	fs.StringVar(&c.ConsulTokenFile, "consul-token-file", envOr("CONSUL_TOKEN_FILE", ""), "Consul ACL token file")
	fs.StringVar(&c.ClusterName, "cluster", envOr("GUARD_CLUSTER", ""), "cluster name for KV keys (required)")
	fs.StringVar(&raw.peers, "peers", envOr("GUARD_PEERS", ""), "comma-separated db peer names (required)")
	fs.StringVar(&c.HTTPListen, "http", envOr("GUARD_HTTP", c.HTTPListen), "HTTP listen address")
	fs.DurationVar(&c.Interval, "interval", c.Interval, "health loop interval")
	fs.DurationVar(&c.ProbeTimeout, "probe-timeout", c.ProbeTimeout, "per-probe deadline")
	fs.IntVar(&c.SlowThreshold, "slow-threshold", c.SlowThreshold, "consecutive slow probes before failed")
	fs.DurationVar(&c.DeadAfter, "dead-after", c.DeadAfter, "time since first failure before vote counts dead")
	fs.IntVar(&c.VotesNeeded, "votes-needed", c.VotesNeeded, "dead votes required (voters: each db guard, the witness connect check, and the replica IO vote; see README Quorum)")
	fs.DurationVar(&c.SessionTTL, "session-ttl", c.SessionTTL, "Consul session TTL")
	fs.DurationVar(&c.LockDelay, "lock-delay", c.LockDelay, "Consul lock delay")
	fs.DurationVar(&c.MinBetweenFail, "min-between-failovers", c.MinBetweenFail, "rate limit between failovers")
	fs.DurationVar(&c.MaxApplyLag, "max-apply-lag", c.MaxApplyLag, "allowed time to apply retrieved GTIDs")
	fs.DurationVar(&c.FenceDrainTimeout, "fence-drain-timeout", envDuration("FENCE_DRAIN_TIMEOUT", c.FenceDrainTimeout), "switchover: wait for open transactions on the fenced source, then kill them")
	fs.DurationVar(&c.SwitchoverCatchupTimeout, "switchover-catchup-timeout", envDuration("SWITCHOVER_CATCHUP_TIMEOUT", c.SwitchoverCatchupTimeout), "switchover: max wait for the target to apply the fenced source's final GTID set (0 = 2x max-apply-lag); on timeout the source is made writable again, epoch unchanged")
	fs.DurationVar(&c.LagGateMaxWait, "lag-gate-max-wait", c.LagGateMaxWait, "max wait for missing GTIDs; 0=never promote")
	fs.Func("fence-grace", "removed in v0.3.6 (was never used); accepted and ignored so old job files still start", func(string) error { return nil })
	fs.DurationVar(&c.ReadOnlyGrace, "read-only-grace", c.ReadOnlyGrace, "reachable primary read-only this long counts as failed: restored if its GTIDs include every peer's, else failover")
	fs.DurationVar(&c.ReplicaStale, "replica-stale", c.ReplicaStale, "replica IO vote: the IO thread has lost the primary when it is connecting/erroring or has received no event or heartbeat for this long (set SOURCE_HEARTBEAT_PERIOD well below it)")
	fs.StringVar((*string)(&c.ProxySQLMode), "proxysql-mode", envOr("PROXYSQL_MODE", "monitor"), "monitor or admin")
	fs.StringVar(&c.ProxySQLAdminDSN, "proxysql-admin-dsn", envOr("PROXYSQL_ADMIN_DSN", ""), "ProxySQL admin DSN")
	fs.BoolVar(&c.ProxySQLAdminEnable, "proxysql-admin-enable", envBool("PROXYSQL_ADMIN_ENABLE", false), "apply ProxySQL admin hostgroup updates on promote/fence")
	fs.IntVar(&c.ProxySQLWriterHG, "proxysql-writer-hg", c.ProxySQLWriterHG, "writer hostgroup id")
	fs.IntVar(&c.ProxySQLReaderHG, "proxysql-reader-hg", c.ProxySQLReaderHG, "reader hostgroup id")
	fs.StringVar(&c.NtfyURL, "ntfy-url", envOr("NTFY_URL", ""), "ntfy topic URL for alerts")
	fs.DurationVar(&c.StallAfter, "stall-after", envDuration("STALL_AFTER", c.StallAfter), "replica with threads ON whose GTID gap to the writer does not shrink for this long is stalled (alert, warning check, never a promotion candidate)")
	fs.Float64Var(&c.ReplLagAlertSec, "repl-lag-alert-sec", c.ReplLagAlertSec, "replication lag alert threshold")
	c.ReplThreadAlertDelay = envDuration("REPL_ALERT_DELAY", envDuration("REPL_THREAD_ALERT_DELAY", c.ReplThreadAlertDelay))
	fs.DurationVar(&c.ReplThreadAlertDelay, "repl-alert-delay", c.ReplThreadAlertDelay, "replication-health alerts (thread stopped, read_only drift, primary read_only, lag, stall, unknown) are raised only after persisting this long and outside a switchover/failover window")
	fs.DurationVar(&c.FenceFollowupWindow, "fence-followup-window", envDuration("FENCE_FOLLOWUP_WINDOW", c.FenceFollowupWindow), "after a failover, the fenced old primary waiting to rejoin is fenced without a separate alert and its replication-health alerts are held for this long; past it they alert once")
	fs.DurationVar(&c.ReplThreadAlertDelay, "repl-thread-alert-delay", c.ReplThreadAlertDelay, "deprecated alias of -repl-alert-delay (v0.3.2)")
	fs.StringVar(&c.ConsulServiceID, "consul-service-id", envOr("CONSUL_SERVICE_ID", ""), "register this agent service id and publish TTL checks (recommended)")
	fs.StringVar(&c.ConsulServiceName, "consul-service-name", envOr("CONSUL_SERVICE_NAME", c.ConsulServiceName), "Consul service name when registering")
	fs.StringVar(&raw.proxysqlPeers, "proxysql-peer-endpoints", envOr("PROXYSQL_PEER_ENDPOINTS", ""), "peer=host:port map for ProxySQL mysql_servers matching (comma-separated)")
	fs.StringVar(&raw.mysqlPeers, "mysql-peer-endpoints", envOr("MYSQL_PEER_ENDPOINTS", ""), "peer=host:port map for remote MySQL admin (comma-separated)")
	fs.BoolVar(&c.MySQLTLS, "mysql-tls", envBool("MYSQL_TLS", false), "enable TLS for peer MySQL connections")
	fs.StringVar(&c.MySQLCAFile, "mysql-ca-file", envOr("MYSQL_CA_FILE", ""), "CA PEM for peer MySQL TLS")
	fs.IntVar(&c.ActionLogMax, "action-log-max", c.ActionLogMax, "max entries in Consul action log KV")

	fs.StringVar(&c.MasterFormat, "master-format", envOr("MASTER_FORMAT", "epoch"), "mysql/master/<cluster> write format: epoch (node|epoch) or legacy (host:port, epoch in writer/epoch); reads accept both")
	fs.StringVar(&c.MemberServiceName, "member-service", envOr("MEMBER_SERVICE", ""), "Consul catalog service with one entry per member on vnode <svc>-<node>-vnode, tagged writer/reader/lagging/fenced with a check <svc>:<node>:mysql (act mode only); empty = off")
	fs.Float64Var(&c.MemberMaxLag, "member-max-lag", envFloat("MEMBER_MAX_LAG", 30), "member service: a replica lagging more seconds than this is tagged lagging (check warning) instead of reader")
	fs.DurationVar(&c.MemberCheckDelay, "member-check-delay", envDuration("MEMBER_CHECK_DELAY", 10*time.Second), "member service: a role/check change must persist this long before it is written (anti-flap); promotions and fencing are immediate")
	fs.StringVar(&c.WriterServiceName, "writer-service", envOr("WRITER_SERVICE", ""), "Consul catalog service kept pointing at the writer (e.g. <cluster>-writer); empty = off")
	fs.StringVar(&raw.writerTags, "writer-service-tags", envOr("WRITER_SERVICE_TAGS", "mysql,writer"), "comma-separated tags for -writer-service")
	fs.BoolVar(&c.ProxySQLVerify, "proxysql-verify", envBool("PROXYSQL_VERIFY", false), "read-only: compare ProxySQL runtime writer hostgroup with the writer key via -proxysql-admin-dsn and alert on mismatch (no ProxySQL writes)")
	fs.DurationVar(&c.ProxySQLVerifyGrace, "proxysql-verify-grace", envDuration("PROXYSQL_VERIFY_GRACE", 20*time.Second), "mismatch must persist this long before the alert")
	fs.BoolVar(&c.ForeignWriteAlert, "foreign-write-alert", envBool("FOREIGN_WRITE_ALERT", true), "alert when the master key was changed by something other than a guard (repman/bash post-script)")
	fs.BoolVar(&c.WitnessProbe, "witness-probe", envBool("WITNESS_PROBE", true), "witness: probe the primary with a MySQL connect check and vote (needs -mysql-user/-mysql-password-file and -mysql-peer-endpoints; without them the witness never votes)")
	fs.BoolVar(&c.SemiSyncGate, "semisync-gate", envBool("SEMISYNC_GATE", true), "refuse automatic failover when semi-sync is enabled on the primary but was last seen async (acknowledged writes could be lost)")
	fs.BoolVar(&c.SemiSyncRestore, "semisync-restore", envBool("SEMISYNC_RESTORE", true), "after failover/switchover/rejoin, re-enable semi-sync source/replica settings when they were enabled before")
	fs.BoolVar(&c.SemiSyncStall, "semisync-stall", envBool("SEMISYNC_STALL", false), "leader only, act mode: when the replica is confirmed dead (own probe + witness connect check + no semi-sync client, for -semisync-stall-probes ticks) set rpl_semi_sync_source_enabled=OFF on the primary so commits stop stalling for the semi-sync timeout; turn it on again when the replica is caught up. Run it on the witness too (it then probes the replica). Default off")
	fs.IntVar(&c.SemiSyncStallProbes, "semisync-stall-probes", envInt("SEMISYNC_STALL_PROBES", 3), "consecutive leader ticks the replica must be confirmed dead before semi-sync is turned off")
	fs.IntVar(&c.SemiSyncReenable, "semisync-reenable-ticks", envInt("SEMISYNC_REENABLE_TICKS", 5), "consecutive leader ticks the replica must be healthy (IO+SQL running, GTID caught up, lag within -semisync-reenable-max-lag) before semi-sync is turned on again")
	fs.Float64Var(&c.SemiSyncMaxLag, "semisync-reenable-max-lag", envFloat("SEMISYNC_REENABLE_MAX_LAG", 1), "semi-sync is turned on again only when the replica lags at most this many seconds")
	fs.StringVar(&c.SwitchoverTo, "to", "", "switchover: the node to promote (same as the positional argument)")
	fs.BoolVar(&c.SwitchoverDryRun, "dry-run", true, "switchover: print the preflight and the plan and change nothing; pass -dry-run=false to execute")
	fs.DurationVar(&c.SwitchoverProxyWait, "switchover-proxysql-wait", envDuration("SWITCHOVER_PROXYSQL_WAIT", 30*time.Second), "after a switchover, wait this long for ProxySQL's writer hostgroup to show the new primary (needs -proxysql-verify; read-only check, never fails the switchover)")
	fs.BoolVar(&c.ReplSourceSSL, "repl-source-ssl", envBool("REPL_SOURCE_SSL", true), "CHANGE REPLICATION SOURCE uses SOURCE_SSL=1")
	fs.StringVar(&c.NtfyTokenFile, "ntfy-token-file", envOr("NTFY_TOKEN_FILE", ""), "optional ntfy access token file (Authorization: Bearer)")

}

func envFloat(key string, def float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}
