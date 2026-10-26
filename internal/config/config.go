package config

import (
	"flag"
	"fmt"
	"os"
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

	MySQLDSN            string
	MySQLHost           string
	MySQLPort           int
	MySQLAdminUser      string
	MySQLAdminPass      string
	MySQLAdminPassFile  string

	ConsulAddr      string
	ConsulTokenFile string
	ConsulToken     string

	ClusterName string
	Peers       []string // mysql node names in the pair

	HTTPListen string

	Interval         time.Duration
	ProbeTimeout     time.Duration
	SlowThreshold    int
	DeadAfter        time.Duration
	VotesNeeded      int
	SessionTTL       time.Duration
	LockDelay        time.Duration
	MinBetweenFail   time.Duration
	MaxApplyLag      time.Duration
	LagGateMaxWait   time.Duration
	FenceGrace       time.Duration
	ReplicaStale     time.Duration
	ProxySQLMode        ProxySQLMode
	ProxySQLAdminDSN    string
	ProxySQLAdminEnable bool
	ProxySQLWriterHG    int
	ProxySQLReaderHG    int

	NtfyURL          string
	ReplLagAlertSec  float64
	ConsulServiceID   string
	ConsulServiceName string

	ActionLogMax int

	ProxySQLPeerEndpoints map[string]string // peer name -> host:port for mysql_servers rows
	MySQLPeerEndpoints    map[string]string // peer name -> host:port for admin connections
	MySQLTLS              bool
	MySQLCAFile           string

	Promotable bool
}

// ProxySQLConfig is passed to the ProxySQL client.
func (c *Config) ProxySQLConfig() ProxySQLSettings {
	admin := c.ProxySQLAdminEnable || c.ProxySQLMode == ProxySQLAdmin
	return ProxySQLSettings{
		AdminEnabled:    admin,
		AdminDSN:        c.ProxySQLAdminDSN,
		WriterHostgroup: c.ProxySQLWriterHG,
		ReaderHostgroup: c.ProxySQLReaderHG,
		PeerEndpoints:   c.ProxySQLPeerEndpoints,
	}
}

// ProxySQLSettings for the proxysql package.
type ProxySQLSettings struct {
	AdminEnabled    bool
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
		Mode:           ModeObserve,
		Interval:       2 * time.Second,
		ProbeTimeout:   3 * time.Second,
		SlowThreshold:  4,
		DeadAfter:      30 * time.Second,
		VotesNeeded:    2,
		SessionTTL:     15 * time.Second,
		LockDelay:      15 * time.Second,
		MinBetweenFail: time.Hour,
		MaxApplyLag:    30 * time.Second,
		LagGateMaxWait: 0,
		FenceGrace:     30 * time.Second,
		ReplicaStale:   35 * time.Second,
		ProxySQLMode:        ProxySQLMonitor,
		ProxySQLWriterHG:    10,
		ProxySQLReaderHG:    11,
		ReplLagAlertSec:     30,
		HTTPListen:          ":9470",
		ConsulAddr:          "127.0.0.1:8500",
		MySQLPort:           3306,
		ConsulServiceName:   "mysql-guard",
		ActionLogMax:        500,
	}

	var peers string
	var proxysqlPeers string
	var mysqlPeers string
	fs.StringVar(&c.NodeName, "node", envOr("GUARD_NODE", ""), "this node's logical name")
	fs.StringVar((*string)(&c.Role), "role", envOr("GUARD_ROLE", "db"), "db or witness")
	fs.StringVar((*string)(&c.Mode), "mode", envOr("GUARD_MODE", "observe"), "observe, fence, or act")
	fs.StringVar(&c.MySQLDSN, "mysql-dsn", envOr("MYSQL_DSN", ""), "full MySQL DSN override (must not include password if -mysql-password-file is set)")
	fs.StringVar(&c.MySQLHost, "mysql-host", envOr("MYSQL_HOST", "127.0.0.1"), "MySQL host when building DSN from -mysql-user")
	fs.IntVar(&c.MySQLPort, "mysql-port", c.MySQLPort, "MySQL port when building DSN")
	fs.StringVar(&c.MySQLAdminUser, "mysql-user", envOr("MYSQL_USER", ""), "MySQL admin user")
	fs.StringVar(&c.MySQLAdminPassFile, "mysql-password-file", envOr("MYSQL_PASSWORD_FILE", ""), "file containing MySQL password")
	fs.StringVar(&c.ConsulAddr, "consul-addr", envOr("CONSUL_HTTP_ADDR", c.ConsulAddr), "Consul HTTP address")
	fs.StringVar(&c.ConsulTokenFile, "consul-token-file", envOr("CONSUL_TOKEN_FILE", ""), "Consul ACL token file")
	fs.StringVar(&c.ClusterName, "cluster", envOr("GUARD_CLUSTER", ""), "cluster name for KV keys (required)")
	fs.StringVar(&peers, "peers", envOr("GUARD_PEERS", ""), "comma-separated db peer names (required)")
	fs.StringVar(&c.HTTPListen, "http", envOr("GUARD_HTTP", c.HTTPListen), "HTTP listen address")
	fs.DurationVar(&c.Interval, "interval", c.Interval, "health loop interval")
	fs.DurationVar(&c.ProbeTimeout, "probe-timeout", c.ProbeTimeout, "per-probe deadline")
	fs.IntVar(&c.SlowThreshold, "slow-threshold", c.SlowThreshold, "consecutive slow probes before failed")
	fs.DurationVar(&c.DeadAfter, "dead-after", c.DeadAfter, "time since first failure before vote counts dead")
	fs.IntVar(&c.VotesNeeded, "votes-needed", c.VotesNeeded, "dead votes required of 3")
	fs.DurationVar(&c.SessionTTL, "session-ttl", c.SessionTTL, "Consul session TTL")
	fs.DurationVar(&c.LockDelay, "lock-delay", c.LockDelay, "Consul lock delay")
	fs.DurationVar(&c.MinBetweenFail, "min-between-failovers", c.MinBetweenFail, "rate limit between failovers")
	fs.DurationVar(&c.MaxApplyLag, "max-apply-lag", c.MaxApplyLag, "allowed time to apply retrieved GTIDs")
	fs.DurationVar(&c.LagGateMaxWait, "lag-gate-max-wait", c.LagGateMaxWait, "max wait for missing GTIDs; 0=never promote")
	fs.DurationVar(&c.FenceGrace, "fence-grace", c.FenceGrace, "grace before fencing marked sole writer")
	fs.DurationVar(&c.ReplicaStale, "replica-stale", c.ReplicaStale, "heartbeat and last-queued trx stale threshold")
	fs.StringVar((*string)(&c.ProxySQLMode), "proxysql-mode", envOr("PROXYSQL_MODE", "monitor"), "monitor or admin")
	fs.StringVar(&c.ProxySQLAdminDSN, "proxysql-admin-dsn", envOr("PROXYSQL_ADMIN_DSN", ""), "ProxySQL admin DSN")
	fs.BoolVar(&c.ProxySQLAdminEnable, "proxysql-admin-enable", envBool("PROXYSQL_ADMIN_ENABLE", false), "apply ProxySQL admin hostgroup updates on promote/fence")
	fs.IntVar(&c.ProxySQLWriterHG, "proxysql-writer-hg", c.ProxySQLWriterHG, "writer hostgroup id")
	fs.IntVar(&c.ProxySQLReaderHG, "proxysql-reader-hg", c.ProxySQLReaderHG, "reader hostgroup id")
	fs.StringVar(&c.NtfyURL, "ntfy-url", envOr("NTFY_URL", ""), "ntfy topic URL for alerts")
	fs.Float64Var(&c.ReplLagAlertSec, "repl-lag-alert-sec", c.ReplLagAlertSec, "replication lag alert threshold")
	fs.StringVar(&c.ConsulServiceID, "consul-service-id", envOr("CONSUL_SERVICE_ID", ""), "register this agent service id and publish TTL checks (recommended)")
	fs.StringVar(&c.ConsulServiceName, "consul-service-name", envOr("CONSUL_SERVICE_NAME", c.ConsulServiceName), "Consul service name when registering")
	fs.StringVar(&proxysqlPeers, "proxysql-peer-endpoints", envOr("PROXYSQL_PEER_ENDPOINTS", ""), "peer=host:port map for ProxySQL mysql_servers matching (comma-separated)")
	fs.StringVar(&mysqlPeers, "mysql-peer-endpoints", envOr("MYSQL_PEER_ENDPOINTS", ""), "peer=host:port map for remote MySQL admin (comma-separated)")
	fs.BoolVar(&c.MySQLTLS, "mysql-tls", envBool("MYSQL_TLS", false), "enable TLS for peer MySQL connections")
	fs.StringVar(&c.MySQLCAFile, "mysql-ca-file", envOr("MYSQL_CA_FILE", ""), "CA PEM for peer MySQL TLS")
	fs.IntVar(&c.ActionLogMax, "action-log-max", c.ActionLogMax, "max entries in Consul action log KV")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	c.Peers = splitCSV(peers)
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
