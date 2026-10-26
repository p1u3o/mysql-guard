package proxysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"mysql-guard/internal/config"

	_ "github.com/go-sql-driver/mysql"
)

// AdminAction describes hostgroup/status changes applied on promote/fence.
type AdminAction struct {
	PeerName    string
	NewPrimary  string
	OldPrimary  string
	NewEndpoint Endpoint
	OldEndpoint Endpoint
	WriterGroup int
	ReaderGroup int
	Fence       bool
}

// Client updates ProxySQL when admin mode is enabled.
type Client struct {
	enabled   bool
	verify    bool
	writerHG  int
	readerHG  int
	endpoints map[string]string
	db        *sql.DB
}

func NewClient(cfg config.ProxySQLSettings) (*Client, error) {
	c := &Client{
		enabled:   cfg.AdminEnabled,
		verify:    cfg.Verify,
		writerHG:  cfg.WriterHostgroup,
		readerHG:  cfg.ReaderHostgroup,
		endpoints: cfg.PeerEndpoints,
	}
	if (!cfg.AdminEnabled && !cfg.Verify) || cfg.AdminDSN == "" {
		return c, nil
	}
	db, err := sql.Open("mysql", cfg.AdminDSN)
	if err != nil {
		return nil, err
	}
	c.db = db
	return c, nil
}

func (c *Client) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *Client) Enabled() bool { return c.enabled }

func (c *Client) endpoint(peer string) Endpoint {
	return ResolveEndpoint(peer, c.endpoints)
}

// PlanPromote updates rows matching hostname and port in mysql_servers.
func PlanPromote(act AdminAction) []string {
	return []string{
		fmt.Sprintf("UPDATE mysql_servers SET hostgroup_id=%d, status='ONLINE' WHERE hostname='%s' AND port=%d",
			act.WriterGroup, act.NewEndpoint.Host, act.NewEndpoint.Port),
		fmt.Sprintf("UPDATE mysql_servers SET hostgroup_id=%d, status='OFFLINE_SOFT' WHERE hostname='%s' AND port=%d",
			act.ReaderGroup, act.OldEndpoint.Host, act.OldEndpoint.Port),
		"LOAD MYSQL SERVERS TO RUNTIME",
	}
}

// PlanFence hard-offlines a backend row by hostname and port.
func PlanFence(act AdminAction) []string {
	ep := act.NewEndpoint
	if ep.Host == "" {
		ep = act.OldEndpoint
	}
	return []string{
		fmt.Sprintf("UPDATE mysql_servers SET status='OFFLINE_HARD', hostgroup_id=%d WHERE hostname='%s' AND port=%d",
			act.ReaderGroup, ep.Host, ep.Port),
		"LOAD MYSQL SERVERS TO RUNTIME",
	}
}

func (c *Client) ApplyPromote(ctx context.Context, newPrimary, oldPrimary string) error {
	if !c.enabled {
		return nil
	}
	if c.db == nil {
		return errors.New("proxysql admin DSN not configured")
	}
	act := AdminAction{
		NewPrimary:  newPrimary,
		OldPrimary:  oldPrimary,
		NewEndpoint: c.endpoint(newPrimary),
		OldEndpoint: c.endpoint(oldPrimary),
		WriterGroup: c.writerHG,
		ReaderGroup: c.readerHG,
	}
	for _, q := range PlanPromote(act) {
		if _, err := c.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) ApplyFence(ctx context.Context, peer string) error {
	if !c.enabled {
		return nil
	}
	if c.db == nil {
		return errors.New("proxysql admin DSN not configured")
	}
	act := AdminAction{
		OldEndpoint: c.endpoint(peer),
		ReaderGroup: c.readerHG,
	}
	for _, q := range PlanFence(act) {
		if _, err := c.db.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return nil
}

// VerifyEnabled reports whether the read-only writer-hostgroup check can run.
func (c *Client) VerifyEnabled() bool { return c.verify && c.db != nil }

// WriterHostgroup returns the ONLINE host:port rows of the writer hostgroup in
// ProxySQL's runtime config (read-only query, no admin writes).
func (c *Client) WriterHostgroup(ctx context.Context) ([]string, error) {
	if c.db == nil {
		return nil, errors.New("proxysql admin DSN not configured")
	}
	rows, err := c.db.QueryContext(ctx, fmt.Sprintf("SELECT hostname, port FROM runtime_mysql_servers WHERE hostgroup_id=%d AND status='ONLINE'", c.writerHG))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		var p int
		if err := rows.Scan(&h, &p); err != nil {
			return nil, err
		}
		out = append(out, fmt.Sprintf("%s:%d", h, p))
	}
	return out, rows.Err()
}

// WriterMismatch compares the ProxySQL writer hostgroup with the expected
// writer endpoint. "" means consistent (exactly the writer, nothing else).
func WriterMismatch(online []string, want string) string {
	if want == "" {
		return ""
	}
	if len(online) == 1 && online[0] == want {
		return ""
	}
	return fmt.Sprintf("ProxySQL writer hostgroup ONLINE=%v, writer key says %s", online, want)
}

// ExpectedEndpoint is the host:port ProxySQL should have for peer.
func (c *Client) ExpectedEndpoint(peer string) string {
	if c.endpoints[peer] == "" {
		return ""
	}
	ep := c.endpoint(peer)
	if ep.Host == "" {
		return ""
	}
	return fmt.Sprintf("%s:%d", ep.Host, ep.Port)
}
