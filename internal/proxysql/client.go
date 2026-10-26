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
	PeerName      string
	NewPrimary    string
	OldPrimary    string
	NewEndpoint   Endpoint
	OldEndpoint   Endpoint
	WriterGroup   int
	ReaderGroup   int
	Fence         bool
}

// Client updates ProxySQL when admin mode is enabled.
type Client struct {
	enabled   bool
	writerHG  int
	readerHG  int
	endpoints map[string]string
	db        *sql.DB
}

func NewClient(cfg config.ProxySQLSettings) (*Client, error) {
	c := &Client{
		enabled:   cfg.AdminEnabled,
		writerHG:  cfg.WriterHostgroup,
		readerHG:  cfg.ReaderHostgroup,
		endpoints: cfg.PeerEndpoints,
	}
	if !cfg.AdminEnabled || cfg.AdminDSN == "" {
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
