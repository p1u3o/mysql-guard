package proxysql

import "context"

// RuntimeServer is a row from stats.runtime_mysql_servers (read-only view).
type RuntimeServer struct {
	Hostname  string `json:"hostname"`
	Port      int    `json:"port"`
	Hostgroup int    `json:"hostgroup"`
	Status    string `json:"status"`
}

// ListRuntimeServers queries ProxySQL when admin DB is configured.
func (c *Client) ListRuntimeServers(ctx context.Context) ([]RuntimeServer, error) {
	if c.db == nil {
		return nil, nil
	}
	rows, err := c.db.QueryContext(ctx, "SELECT hostname, port, hostgroup_id, status FROM runtime_mysql_servers")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RuntimeServer
	for rows.Next() {
		var r RuntimeServer
		if err := rows.Scan(&r.Hostname, &r.Port, &r.Hostgroup, &r.Status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
