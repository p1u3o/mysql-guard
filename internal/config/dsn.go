package config

import (
	"fmt"
	"net/url"
	"strings"
)

// MySQLHostPort defaults for DSN assembly when -mysql-dsn is unset.
type MySQLHostPort struct {
	Host string
	Port int
}

// FinalizeMySQLDSN builds or validates the DSN after credentials are loaded.
func (c *Config) FinalizeMySQLDSN() error {
	if c.MySQLDSN == "" && c.MySQLAdminUser == "" {
		return nil
	}
	if c.MySQLDSN != "" {
		if c.MySQLAdminPassFile != "" && dsnContainsPassword(c.MySQLDSN) {
			return fmt.Errorf("-mysql-dsn must not embed a password when -mysql-password-file is set")
		}
		return nil
	}
	if c.MySQLAdminUser == "" {
		return fmt.Errorf("mysql connection requires -mysql-dsn or -mysql-user with -mysql-host")
	}
	if c.MySQLHost == "" {
		return fmt.Errorf("-mysql-host is required when -mysql-dsn is not set")
	}
	pass := c.MySQLAdminPass
	user := url.UserPassword(c.MySQLAdminUser, pass)
	c.MySQLDSN = fmt.Sprintf("%s@tcp(%s:%d)/?timeout=3s&readTimeout=3s&writeTimeout=3s",
		user.String(), c.MySQLHost, c.MySQLPort)
	return nil
}

func dsnContainsPassword(dsn string) bool {
	at := strings.Index(dsn, "@")
	if at <= 0 {
		return false
	}
	userinfo := dsn[:at]
	if strings.HasPrefix(userinfo, "tcp(") || strings.HasPrefix(userinfo, "unix(") {
		return false
	}
	return strings.Contains(userinfo, ":")
}
