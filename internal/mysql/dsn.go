package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	"mysql-guard/internal/config"

	"github.com/go-sql-driver/mysql"
)

// WithIOTimeout returns dsn with readTimeout and writeTimeout set to d.
func WithIOTimeout(dsn string, d time.Duration) (string, error) {
	c, err := mysql.ParseDSN(dsn)
	if err != nil {
		return "", err
	}
	c.ReadTimeout = d
	c.WriteTimeout = d
	return c.FormatDSN(), nil
}

// BuildPeerDSN builds a DSN for a logical peer name.
func BuildPeerDSN(cfg *config.Config, peer string) (string, error) {
	if peer == cfg.NodeName && cfg.MySQLDSN != "" {
		return cfg.MySQLDSN, nil
	}
	ep, ok := cfg.MySQLPeerEndpoints[peer]
	if !ok || ep == "" {
		if peer == cfg.NodeName {
			return BuildDSN(cfg.MySQLAdminUser, cfg.MySQLAdminPass, cfg.MySQLHost, cfg.MySQLPort, cfg)
		}
		return "", fmt.Errorf("no mysql endpoint for peer %q", peer)
	}
	host, port := splitHostPort(ep, cfg.MySQLPort)
	return BuildDSN(cfg.MySQLAdminUser, cfg.MySQLAdminPass, host, port, cfg)
}

func BuildDSN(user, pass, host string, port int, cfg *config.Config) (string, error) {
	if user == "" {
		return "", fmt.Errorf("mysql user required")
	}
	if cfg.MySQLTLS {
		if err := registerTLS(cfg.MySQLCAFile); err != nil {
			return "", err
		}
	}
	userEsc := url.UserPassword(user, pass)
	base := fmt.Sprintf("%s@tcp(%s:%d)/?timeout=3s&readTimeout=3s&writeTimeout=3s", userEsc.String(), host, port)
	if cfg.MySQLTLS {
		base += "&tls=mysqlguard"
	}
	return base, nil
}

func registerTLS(caFile string) error {
	if caFile == "" {
		mysql.RegisterTLSConfig("mysqlguard", &tls.Config{MinVersion: tls.VersionTLS12})
		return nil
	}
	b, err := os.ReadFile(caFile)
	if err != nil {
		return err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(b) {
		return fmt.Errorf("invalid CA file")
	}
	mysql.RegisterTLSConfig("mysqlguard", &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12})
	return nil
}

func splitHostPort(ep string, defaultPort int) (string, int) {
	if strings.Contains(ep, ":") {
		h, p, _ := strings.Cut(ep, ":")
		var port int
		fmt.Sscanf(p, "%d", &port)
		if port == 0 {
			port = defaultPort
		}
		return h, port
	}
	return ep, defaultPort
}
