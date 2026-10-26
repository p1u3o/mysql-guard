package proxysql

import (
	"fmt"
	"strconv"
	"strings"
)

// Endpoint is a row in ProxySQL mysql_servers (matched by hostname and port).
type Endpoint struct {
	Host string
	Port int
}

// ResolveEndpoint maps a logical peer name to the ProxySQL backend address.
func ResolveEndpoint(peer string, defaults map[string]string) Endpoint {
	raw := peer
	if defaults != nil && defaults[peer] != "" {
		raw = defaults[peer]
	}
	host, portStr, err := netSplitHostPortDefault(raw, 3306)
	if err != nil {
		return Endpoint{Host: peer, Port: 3306}
	}
	port, _ := strconv.Atoi(portStr)
	if port == 0 {
		port = 3306
	}
	return Endpoint{Host: host, Port: port}
}

func netSplitHostPortDefault(hostport string, defaultPort int) (host, port string, err error) {
	if strings.Contains(hostport, ":") {
		h, p, e := splitHostPort(hostport)
		return h, p, e
	}
	return hostport, fmt.Sprintf("%d", defaultPort), nil
}

func splitHostPort(s string) (string, string, error) {
	i := strings.LastIndex(s, ":")
	if i < 0 {
		return s, "", fmt.Errorf("no port")
	}
	return s[:i], s[i+1:], nil
}
