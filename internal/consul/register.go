package consul

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"

	capi "github.com/hashicorp/consul/api"
)

// AgentChecks updates TTL checks on a self-registered service.
type AgentChecks struct {
	client    *capi.Client
	serviceID string
	checkID   string
	logger    *slog.Logger

	mu       sync.Mutex
	disabled bool
	once     sync.Once
}

func NewAgentChecks(client *capi.Client, serviceID string, log *slog.Logger) *AgentChecks {
	checkID := ""
	if serviceID != "" {
		checkID = serviceID + ":guard"
	}
	return &AgentChecks{client: client, serviceID: serviceID, checkID: checkID, logger: log}
}

// RegisterAgentService registers name/id with a TTL check when serviceID is set.
func RegisterAgentService(ctx context.Context, client *capi.Client, name, id, listenAddr string) error {
	if id == "" {
		return nil
	}
	port := parseListenPort(listenAddr)
	reg := &capi.AgentServiceRegistration{
		ID:      id,
		Name:    name,
		Port:    port,
		Address: "127.0.0.1",
		Check: &capi.AgentServiceCheck{
			CheckID:                        id + ":guard",
			TTL:                            "30s",
			DeregisterCriticalServiceAfter: "24h",
		},
	}
	return client.Agent().ServiceRegister(reg)
}

func parseListenPort(addr string) int {
	if addr == "" {
		return 9470
	}
	if strings.HasPrefix(addr, ":") {
		p, _ := strconv.Atoi(strings.TrimPrefix(addr, ":"))
		if p > 0 {
			return p
		}
		return 9470
	}
	_, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return 9470
	}
	p, _ := strconv.Atoi(portStr)
	if p == 0 {
		return 9470
	}
	return p
}

func (a *AgentChecks) SetCheckStatus(ctx context.Context, _ string, status, output string) error {
	if a.serviceID == "" || a.disabled {
		return nil
	}
	err := a.client.Agent().UpdateTTL(a.checkID, output, status)
	if err == nil {
		return nil
	}
	a.once.Do(func() {
		if a.logger != nil {
			a.logger.Warn("consul TTL updates disabled; register service or fix agent", "service_id", a.serviceID, "err", err)
		}
	})
	a.mu.Lock()
	a.disabled = true
	a.mu.Unlock()
	return err
}

// PassTTL marks the guard check healthy (call periodically from leader).
func (a *AgentChecks) PassTTL(output string) {
	_ = a.SetCheckStatus(context.Background(), "guard", "pass", output)
}

// AlertCheckID returns the check id used for replication alerts.
func (a *AgentChecks) AlertCheckID(code string) string {
	return fmt.Sprintf("%s:%s", a.serviceID, code)
}
