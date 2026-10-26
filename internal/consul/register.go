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

	ttl ttlUpdater // nil -> client.Agent()

	mu      sync.Mutex
	failing bool
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

// ttlUpdater is the subset of the Consul agent API used for TTL updates (fake in tests).
type ttlUpdater interface {
	UpdateTTL(checkID, output, status string) error
}

func (a *AgentChecks) updater() ttlUpdater {
	if a.ttl != nil {
		return a.ttl
	}
	return a.client.Agent()
}

// SetCheckStatus is the alert sink used by notify.Fanout. Alerts are logged and
// sent to ntfy; they must not overwrite the guard's own liveness TTL check, so
// this is a no-op (the guard check reflects the guard tick via UpdateGuard).
func (a *AgentChecks) SetCheckStatus(ctx context.Context, _ string, status, output string) error {
	return nil
}

// UpdateGuard sets the guard's own TTL check (<service_id>:guard). status is
// capi.HealthPassing, HealthWarning or HealthCritical. Errors are logged once per
// failure streak and retried on the next tick (never permanently disabled).
func (a *AgentChecks) UpdateGuard(status, output string) error {
	if a.serviceID == "" {
		return nil
	}
	if len(output) > 4096 {
		output = output[:4096]
	}
	err := a.updater().UpdateTTL(a.checkID, output, status)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err != nil {
		if !a.failing && a.logger != nil {
			a.logger.Warn("consul TTL update failed; retrying each tick", "check_id", a.checkID, "err", err)
		}
		a.failing = true
		return err
	}
	if a.failing && a.logger != nil {
		a.logger.Info("consul TTL update recovered", "check_id", a.checkID)
	}
	a.failing = false
	return nil
}

// PassTTL marks the guard check healthy.
func (a *AgentChecks) PassTTL(output string) { _ = a.UpdateGuard(capi.HealthPassing, output) }

// AlertCheckID returns the check id used for replication alerts.
func (a *AgentChecks) AlertCheckID(code string) string {
	return fmt.Sprintf("%s:%s", a.serviceID, code)
}
