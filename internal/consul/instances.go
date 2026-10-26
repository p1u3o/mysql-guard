package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// GuardInstance is published by each guard for the dashboard.
type GuardInstance struct {
	Node          string    `json:"node"`
	Role          string    `json:"role"`
	Mode          string    `json:"mode"`
	Leader        bool      `json:"leader"`
	LastHeartbeat time.Time `json:"last_heartbeat"`
	LoopRenewSec  float64   `json:"loop_renew_age_sec"`
	LoopHealthSec float64   `json:"loop_health_age_sec"`
	LoopWatchSec  float64   `json:"loop_watch_age_sec"`
}

func (k KeyPaths) GuardInstance(node string) string {
	return fmt.Sprintf("mysql/%s/guard/instance/%s", k.Cluster, node)
}

func (k KeyPaths) GuardInstancePrefix() string {
	return fmt.Sprintf("mysql/%s/guard/instance/", k.Cluster)
}

func (s *Store) PutGuardInstance(ctx context.Context, st GuardInstance) error {
	b, _ := json.Marshal(st)
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.GuardInstance(st.Node), Value: b}, nil)
	return err
}

func (s *Store) ListGuardInstances(ctx context.Context) ([]GuardInstance, error) {
	prefix := s.keys.GuardInstancePrefix()
	pairs, _, err := s.client.KV().List(prefix, nil)
	if err != nil {
		return nil, err
	}
	var out []GuardInstance
	for _, p := range pairs {
		var st GuardInstance
		if err := json.Unmarshal(p.Value, &st); err != nil {
			continue
		}
		out = append(out, st)
	}
	return out, nil
}

// PeerReplicationHost returns host:port for CHANGE REPLICATION SOURCE from peer map.
func PeerReplicationHost(peer string, endpoints map[string]string, defaultPort int) string {
	if endpoints != nil && endpoints[peer] != "" {
		return endpoints[peer]
	}
	return fmt.Sprintf("%s:%d", peer, defaultPort)
}

func HostFromEndpoint(ep string) string {
	host, _, _ := strings.Cut(ep, ":")
	return host
}
