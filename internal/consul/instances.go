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
	// PrimaryView is the primary this guard probed; PrimaryOK its reachability.
	PrimaryView string `json:"primary_view,omitempty"`
	PrimaryOK   bool   `json:"primary_ok"`
	// v0.3.6 quorum inputs (see guard/quorum.go).
	// PrimaryProbe is the witness connect check of PrimaryView: "ok", "dead",
	// or "" (not probing: no credentials/endpoints; never a vote).
	PrimaryProbe string `json:"primary_probe,omitempty"`
	// ReplicaIOLost: this db guard's local IO thread has lost the primary
	// (connecting, erroring, or no event/heartbeat for -replica-stale).
	ReplicaIOLost   bool   `json:"replica_io_lost,omitempty"`
	ReplicaIOReason string `json:"replica_io_reason,omitempty"`
	// PeerProbes (witness, -semisync-stall): connect check of each db peer
	// that is not the primary, "ok" or "dead". A confirmation for the primary
	// guard's own probe of the replica; never part of the failover quorum.
	PeerProbes map[string]string `json:"peer_probes,omitempty"`
	// SemiSync is this db guard's local semi-sync mode (sync/async/off/unknown).
	SemiSync string `json:"semisync,omitempty"`
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
