package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// GTIDRecord is published per node.
type GTIDRecord struct {
	Node       string    `json:"node"`
	Executed   string    `json:"executed"`
	Retrieved  string    `json:"retrieved"`
	UpdatedAt  time.Time `json:"updated_at"`
	Heartbeat  int64     `json:"heartbeat_age_sec"`
	LastQueued int64     `json:"last_queued_age_sec"`
}

// Store wraps Consul KV and sessions.
type Store struct {
	client *capi.Client
	keys   KeyPaths
}

func NewStore(addr, token string, cluster string) (*Store, error) {
	cfg := capi.DefaultConfig()
	cfg.Address = addr
	if token != "" {
		cfg.Token = token
	}
	client, err := capi.NewClient(cfg)
	if err != nil {
		return nil, err
	}
	return &Store{client: client, keys: KeyPaths{Cluster: cluster}}, nil
}

func (s *Store) Client() *capi.Client { return s.client }

func (s *Store) Keys() KeyPaths { return s.keys }

func (s *Store) PutGTID(ctx context.Context, rec GTIDRecord) error {
	b, _ := json.Marshal(rec)
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.GTID(rec.Node), Value: b}, nil)
	return err
}

func (s *Store) GetGTID(ctx context.Context, node string) (*GTIDRecord, error) {
	pair, _, err := s.client.KV().Get(s.keys.GTID(node), nil)
	if err != nil || pair == nil {
		return nil, err
	}
	var rec GTIDRecord
	if err := json.Unmarshal(pair.Value, &rec); err != nil {
		return nil, err
	}
	return &rec, nil
}

func (s *Store) GetEpoch(ctx context.Context) (uint64, error) {
	pair, _, err := s.client.KV().Get(s.keys.WriterEpoch(), nil)
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return 0, err
	}
	var n uint64
	_, err = fmt.Sscanf(string(pair.Value), "%d", &n)
	return n, err
}

func (s *Store) CasBumpEpoch(ctx context.Context, expect uint64) (uint64, error) {
	key := s.keys.WriterEpoch()
	pair, _, err := s.client.KV().Get(key, nil)
	if err != nil {
		return 0, err
	}
	var cur uint64
	var index uint64 // 0 = create only if absent
	if pair != nil {
		index = pair.ModifyIndex
		if len(pair.Value) > 0 {
			_, _ = fmt.Sscanf(string(pair.Value), "%d", &cur)
		}
	}
	if cur != expect {
		return cur, fmt.Errorf("epoch CAS mismatch: have %d expect %d", cur, expect)
	}
	next := cur + 1
	ok, _, err := s.client.KV().CAS(&capi.KVPair{Key: key, Value: []byte(fmt.Sprintf("%d", next)), ModifyIndex: index}, nil)
	if err != nil {
		return 0, err
	}
	if !ok {
		return cur, fmt.Errorf("epoch CAS lost race")
	}
	return next, nil
}

func (s *Store) SetMaster(ctx context.Context, node string, epoch uint64) error {
	val := fmt.Sprintf("%s|%d", node, epoch)
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.MasterKV(), Value: []byte(val)}, nil)
	return err
}

func (s *Store) MarkFenced(ctx context.Context, node string, epoch uint64) error {
	val := fmt.Sprintf("%d", epoch)
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.Fenced(node), Value: []byte(val)}, nil)
	return err
}

func (s *Store) MarkNeedsRebuild(ctx context.Context, node string, reason string) error {
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.NeedsRebuild(node), Value: []byte(reason)}, nil)
	return err
}

// MaintenanceEnabled reads the maintenance KV flag (any non-empty value pauses actions).
func (s *Store) MaintenanceEnabled(ctx context.Context) (bool, string, error) {
	pair, _, err := s.client.KV().Get(s.keys.Maintenance(), nil)
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return false, "", err
	}
	return true, string(pair.Value), nil
}

// GuardPresence is a minimal record for /status topology.
type GuardPresence struct {
	Node   string `json:"node"`
	Leader bool   `json:"leader"`
	Role   string `json:"role"`
}

// ListGuardPresence reads registered mysql-guard services (draft: KV prefix optional).
func (s *Store) ListGuardPresence(ctx context.Context) ([]GuardPresence, error) {
	// TODO: discover via Consul health API for service mysql-guard
	return nil, nil
}

// GetPrimary returns current primary node from master KV.
func (s *Store) GetPrimary(ctx context.Context) (string, uint64, error) {
	pair, _, err := s.client.KV().Get(s.keys.MasterKV(), nil)
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return "", 0, err
	}
	val := string(pair.Value)
	node, epochStr, _ := strings.Cut(val, "|")
	var epoch uint64
	if epochStr != "" {
		_, _ = fmt.Sscanf(epochStr, "%d", &epoch)
	}
	return node, epoch, nil
}
