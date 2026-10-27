package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// SemiSyncRecord is the cluster's last-known semi-sync state of the writer
// (v0.3.6). Enabled survives a failover so rejoin can re-enable semi-sync.
type SemiSyncRecord struct {
	Enabled bool      `json:"enabled"`           // semi-sync was configured on the writer
	Pending bool      `json:"pending,omitempty"` // failover left semi-sync off; rejoin re-enables it
	Mode    string    `json:"mode"`              // sync, async, off, unknown (last sample)
	Node    string    `json:"node"`
	Epoch   uint64    `json:"epoch"`
	At      time.Time `json:"at"`
}

func (k KeyPaths) SemiSync() string { return fmt.Sprintf("mysql/%s/guard/semisync", k.Cluster) }

func (s *Store) PutSemiSync(ctx context.Context, r SemiSyncRecord) error {
	b, _ := json.Marshal(r)
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.SemiSync(), Value: b}, (&capi.WriteOptions{}).WithContext(ctx))
	return err
}

func (s *Store) GetSemiSync(ctx context.Context) (*SemiSyncRecord, error) {
	pair, _, err := s.client.KV().Get(s.keys.SemiSync(), (&capi.QueryOptions{}).WithContext(ctx))
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return nil, err
	}
	var r SemiSyncRecord
	if err := json.Unmarshal(pair.Value, &r); err != nil {
		return nil, err
	}
	return &r, nil
}
