package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// Transition records an in-progress (or just finished) switchover/failover.
type Transition struct {
	Action  string    `json:"action"`
	From    string    `json:"from"`
	To      string    `json:"to"`
	By      string    `json:"by"`
	Started time.Time `json:"started"`
	Ended   time.Time `json:"ended,omitempty"`
}

func (s *Store) PutTransition(ctx context.Context, t Transition) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	_, err = s.client.KV().Put(&capi.KVPair{Key: s.keys.Transition(), Value: b}, (&capi.WriteOptions{}).WithContext(ctx))
	return err
}

// PutKeyRepair / GetKeyRepair / DeleteKeyRepair: pending master-key rewrite.
func (s *Store) PutKeyRepair(ctx context.Context, node string, epoch uint64) error {
	_, err := s.client.KV().Put(&capi.KVPair{Key: s.keys.KeyRepair(), Value: []byte(fmt.Sprintf("%s|%d", node, epoch))}, (&capi.WriteOptions{}).WithContext(ctx))
	return err
}

func (s *Store) GetKeyRepair(ctx context.Context) (string, uint64, error) {
	pair, _, err := s.client.KV().Get(s.keys.KeyRepair(), (&capi.QueryOptions{}).WithContext(ctx))
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return "", 0, err
	}
	var node string
	var epoch uint64
	if _, err := fmt.Sscanf(strings.Replace(string(pair.Value), "|", " ", 1), "%s %d", &node, &epoch); err != nil {
		return "", 0, err
	}
	return node, epoch, nil
}

func (s *Store) DeleteKeyRepair(ctx context.Context) error {
	_, err := s.client.KV().Delete(s.keys.KeyRepair(), (&capi.WriteOptions{}).WithContext(ctx))
	return err
}

func (s *Store) GetTransition(ctx context.Context) (*Transition, error) {
	pair, _, err := s.client.KV().Get(s.keys.Transition(), (&capi.QueryOptions{}).WithContext(ctx))
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return nil, err
	}
	var t Transition
	if err := json.Unmarshal(pair.Value, &t); err != nil {
		return nil, err
	}
	return &t, nil
}
