package consul

import (
	"context"
	"fmt"

	"mysql-guard/internal/actionlog"

	capi "github.com/hashicorp/consul/api"
)

func (k KeyPaths) ActionLog() string {
	return fmt.Sprintf("mysql/%s/actions", k.Cluster)
}

// GetActionLog reads the capped action log from KV.
func (s *Store) GetActionLog(ctx context.Context) ([]actionlog.Entry, error) {
	pair, _, err := s.client.KV().Get(s.keys.ActionLog(), nil)
	if err != nil || pair == nil || len(pair.Value) == 0 {
		return nil, err
	}
	return actionlog.Decode(pair.Value)
}

// AppendActionLog CAS-appends one entry (retries on conflict).
func (s *Store) AppendActionLog(ctx context.Context, e actionlog.Entry, max int) error {
	key := s.keys.ActionLog()
	for attempt := 0; attempt < 8; attempt++ {
		pair, _, err := s.client.KV().Get(key, nil)
		if err != nil {
			return err
		}
		var list []actionlog.Entry
		if pair != nil && len(pair.Value) > 0 {
			list, err = actionlog.Decode(pair.Value)
			if err != nil {
				return err
			}
		}
		list = actionlog.Append(list, e, max)
		b, err := actionlog.Encode(list)
		if err != nil {
			return err
		}
		kv := &capi.KVPair{Key: key, Value: b}
		if pair != nil {
			kv.ModifyIndex = pair.ModifyIndex
			ok, _, err := s.client.KV().CAS(kv, nil)
			if err != nil {
				return err
			}
			if ok {
				return nil
			}
			continue
		}
		ok, _, err := s.client.KV().CAS(kv, nil)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}
	return fmt.Errorf("action log CAS exhausted retries")
}
