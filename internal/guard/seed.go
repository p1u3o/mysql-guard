package guard

import (
	"context"
	"log/slog"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/mysql"
)

// SeedInput for initial writer KV on a fresh cluster.
type SeedInput struct {
	MasterExists   bool
	NodeName       string
	FirstPeer      string
	IsLeader       bool
	Writable       bool
	HasReplSource  bool
}

// ShouldSeedMaster documents when the leader may write mysql/master/<cluster>.
func ShouldSeedMaster(in SeedInput) bool {
	if !in.IsLeader || in.MasterExists {
		return false
	}
	if in.NodeName != in.FirstPeer {
		return false
	}
	return in.Writable && !in.HasReplSource
}

// MaybeSeedMaster seeds the writer key from the first peer when appropriate.
func MaybeSeedMaster(ctx context.Context, log *slog.Logger, store *consul.Store, my *mysql.Prober, nodeName string, peers []string, leader bool) error {
	if len(peers) == 0 {
		return nil
	}
	primary, _, err := store.GetPrimary(ctx)
	if err != nil {
		return err
	}
	exists := primary != ""
	writable, hasSource := false, false
	if st, err := my.ReadGTIDState(ctx); err == nil {
		writable = !st.ReadOnly
		hasSource = st.HasReplicationSource
	}
	in := SeedInput{
		MasterExists:  exists,
		NodeName:      nodeName,
		FirstPeer:     peers[0],
		IsLeader:      leader,
		Writable:      writable,
		HasReplSource: hasSource,
	}
	if !ShouldSeedMaster(in) {
		return nil
	}
	log.Info("seeding initial writer key from first peer", "peer", peers[0], "kv", store.Keys().MasterKV())
	return store.SetMaster(ctx, peers[0], 1)
}
