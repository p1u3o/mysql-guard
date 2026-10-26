package guard

import (
	"context"
	"fmt"
	"log/slog"

	"mysql-guard/internal/actionlog"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/gtid"
)

// SeedNode is one observed DB peer used to decide the initial writer key.
type SeedNode struct {
	Node      string
	Reachable bool
	Writable  bool // read_only=OFF
	Executed  gtid.Set
}

// SeedInput for initial writer KV on a fresh cluster.
type SeedInput struct {
	MasterExists bool
	IsLeader     bool
	Nodes        []SeedNode
}

// ShouldSeedMaster decides whether the leader may write mysql/master/<cluster>.
// It seeds only when the key is empty, every peer is reachable, exactly one
// peer is writable, and every other peer's gtid_executed is a subset of the
// writer's (no errant transactions).
func ShouldSeedMaster(in SeedInput) (node string, reason string, ok bool) {
	if !in.IsLeader {
		return "", "not leader", false
	}
	if in.MasterExists {
		return "", "master key already set", false
	}
	if len(in.Nodes) == 0 {
		return "", "no peers", false
	}
	var writer *SeedNode
	for i := range in.Nodes {
		n := &in.Nodes[i]
		if !n.Reachable {
			return "", fmt.Sprintf("peer %s unreachable", n.Node), false
		}
		if n.Writable {
			if writer != nil {
				return "", fmt.Sprintf("multiple writable peers (%s, %s)", writer.Node, n.Node), false
			}
			writer = n
		}
	}
	if writer == nil {
		return "", "no writable peer", false
	}
	for _, n := range in.Nodes {
		if n.Node == writer.Node {
			continue
		}
		if extra := gtid.Subtract(n.Executed, writer.Executed); !extra.Empty() {
			return "", fmt.Sprintf("peer %s has GTIDs not on writer %s: %s", n.Node, writer.Node, extra), false
		}
	}
	return writer.Node, fmt.Sprintf("observed single writable peer %s with consistent GTIDs", writer.Node), true
}

// maybeSeedMaster seeds the writer key from observed topology when appropriate.
func maybeSeedMaster(ctx context.Context, log *slog.Logger, store *consul.Store, guardNode string, in SeedInput, logMax int) (string, error) {
	node, reason, ok := ShouldSeedMaster(in)
	if !ok {
		return "", nil
	}
	epoch, err := store.GetEpoch(ctx)
	if err != nil {
		return "", err
	}
	if epoch == 0 {
		if epoch, err = store.CasBumpEpoch(ctx, 0); err != nil {
			return "", err
		}
	}
	if err := store.SetMaster(ctx, node, epoch); err != nil {
		return "", err
	}
	log.Info("seeded writer key from observed topology", "primary", node, "epoch", epoch, "kv", store.Keys().MasterKV())
	_ = store.AppendActionLog(ctx, actionlog.Entry{
		Timestamp: nowUTC(), Epoch: epoch, Guard: guardNode,
		Action: "seed_primary", Reason: reason, Result: "ok", Step: "seed",
	}, logMax)
	return node, nil
}
