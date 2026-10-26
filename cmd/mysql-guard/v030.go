package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/notify"
)

// storeOptions builds the master-key adapter / writer service options.
// Peer endpoints come from -mysql-peer-endpoints (host[:port], default -mysql-port).
func storeOptions(cfg *config.Config) consul.Options {
	eps := map[string]string{}
	for _, p := range cfg.Peers {
		if _, ok := cfg.MySQLPeerEndpoints[p]; !ok {
			continue
		}
		h, port, err := mysql.PeerHostPort(cfg, p)
		if err == nil {
			eps[p] = net.JoinHostPort(h, strconv.Itoa(port))
		}
	}
	return consul.Options{
		Format:    consul.MasterFormat(cfg.MasterFormat),
		Endpoints: eps,
		Writer:    consul.WriterService{Name: cfg.WriterServiceName, Tags: cfg.WriterServiceTags},
	}
}

func newAlerts(cfg *config.Config, checks notify.ConsulCheckUpdater) *notify.Fanout {
	n := notify.NewNtfy(cfg.NtfyURL)
	n.Token = cfg.NtfyToken
	return &notify.Fanout{Ntfy: n, Consul: checks, Prefix: "guard-", Cluster: cfg.ClusterName}
}

// runMigrateKey: mysql-guard migrate-key -to=epoch|legacy [-dry-run] <guard flags>
// Rewrites mysql/master/<cluster> in the other format with a CAS (see README
// "Master key format"). Refused in observe mode and while unmapped.
func runMigrateKey(args []string) int {
	to, dry := "", false
	var rest []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		switch {
		case a == "-dry-run" || a == "--dry-run":
			dry = true
		case len(a) > 4 && (a[:4] == "-to=" || (len(a) > 5 && a[:5] == "--to=")):
			to = a[len("-to="):]
			if a[1] == '-' {
				to = a[len("--to="):]
			}
		default:
			rest = append(rest, a)
		}
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if to != "epoch" && to != "legacy" {
		log.Error("migrate-key needs -to=epoch or -to=legacy")
		return 2
	}
	cfg, err := config.Load(rest)
	if err != nil {
		log.Error("config", "err", err)
		return 2
	}
	store, err := consul.NewStore(cfg.ConsulAddr, cfg.ConsulToken, cfg.ClusterName)
	if err != nil {
		log.Error("consul", "err", err)
		return 1
	}
	store.SetOptions(storeOptions(cfg))
	ctx := context.Background()
	node, epoch, err := store.GetPrimary(ctx)
	if err != nil {
		log.Error("read master key", "err", err)
		return 1
	}
	val, _ := consul.EncodeMaster(consul.MasterFormat(to), store.Options().Endpoints, node, epoch)
	fmt.Printf("key=%s primary=%s epoch=%d target_format=%s target_value=%s\n", store.Keys().MasterKV(), node, epoch, to, val)
	if dry {
		return 0
	}
	if cfg.Mode == config.ModeObserve {
		log.Error("migrate-key refused: -mode=observe writes nothing")
		return 1
	}
	old, nv, err := store.MigrateMaster(ctx, consul.MasterFormat(to))
	if err != nil {
		log.Error("migrate", "err", err)
		return 1
	}
	fmt.Printf("migrated %s: %q -> %q\n", store.Keys().MasterKV(), old, nv)
	return 0
}
