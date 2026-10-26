package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"mysql-guard/internal/actions"
	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/gtid"
	"mysql-guard/internal/guard"
	"mysql-guard/internal/metrics"
	"mysql-guard/internal/mysql"
	"mysql-guard/internal/proxysql"
	"mysql-guard/internal/server"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "migrate-key" {
		return runMigrateKey(args[1:])
	}
	if len(args) > 0 && isSubcommand(args[0]) {
		return runCLI(args)
	}

	cfg, err := config.Load(args)
	if err != nil {
		slog.Error("config", "err", err)
		return 2
	}
	return runServer(cfg)
}

func runServer(cfg *config.Config) int {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	store, err := consul.NewStore(cfg.ConsulAddr, cfg.ConsulToken, cfg.ClusterName)
	if err != nil {
		log.Error("consul", "err", err)
		return 1
	}
	store.SetOptions(storeOptions(cfg))
	log.Info("master key adapter", "format", cfg.MasterFormat, "writer_service", cfg.WriterServiceName, "proxysql_verify", cfg.ProxySQLVerify)
	if cfg.ConsulServiceID != "" {
		if err := consul.RegisterAgentService(context.Background(), store.Client(), cfg.ConsulServiceName, cfg.ConsulServiceID, cfg.HTTPListen); err != nil {
			log.Warn("consul service register", "err", err)
		} else {
			log.Info("consul service registered", "id", cfg.ConsulServiceID, "name", cfg.ConsulServiceName)
		}
	}
	lock := consul.NewLeaderLock(store, cfg.NodeName, cfg.SessionTTL, cfg.LockDelay)

	prober, err := mysql.NewProber(cfg.MySQLDSN, mysql.ProbeConfig{Timeout: cfg.ProbeTimeout, SlowThreshold: cfg.SlowThreshold})
	if err != nil {
		log.Error("mysql", "err", err)
		return 1
	}
	peerPool, err := mysql.NewPool(cfg)
	if err != nil {
		log.Warn("mysql peer pool partial", "err", err)
	}
	px, err := proxysql.NewClient(cfg.ProxySQLConfig())
	if err != nil {
		log.Error("proxysql", "err", err)
		return 1
	}
	checks := consul.NewAgentChecks(store.Client(), cfg.ConsulServiceID, log)
	alerts := newAlerts(cfg, checks)
	mreg := metrics.New()
	exec := actions.NewExecutor(cfg, log, prober, peerPool, px, store, alerts)
	rt := guard.NewRuntime(cfg, log, store, lock, prober, peerPool, px, exec, mreg)
	rt.SetHealthReporter(checks)
	httpsrv := server.New(rt, store, mreg)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Only DB guards compete for leadership; witnesses never act on MySQL.
	if cfg.Role == config.RoleDB {
		go lock.RunRenewalLoop()
	}
	rt.Run(ctx)

	srv := &http.Server{Addr: cfg.HTTPListen, Handler: httpsrv.Handler(), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		log.Info("mysql-guard listen", "addr", cfg.HTTPListen, "mode", cfg.Mode, "version", version)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("http", "err", err)
			cancel()
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	_ = srv.Shutdown(shutdownCtx)
	lock.Stop()
	_ = prober.Close()
	if peerPool != nil {
		peerPool.Close()
	}
	_ = px.Close()
	return 0
}

// version is set at build time: -ldflags "-X main.version=vX.Y.Z".
var version = "dev"

func init() { guard.Version = version }

func isSubcommand(s string) bool {
	switch s {
	case "switchover", "handover", "rejoin":
		return true
	default:
		return false
	}
}

func runCLI(args []string) int {
	sub := args[0]
	cfg, target, err := config.LoadSubcommand(args[1:])
	if err != nil {
		slog.Error("config", "err", err)
		return 2
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	store, err := consul.NewStore(cfg.ConsulAddr, cfg.ConsulToken, cfg.ClusterName)
	if err != nil {
		log.Error("consul", "err", err)
		return 1
	}
	store.SetOptions(storeOptions(cfg))
	prober, _ := mysql.NewProber(cfg.MySQLDSN, mysql.ProbeConfig{Timeout: cfg.ProbeTimeout})
	peerPool, _ := mysql.NewPool(cfg)
	px, _ := proxysql.NewClient(cfg.ProxySQLConfig())
	exec := actions.NewExecutor(cfg, log, prober, peerPool, px, store, newAlerts(cfg, nil))
	ctx := context.Background()
	readExecuted := func(peer string) gtid.Set {
		if peerPool != nil {
			if st, err := peerPool.ReadState(ctx, peer); err == nil {
				return gtid.Parse(st.Executed)
			}
		}
		if st, err := prober.ReadGTIDState(ctx); err == nil && peer == cfg.NodeName {
			return gtid.Parse(st.Executed)
		}
		return gtid.Set("")
	}
	switch sub {
	case "switchover", "handover":
		if target == "" {
			target = cfg.Peers[len(cfg.Peers)-1]
		}
		oldPrimary, newPrimary := mysql.ResolveOldNewPrimary(cfg, target)
		if cur, _, err := store.GetPrimary(ctx); err == nil && cur != "" && cur != newPrimary {
			oldPrimary = cur
		}
		return exitErr(exec.Switchover(ctx, newPrimary, readExecuted(oldPrimary), readExecuted(newPrimary)))
	case "rejoin":
		if target == "" {
			target = cfg.Peers[0]
		}
		_, err := exec.Rejoin(ctx, target, cfg.NodeName, readExecuted(cfg.NodeName), readExecuted(target), true)
		return exitErr(err)
	default:
		return 2
	}
}

func exitErr(err error) int {
	if err != nil {
		slog.Error("command failed", "err", err)
		return 1
	}
	return 0
}
