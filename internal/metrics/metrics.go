package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Registry holds mysql-guard Prometheus metrics from the plan.
type Registry struct {
	Leader            prometheus.Gauge
	DeadVotes         *prometheus.GaugeVec
	ProbeSeconds      prometheus.Histogram
	ProbeSlowTotal    prometheus.Counter
	ProbeFailTotal    prometheus.Counter
	SessionRenewAge   prometheus.Gauge
	Writers           prometheus.Gauge
	GTIDMissing       prometheus.Gauge
	ReplicaLagSeconds prometheus.Gauge
	FailoversTotal    prometheus.Counter
	FencesTotal       prometheus.Counter
	Epoch             prometheus.Gauge
	WouldActTotal     prometheus.Counter

	// semi-sync stall handling
	SemiStallOff       prometheus.Gauge       // 1 while the guard has semi-sync source switched off for a dead replica
	SemiStallDisabled  prometheus.Counter     // semi-sync turned off for a dead replica
	SemiStallReenabled prometheus.Counter     // semi-sync turned on again after the replica returned
	SemiStallSkipped   *prometheus.CounterVec // decisions not to act, by reason
}

// New registers metrics on the default Prometheus registerer.
func New() *Registry { return NewWith(prometheus.DefaultRegisterer) }

// NewWith registers metrics on reg.
func NewWith(reg prometheus.Registerer) *Registry {
	r := &Registry{
		Leader:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_leader", Help: "1 if this instance holds the Consul leader lock"}),
		DeadVotes: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "mysql_guard_dead_votes", Help: "dead vote count per channel"}, []string{"channel"}),
		ProbeSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "mysql_guard_probe_seconds",
			Help:    "MySQL probe latency",
			Buckets: prometheus.DefBuckets,
		}),
		ProbeSlowTotal:     prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_probe_slow_total", Help: "slow probes"}),
		ProbeFailTotal:     prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_probe_fail_total", Help: "failed probes"}),
		SessionRenewAge:    prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_session_renew_age_seconds", Help: "seconds since last session renew"}),
		Writers:            prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_writers", Help: "count of writable nodes observed"}),
		GTIDMissing:        prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_gtid_missing_trx", Help: "1 if promotion blocked by missing GTIDs"}),
		ReplicaLagSeconds:  prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_replica_lag_seconds", Help: "replica apply lag"}),
		FailoversTotal:     prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_failovers_total", Help: "failovers executed"}),
		FencesTotal:        prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_fences_total", Help: "fences executed"}),
		Epoch:              prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_epoch", Help: "current writer epoch"}),
		WouldActTotal:      prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_would_act_total", Help: "actions that would run in observe mode"}),
		SemiStallOff:       prometheus.NewGauge(prometheus.GaugeOpts{Name: "mysql_guard_semisync_stall_off", Help: "1 while the guard has turned semi-sync source off because the replica is dead"}),
		SemiStallDisabled:  prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_semisync_stall_disabled_total", Help: "times semi-sync source was turned off for a dead replica"}),
		SemiStallReenabled: prometheus.NewCounter(prometheus.CounterOpts{Name: "mysql_guard_semisync_stall_reenabled_total", Help: "times semi-sync source was turned on again after the replica returned"}),
		SemiStallSkipped:   prometheus.NewCounterVec(prometheus.CounterOpts{Name: "mysql_guard_semisync_stall_skipped_total", Help: "semi-sync stall actions not taken, by reason"}, []string{"reason"}),
	}
	reg.MustRegister(
		r.Leader, r.DeadVotes, r.ProbeSeconds, r.ProbeSlowTotal, r.ProbeFailTotal,
		r.SessionRenewAge, r.Writers, r.GTIDMissing, r.ReplicaLagSeconds,
		r.FailoversTotal, r.FencesTotal, r.Epoch, r.WouldActTotal,
		r.SemiStallOff, r.SemiStallDisabled, r.SemiStallReenabled, r.SemiStallSkipped,
	)
	return r
}
