package mysql

import (
	"context"
	"database/sql"
	"errors"
	"net"
	"strings"
	"time"

	"mysql-guard/internal/vote"

	_ "github.com/go-sql-driver/mysql"
)

// ProbeConfig for health checks.
type ProbeConfig struct {
	Timeout       time.Duration
	SlowThreshold int
}

// Prober runs MySQL health probes with deadlines.
type Prober struct {
	db  *sql.DB
	cfg ProbeConfig
}

func NewProber(dsn string, cfg ProbeConfig) (*Prober, error) {
	if dsn == "" {
		return &Prober{cfg: cfg}, nil
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	db.SetConnMaxLifetime(0)
	return &Prober{db: db, cfg: cfg}, nil
}

func (p *Prober) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// Ping runs SELECT 1 with timeout; refused/reset => ProbeRefused; over timeout => slow/fail.
func (p *Prober) Ping(ctx context.Context) (vote.ProbeResult, time.Duration) {
	if p.db == nil {
		return vote.ProbeFailed, 0
	}
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	var one int
	err := p.db.QueryRowContext(ctx, "SELECT 1").Scan(&one)
	elapsed := time.Since(start)
	if err != nil {
		if isRefused(err) {
			return vote.ProbeRefused, elapsed
		}
		if errors.Is(err, context.DeadlineExceeded) || elapsed >= p.cfg.Timeout {
			return vote.ProbeSlow, elapsed
		}
		return vote.ProbeFailed, elapsed
	}
	if elapsed >= p.cfg.Timeout {
		return vote.ProbeSlow, elapsed
	}
	return vote.ProbeOK, elapsed
}

func isRefused(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) {
		return false
	}
	var op *net.OpError
	if errors.As(err, &op) {
		return true
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "connection refused") || strings.Contains(s, "reset by peer")
}

// GTIDState reads executed and retrieved sets from the local instance.
type GTIDState struct {
	Executed             string
	Retrieved            string
	ReadOnly             bool
	IOThread             bool
	SQLThread            bool
	LagSeconds           float64
	HasReplicationSource bool
}

func (p *Prober) ReadGTIDState(ctx context.Context) (GTIDState, error) {
	if p.db == nil {
		return GTIDState{}, errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	var st GTIDState
	row := p.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed, @@global.gtid_executed, @@global.read_only")
	if err := row.Scan(&st.Executed, &st.Retrieved, &st.ReadOnly); err != nil {
		return GTIDState{}, err
	}
	var slaveStatus string
	err := p.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&slaveStatus)
	_ = err
	// Replica thread state (MySQL 8): simplified for draft
	var io, sql string
	_ = p.db.QueryRowContext(ctx, "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1").Scan(&io)
	_ = p.db.QueryRowContext(ctx, "SELECT SERVICE_STATE FROM performance_schema.replication_applier_status_by_worker LIMIT 1").Scan(&sql)
	st.IOThread = strings.EqualFold(io, "ON")
	st.SQLThread = sql != ""
	var srcCount int
	_ = p.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM performance_schema.replication_connection_status WHERE SERVICE_STATE IS NOT NULL").Scan(&srcCount)
	st.HasReplicationSource = srcCount > 0
	return st, nil
}

func (p *Prober) SetSuperReadOnly(ctx context.Context) error {
	if p.db == nil {
		return errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	_, err := p.db.ExecContext(ctx, "SET GLOBAL super_read_only=ON")
	return err
}

func (p *Prober) PromoteWritable(ctx context.Context) error {
	if p.db == nil {
		return errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	_, err := p.db.ExecContext(ctx, "SET GLOBAL super_read_only=OFF, read_only=OFF")
	return err
}
