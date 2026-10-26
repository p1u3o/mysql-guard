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

func (p *Prober) DB() *sql.DB { return p.db }

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
//
// Replication thread fields are only meaningful when ReplKnown is true. When a
// replication query fails (for example the guard user cannot read
// performance_schema), ReplKnown is false and ReplErr carries the error: callers
// must treat the replication state as unknown, never as stopped, and must not
// act on it.
type GTIDState struct {
	Executed             string
	Retrieved            string
	ReadOnly             bool
	SuperReadOnly        bool
	IOThread             bool
	SQLThread            bool
	LagSeconds           float64
	HasReplicationSource bool
	ReplKnown            bool
	ReplErr              string
}

// ReplState summarises replication for display: running, stopped, partial,
// none (no replication source configured) or unknown.
func (s GTIDState) ReplState() string {
	switch {
	case !s.ReplKnown:
		return "unknown"
	case !s.HasReplicationSource:
		return "none"
	case s.IOThread && s.SQLThread:
		return "running"
	case !s.IOThread && !s.SQLThread:
		return "stopped"
	default:
		return "partial"
	}
}

// rowQuerier is the subset of *sql.DB used by the replication probe (tests stub it).
type rowQuerier interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

const (
	qReceived = "SELECT RECEIVED_TRANSACTION_SET FROM performance_schema.replication_connection_status LIMIT 1"
	qIOState  = "SELECT SERVICE_STATE FROM performance_schema.replication_connection_status LIMIT 1"
	qSQLState = "SELECT SERVICE_STATE FROM performance_schema.replication_applier_status LIMIT 1"
)

// scanString runs a single-column query. A missing row is not an error (no
// replication channel); any other error is returned.
type stringScanner func(ctx context.Context, q string) (val string, valid bool, found bool, err error)

func dbScanner(db rowQuerier) stringScanner {
	return func(ctx context.Context, q string) (string, bool, bool, error) {
		var v sql.NullString
		err := db.QueryRowContext(ctx, q).Scan(&v)
		if errors.Is(err, sql.ErrNoRows) {
			return "", false, false, nil
		}
		if err != nil {
			return "", false, false, err
		}
		return v.String, v.Valid, true, nil
	}
}

// readReplication fills the replication fields of st. It never turns an error
// into "stopped": on any query error ReplKnown stays false and ReplErr is set.
func readReplication(ctx context.Context, scan stringScanner, st *GTIDState) {
	fail := func(what string, err error) {
		st.ReplKnown = false
		st.IOThread, st.SQLThread, st.HasReplicationSource = false, false, false
		st.Retrieved = ""
		st.ReplErr = what + ": " + err.Error()
	}
	recv, recvValid, _, err := scan(ctx, qReceived)
	if err != nil {
		fail("replication_connection_status", err)
		return
	}
	io, _, ioFound, err := scan(ctx, qIOState)
	if err != nil {
		fail("replication_connection_status", err)
		return
	}
	sqlState, _, _, err := scan(ctx, qSQLState)
	if err != nil {
		fail("replication_applier_status", err)
		return
	}
	st.ReplKnown = true
	st.ReplErr = ""
	if recvValid {
		st.Retrieved = recv
	} else {
		st.Retrieved = ""
	}
	st.HasReplicationSource = ioFound
	st.IOThread = strings.EqualFold(io, "ON")
	st.SQLThread = strings.EqualFold(sqlState, "ON")
}

// ReadGTIDState returns an error only when the base variables cannot be read
// (node unreachable). Replication query errors are surfaced via ReplKnown/ReplErr.
func (p *Prober) ReadGTIDState(ctx context.Context) (GTIDState, error) {
	if p.db == nil {
		return GTIDState{}, errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, p.cfg.Timeout)
	defer cancel()
	var st GTIDState
	row := p.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed, @@global.read_only, @@global.super_read_only")
	if err := row.Scan(&st.Executed, &st.ReadOnly, &st.SuperReadOnly); err != nil {
		return GTIDState{}, err
	}
	readReplication(ctx, dbScanner(p.db), &st)
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
