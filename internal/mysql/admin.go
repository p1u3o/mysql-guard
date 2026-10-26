package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"mysql-guard/internal/gtid"
)

// Admin runs replication admin SQL on the local connection.
type Admin struct {
	db          *sql.DB
	repl        *sql.DB       // replication-statement pool (long read timeout); falls back to db
	stmtTimeout time.Duration // per replication statement; default 30s
}

func (a *Admin) replDB() *sql.DB {
	if a.repl != nil {
		return a.repl
	}
	return a.db
}

func (a *Admin) replTimeout() time.Duration {
	if a.stmtTimeout > 0 {
		return a.stmtTimeout
	}
	return 30 * time.Second
}

// replExec runs a replication statement on the long-timeout pool.
func (a *Admin) replExec(ctx context.Context, q string) error {
	db := a.replDB()
	if db == nil {
		return errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, a.replTimeout())
	defer cancel()
	_, err := db.ExecContext(ctx, q)
	return err
}

func NewAdmin(p *Prober) *Admin {
	if p == nil || p.db == nil {
		return &Admin{}
	}
	return &Admin{db: p.db}
}

func NewAdminDB(db *sql.DB) *Admin {
	return &Admin{db: db}
}

func (a *Admin) exec(ctx context.Context, q string, args ...any) error {
	if a.db == nil {
		return errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := a.db.ExecContext(ctx, q, args...)
	return err
}

func (a *Admin) SetSuperReadOnly(ctx context.Context, on bool) error {
	if on {
		return a.exec(ctx, "SET GLOBAL super_read_only=ON")
	}
	if err := a.exec(ctx, "SET GLOBAL super_read_only=OFF"); err != nil {
		return err
	}
	return a.exec(ctx, "SET GLOBAL read_only=OFF")
}

func (a *Admin) StopReplica(ctx context.Context) error {
	return a.replExec(ctx, "STOP REPLICA")
}

func (a *Admin) ResetReplicaAll(ctx context.Context) error {
	return a.replExec(ctx, "RESET REPLICA ALL")
}

func (a *Admin) StartReplica(ctx context.Context) error {
	return a.replExec(ctx, "START REPLICA")
}

func (a *Admin) PromoteWritable(ctx context.Context) error {
	if err := a.SetSuperReadOnly(ctx, false); err != nil {
		return err
	}
	return a.exec(ctx, "SET GLOBAL read_only=OFF")
}

func (a *Admin) ReadSuperReadOnly(ctx context.Context) (bool, error) {
	if a.db == nil {
		return false, errors.New("no mysql connection")
	}
	var v int
	err := a.db.QueryRowContext(ctx, "SELECT @@global.super_read_only").Scan(&v)
	return v == 1, err
}

func (a *Admin) GTIDExecuted(ctx context.Context) (gtid.Set, error) {
	if a.db == nil {
		return "", errors.New("no mysql connection")
	}
	var s string
	err := a.db.QueryRowContext(ctx, "SELECT @@global.gtid_executed").Scan(&s)
	return gtid.Parse(s), err
}

func (a *Admin) GTIDPurged(ctx context.Context) (gtid.Set, error) {
	if a.db == nil {
		return "", errors.New("no mysql connection")
	}
	var s string
	err := a.db.QueryRowContext(ctx, "SELECT @@global.gtid_purged").Scan(&s)
	return gtid.Parse(s), err
}

// WaitExecuted waits until gtid_executed contains target (simplified poll).
func (a *Admin) WaitExecuted(ctx context.Context, target gtid.Set, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cur, err := a.GTIDExecuted(ctx)
		if err != nil {
			return err
		}
		if gtid.Subtract(target, cur).Empty() {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for executed GTID set")
}

// ReplicaThreads returns the IO (connection) and SQL (applier) service states
// and the last IO error from performance_schema.
func (a *Admin) ReplicaThreads(ctx context.Context) (io, sqlState, ioErr string, err error) {
	db := a.replDB()
	if db == nil {
		return "", "", "", errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, a.replTimeout())
	defer cancel()
	var i, e sql.NullString
	if err = db.QueryRowContext(ctx, "SELECT SERVICE_STATE, LAST_ERROR_MESSAGE FROM performance_schema.replication_connection_status LIMIT 1").Scan(&i, &e); err != nil {
		return "", "", "", err
	}
	var s sql.NullString
	if err = db.QueryRowContext(ctx, "SELECT SERVICE_STATE FROM performance_schema.replication_applier_status LIMIT 1").Scan(&s); err != nil {
		return "", "", "", err
	}
	return i.String, s.String, e.String, nil
}

// WaitReplicaConnected polls until both replication threads are ON. On timeout
// it returns an error carrying the last observed states and IO error.
func (a *Admin) WaitReplicaConnected(ctx context.Context, timeout time.Duration) error {
	return waitThreads(ctx, a.ReplicaThreads, timeout, 500*time.Millisecond)
}

type threadReader func(ctx context.Context) (io, sqlState, ioErr string, err error)

func waitThreads(ctx context.Context, read threadReader, timeout, every time.Duration) error {
	deadline := time.Now().Add(timeout)
	last := "no reading"
	for {
		io, st, ioErr, err := read(ctx)
		if err == nil && io == "ON" && st == "ON" {
			return nil
		}
		if err != nil {
			last = err.Error()
		} else {
			last = fmt.Sprintf("io=%s sql=%s", io, st)
			if ioErr != "" {
				last += " io_error=" + ioErr
			}
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("replica not connected after %s: %s", timeout, last)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(every):
		}
	}
}

// ChangeSourceAuto sets replication source with auto positioning.
func (a *Admin) ChangeSourceAuto(ctx context.Context, host string, port int, user, password string) error {
	q := fmt.Sprintf(
		"CHANGE REPLICATION SOURCE TO SOURCE_HOST='%s', SOURCE_PORT=%d, SOURCE_USER='%s', SOURCE_PASSWORD='%s', SOURCE_AUTO_POSITION=1, GET_SOURCE_PUBLIC_KEY=1",
		escapeSQL(host), port, escapeSQL(user), escapeSQL(password),
	)
	return a.replExec(ctx, q)
}

// ReplicaSource reads the configured source of the default channel.
func (a *Admin) ReplicaSource(ctx context.Context) (host string, port int, user string, err error) {
	db := a.replDB()
	if db == nil {
		return "", 0, "", errors.New("no mysql connection")
	}
	ctx, cancel := context.WithTimeout(ctx, a.replTimeout())
	defer cancel()
	var h, u sql.NullString
	var p sql.NullInt64
	err = db.QueryRowContext(ctx, "SELECT HOST, PORT, USER FROM performance_schema.replication_connection_configuration WHERE CHANNEL_NAME='' LIMIT 1").Scan(&h, &p, &u)
	return h.String, int(p.Int64), u.String, err
}

// ChangeSourceVerified runs CHANGE REPLICATION SOURCE. If the statement errors
// (e.g. dropped connection), it reconnects and reads the actual source config;
// when that matches, the change is treated as applied (recovered=true).
func (a *Admin) ChangeSourceVerified(ctx context.Context, host string, port int, user, password string) (recovered bool, err error) {
	err = a.ChangeSourceAuto(ctx, host, port, user, password)
	if err == nil {
		return false, nil
	}
	return verifySource(ctx, err, a.ReplicaSource, host, port, user, 3, 500*time.Millisecond)
}

type sourceReader func(ctx context.Context) (string, int, string, error)

func verifySource(ctx context.Context, orig error, read sourceReader, host string, port int, user string, tries int, every time.Duration) (bool, error) {
	var last error
	for i := 0; i < tries; i++ {
		h, p, u, err := read(ctx)
		if err == nil {
			if h == host && p == port && u == user {
				return true, nil
			}
			return false, fmt.Errorf("%v; actual source %s:%d user %s", orig, h, p, u)
		}
		last = err
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-time.After(every):
		}
	}
	return false, fmt.Errorf("%v; source state unreadable: %v", orig, last)
}

// StartReplicaRetry issues START REPLICA with exponential backoff. Between
// attempts (and after a failed/dropped statement) it reads the actual thread
// state: SQL ON and IO ON/CONNECTING counts as started. Returns attempts used
// and whether success was established by reading state after a statement error.
func (a *Admin) StartReplicaRetry(ctx context.Context, attempts int, base time.Duration) (int, bool, error) {
	return startRetry(ctx, a.StartReplica, a.ReplicaThreads, attempts, base)
}

func startRetry(ctx context.Context, start func(context.Context) error, read threadReader, attempts int, base time.Duration) (int, bool, error) {
	if attempts < 1 {
		attempts = 1
	}
	var last error
	for i := 1; i <= attempts; i++ {
		err := start(ctx)
		if err == nil {
			return i, false, nil
		}
		last = err
		if io, st, _, rerr := read(ctx); rerr == nil && st == "ON" && (io == "ON" || io == "CONNECTING") {
			return i, true, nil
		}
		if i == attempts {
			break
		}
		d := base << (i - 1)
		if d > 8*time.Second {
			d = 8 * time.Second
		}
		select {
		case <-ctx.Done():
			return i, false, ctx.Err()
		case <-time.After(d):
		}
	}
	return attempts, false, fmt.Errorf("START REPLICA failed after %d attempts: %w", attempts, last)
}

func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// PurgedBlocksRejoin checks source gtid_purged against joiner gtid_executed.
func PurgedBlocksRejoin(sourcePurged, joinerExecuted, sourceExecuted gtid.Set) bool {
	behind := gtid.Subtract(joinerExecuted, sourceExecuted)
	if behind.Empty() {
		return false
	}
	// joiner ahead of source executed — errant path should have caught this
	return !gtid.Subtract(behind, sourcePurged).Empty()
}
