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
	db *sql.DB
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
	return a.exec(ctx, "STOP REPLICA")
}

func (a *Admin) ResetReplicaAll(ctx context.Context) error {
	return a.exec(ctx, "RESET REPLICA ALL")
}

func (a *Admin) StartReplica(ctx context.Context) error {
	return a.exec(ctx, "START REPLICA")
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
	var i, e sql.NullString
	if err = a.db.QueryRowContext(ctx, "SELECT SERVICE_STATE, LAST_ERROR_MESSAGE FROM performance_schema.replication_connection_status LIMIT 1").Scan(&i, &e); err != nil {
		return "", "", "", err
	}
	var s sql.NullString
	if err = a.db.QueryRowContext(ctx, "SELECT SERVICE_STATE FROM performance_schema.replication_applier_status LIMIT 1").Scan(&s); err != nil {
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
	return a.exec(ctx, q)
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
