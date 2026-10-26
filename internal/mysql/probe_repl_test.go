package mysql

import (
	"context"
	"errors"
	"testing"
)

type fakeRow struct {
	val   string
	valid bool
	found bool
	err   error
}

func fakeScan(rows map[string]fakeRow) stringScanner {
	return func(_ context.Context, q string) (string, bool, bool, error) {
		r := rows[q]
		return r.val, r.valid, r.found, r.err
	}
}

func TestReadReplicationErrorIsUnknownNotStopped(t *testing.T) {
	denied := errors.New("Error 1142: SELECT command denied to user 'guard'")
	for name, rows := range map[string]map[string]fakeRow{
		"received denied": {qReceived: {err: denied}},
		"io denied":       {qReceived: {val: "u:1-5", valid: true, found: true}, qIOState: {err: denied}},
		"applier denied":  {qReceived: {val: "u:1-5", valid: true, found: true}, qIOState: {val: "ON", valid: true, found: true}, qSQLState: {err: denied}},
	} {
		st := GTIDState{Retrieved: "stale"}
		readReplication(context.Background(), fakeScan(rows), &st)
		if st.ReplKnown || st.ReplErr == "" {
			t.Fatalf("%s: want unknown with error, got %+v", name, st)
		}
		if st.ReplState() != "unknown" {
			t.Fatalf("%s: ReplState=%q, want unknown", name, st.ReplState())
		}
		if st.Retrieved != "" || st.HasReplicationSource {
			t.Fatalf("%s: unknown state must not carry replication data: %+v", name, st)
		}
	}
}

func TestReadReplicationStates(t *testing.T) {
	on := fakeRow{val: "ON", valid: true, found: true}
	off := fakeRow{val: "OFF", valid: true, found: true}
	recv := fakeRow{val: "u:1-9", valid: true, found: true}
	cases := []struct {
		name    string
		rows    map[string]fakeRow
		io, sql bool
		src     bool
		state   string
	}{
		{"running", map[string]fakeRow{qReceived: recv, qIOState: on, qSQLState: on}, true, true, true, "running"},
		{"stopped", map[string]fakeRow{qReceived: recv, qIOState: off, qSQLState: off}, false, false, true, "stopped"},
		// v0.2.1 counted any applier row as running; OFF must be stopped.
		{"sql off", map[string]fakeRow{qReceived: recv, qIOState: on, qSQLState: off}, true, false, true, "partial"},
		{"no channel", map[string]fakeRow{}, false, false, false, "none"},
	}
	for _, c := range cases {
		var st GTIDState
		readReplication(context.Background(), fakeScan(c.rows), &st)
		if !st.ReplKnown || st.ReplErr != "" {
			t.Fatalf("%s: want known, got %+v", c.name, st)
		}
		if st.IOThread != c.io || st.SQLThread != c.sql || st.HasReplicationSource != c.src || st.ReplState() != c.state {
			t.Fatalf("%s: got io=%v sql=%v src=%v state=%s", c.name, st.IOThread, st.SQLThread, st.HasReplicationSource, st.ReplState())
		}
	}
}
