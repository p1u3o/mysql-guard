package replhealth

import "testing"

func TestUnknownNeverReportsStopped(t *testing.T) {
	alerts := Evaluate(State{Node: "db-b", Unknown: true, UnknownErr: "denied", ReadOnly: true, ExpectReadOnly: true}, Config{MaxLagSeconds: 1})
	if len(alerts) != 1 || alerts[0].Code != "repl_state_unknown" {
		t.Fatalf("want only repl_state_unknown, got %+v", alerts)
	}
	alerts = Evaluate(State{Node: "db-b", Unknown: true, ReadOnly: false, ExpectReadOnly: true}, Config{})
	codes := map[string]bool{}
	for _, a := range alerts {
		codes[a.Code] = true
	}
	if !codes["read_only_drift"] || codes["repl_io_stopped"] || codes["repl_sql_stopped"] {
		t.Fatalf("read_only drift is still known; threads are not: %+v", alerts)
	}
}
