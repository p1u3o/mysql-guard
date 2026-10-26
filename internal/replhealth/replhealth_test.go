package replhealth

import "testing"

func TestReplicationAlerts(t *testing.T) {
	alerts := Evaluate(State{
		Node: "db-b", IOThreadOn: false, SQLThreadOn: true,
		ReadOnly: true, ExpectReadOnly: true, LagSeconds: 1,
	}, Config{MaxLagSeconds: 30})
	if len(alerts) != 1 || alerts[0].Code != "repl_io_stopped" {
		t.Fatalf("got %+v", alerts)
	}
	alerts = Evaluate(State{
		Node: "db-b", IOThreadOn: true, SQLThreadOn: true,
		ReadOnly: false, ExpectReadOnly: true, LagSeconds: 60,
	}, Config{MaxLagSeconds: 30})
	if len(alerts) != 2 {
		t.Fatalf("expected lag+drift, got %+v", alerts)
	}
}
