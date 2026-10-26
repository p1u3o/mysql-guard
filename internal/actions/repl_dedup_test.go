package actions

import (
	"testing"

	"mysql-guard/internal/replhealth"
)

func TestReplAlertsLoggedOnChangeOnly(t *testing.T) {
	e := &Executor{}
	a := []replhealth.Alert{{Code: "repl_state_unknown"}}
	if !e.replAlertsChanged(a) {
		t.Fatal("first tick must report")
	}
	if e.replAlertsChanged(a) {
		t.Fatal("same alert set must not repeat")
	}
	if !e.replAlertsChanged(nil) {
		t.Fatal("clearing must report")
	}
	if e.replAlertsChanged(nil) {
		t.Fatal("steady clear must stay quiet")
	}
}
