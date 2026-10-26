package guard

import (
	"strings"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/gtid"
)

func TestSupersetCheck(t *testing.T) {
	lw := consul.WriterGTID{Node: "a", Epoch: 3, Executed: "u:1-10"}
	if ok, _ := supersetCheck(lw, "a", 3, "b", "u:1-8", "u:1-10"); !ok {
		t.Fatal("retrieved covers writer: must pass")
	}
	ok, why := supersetCheck(lw, "a", 3, "b", "u:1-8", "")
	if ok || !strings.Contains(why, "u:9-10") {
		t.Fatalf("want refusal with missing u:9-10, got %v %q", ok, why)
	}
	if ok, _ := supersetCheck(lw, "a", 3, "b", "", ""); ok {
		t.Fatal("empty replica must be refused")
	}
	if ok, _ := supersetCheck(lw, "a", 4, "b", "", ""); !ok {
		t.Fatal("record from another epoch is not applied")
	}
	_ = gtid.Set("")
}

func TestStalledCheckWarning(t *testing.T) {
	st, out := guardCheckStatus(tickHealth{role: config.RoleDB, stalled: []string{"kv01-par"}})
	if st != "warning" || !strings.Contains(out, "kv01-par") {
		t.Fatalf("%s %s", st, out)
	}
}

func TestSupersetRetrievedOverlapsExecuted(t *testing.T) {
	lw := consul.WriterGTID{Node: "a", Epoch: 3, Executed: "b2:1-17,bb:1-3"}
	if ok, why := supersetCheck(lw, "a", 3, "b", "b2:1-17,\nbb:1-3", "b2:12-17"); !ok {
		t.Fatal(why)
	}
}
