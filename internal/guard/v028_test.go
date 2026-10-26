package guard

import (
	"errors"
	"strings"
	"testing"

	"mysql-guard/internal/config"
	"mysql-guard/internal/consul"
	"mysql-guard/internal/mysql"
)

func TestPrimaryComplete(t *testing.T) {
	r := &Runtime{cfg: &config.Config{Peers: []string{"a", "b"}}}
	pa := peerObs{st: mysql.GTIDState{Executed: "u:1-10,v:1-3", ReadOnly: true}}
	obs := map[string]peerObs{"a": pa, "b": {st: mysql.GTIDState{Executed: "u:1-10"}}}
	if ok, why := r.primaryComplete("a", pa, obs, consul.WriterGTID{}); !ok {
		t.Fatalf("superset must be complete: %s", why)
	}
	obs["b"] = peerObs{st: mysql.GTIDState{Executed: "u:1-10,w:1"}}
	ok, why := r.primaryComplete("a", pa, obs, consul.WriterGTID{})
	if ok || !strings.Contains(why, "w:1") {
		t.Fatalf("peer has w:1 the primary lacks: got %v %q", ok, why)
	}
	obs["b"] = peerObs{err: errors.New("down")}
	if ok, _ := r.primaryComplete("a", pa, obs, consul.WriterGTID{}); ok {
		t.Fatal("unreachable peer without last-writer set must be incomplete")
	}
	if ok, _ := r.primaryComplete("a", pa, obs, consul.WriterGTID{Executed: "u:1-10"}); !ok {
		t.Fatal("unreachable peer, primary covers last-writer set: complete")
	}
	if ok, _ := r.primaryComplete("a", pa, obs, consul.WriterGTID{Executed: "u:1-11"}); ok {
		t.Fatal("primary missing last-writer GTIDs must be incomplete")
	}
}
