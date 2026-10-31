package guard

import (
	"testing"
	"time"

	"mysql-guard/internal/consul"
)

func TestReplicaWitnessView(t *testing.T) {
	now := time.Unix(1000, 0)
	age := 10 * time.Second
	w := func(view string, hb time.Duration, probes map[string]string) consul.GuardInstance {
		return consul.GuardInstance{Node: "prod02", Role: "witness", PrimaryView: view, LastHeartbeat: now.Add(-hb), PeerProbes: probes}
	}
	cases := []struct {
		name string
		in   []consul.GuardInstance
		want string
	}{
		{"none", nil, ""},
		{"dead", []consul.GuardInstance{w("a", time.Second, map[string]string{"b": "dead"})}, "dead"},
		{"ok", []consul.GuardInstance{w("a", time.Second, map[string]string{"b": "ok"})}, "ok"},
		{"stale heartbeat ignored", []consul.GuardInstance{w("a", time.Minute, map[string]string{"b": "dead"})}, ""},
		{"other primary view ignored", []consul.GuardInstance{w("b", time.Second, map[string]string{"b": "dead"})}, ""},
		{"no peer probes (old witness)", []consul.GuardInstance{w("a", time.Second, nil)}, ""},
		{"ok beats dead", []consul.GuardInstance{w("a", time.Second, map[string]string{"b": "dead"}), w("a", time.Second, map[string]string{"b": "ok"})}, "ok"},
		{"db guard is not a witness", []consul.GuardInstance{{Node: "a", Role: "db", PrimaryView: "a", LastHeartbeat: now, PeerProbes: map[string]string{"b": "dead"}}}, ""},
	}
	for _, c := range cases {
		if got := ReplicaWitnessView(c.in, "a", "b", now, age); got != c.want {
			t.Errorf("%s: got %q want %q", c.name, got, c.want)
		}
	}
}
