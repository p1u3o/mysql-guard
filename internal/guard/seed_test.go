package guard

import "testing"

func seedBase() SeedInput {
	return SeedInput{IsLeader: true, Nodes: []SeedNode{
		{Node: "db-a", Reachable: true, Writable: true, Executed: "aaaa:1-8"},
		{Node: "db-b", Reachable: true, Writable: false, Executed: "aaaa:1-8"},
	}}
}

func TestShouldSeedMasterSingleWriter(t *testing.T) {
	n, _, ok := ShouldSeedMaster(seedBase())
	if !ok || n != "db-a" {
		t.Fatalf("expected db-a seed, got %q %v", n, ok)
	}
	in := seedBase()
	in.Nodes[0].Writable, in.Nodes[1].Writable = false, true
	in.Nodes[1].Executed = "aaaa:1-9"
	if n, _, ok := ShouldSeedMaster(in); !ok || n != "db-b" {
		t.Fatalf("expected observed writer db-b, got %q %v", n, ok)
	}
}

func TestShouldSeedMasterRefuses(t *testing.T) {
	cases := map[string]func(*SeedInput){
		"not leader":    func(in *SeedInput) { in.IsLeader = false },
		"key exists":    func(in *SeedInput) { in.MasterExists = true },
		"two writers":   func(in *SeedInput) { in.Nodes[1].Writable = true },
		"no writer":     func(in *SeedInput) { in.Nodes[0].Writable = false },
		"unreachable":   func(in *SeedInput) { in.Nodes[1].Reachable = false },
		"errant gtid":   func(in *SeedInput) { in.Nodes[1].Executed = "aaaa:1-8,bbbb:1-5" },
		"replica ahead": func(in *SeedInput) { in.Nodes[1].Executed = "aaaa:1-9" },
		"no peers":      func(in *SeedInput) { in.Nodes = nil },
	}
	for name, mut := range cases {
		in := seedBase()
		mut(&in)
		if n, reason, ok := ShouldSeedMaster(in); ok {
			t.Errorf("%s: expected no seed, got %q (%s)", name, n, reason)
		}
	}
}
