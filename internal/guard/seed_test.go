package guard

import "testing"

func TestShouldSeedMaster(t *testing.T) {
	base := SeedInput{IsLeader: true, MasterExists: false, NodeName: "db-a", FirstPeer: "db-a", Writable: true, HasReplSource: false}
	if !ShouldSeedMaster(base) {
		t.Fatal("expected seed")
	}
	base.MasterExists = true
	if ShouldSeedMaster(base) {
		t.Fatal("exists")
	}
	base.MasterExists = false
	base.NodeName = "db-b"
	if ShouldSeedMaster(base) {
		t.Fatal("wrong node")
	}
	base.NodeName = "db-a"
	base.HasReplSource = true
	if ShouldSeedMaster(base) {
		t.Fatal("replica")
	}
}
