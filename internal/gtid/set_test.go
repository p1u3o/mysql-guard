package gtid

import "testing"

func TestSubtract(t *testing.T) {
	primary := Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-10")
	replica := Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-8")
	m := Subtract(primary, replica)
	if m.Empty() {
		t.Fatalf("expected missing on replica, got empty")
	}
	c := Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-5")
	m = Subtract(primary, c)
	if m.Empty() {
		t.Fatal("expected missing trx")
	}
}

func TestComparePeers(t *testing.T) {
	u := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	peers := map[string]Set{
		"db-b": Parse(u + ":1-10"),
		"db-a": Parse(u + ":1-10"),
	}
	w, ok := ComparePeers(peers)
	if !ok || w != "db-a" {
		t.Fatalf("tie lowest name: got %q ok=%v", w, ok)
	}
	peers["db-b"] = Parse(u + ":1-11")
	w, ok = ComparePeers(peers)
	if !ok || w != "db-b" {
		t.Fatalf("ahead wins: got %q", w)
	}
}
