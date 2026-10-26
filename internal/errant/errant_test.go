package errant

import (
	"testing"

	"mysql-guard/internal/gtid"
)

func TestDetectErrant(t *testing.T) {
	u := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	primary := gtid.Parse(u + ":1-100")
	replica := gtid.Parse(u + ":1-100")
	r := Detect(replica, primary)
	if r.Errant {
		t.Fatal("expected no errant")
	}
	replica = gtid.Parse(u + ":1-101")
	r = Detect(replica, primary)
	if !r.Errant {
		t.Fatal("expected errant")
	}
}

func TestBlocksPromotion(t *testing.T) {
	u := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
	r := BlocksPromotion(gtid.Parse(u+":1-50"), gtid.Parse(u+":1-100"))
	if r.Errant {
		t.Fatal("replica behind primary is not errant")
	}
	r = BlocksPromotion(gtid.Parse(u+":1-110"), gtid.Parse(u+":1-100"))
	if !r.Errant {
		t.Fatal("expected errant ahead of primary")
	}
}
