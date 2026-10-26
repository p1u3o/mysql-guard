package rejoin

import (
	"testing"

	"mysql-guard/internal/gtid"
)

func u() gtid.Set {
	return gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-100")
}

func TestRejoinAutoPosition(t *testing.T) {
	local := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-90")
	p := Evaluate(local, u(), true)
	if p.Outcome != OutcomeAutoPosition {
		t.Fatalf("got %s", p.Outcome)
	}
}

func TestRejoinNeedsRebuild(t *testing.T) {
	local := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-90")
	p := Evaluate(local, u(), false)
	if p.Outcome != OutcomeNeedsRebuild {
		t.Fatalf("got %s", p.Outcome)
	}
}

func TestRejoinBlockedErrant(t *testing.T) {
	local := gtid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:1-110")
	p := Evaluate(local, u(), true)
	if p.Outcome != OutcomeBlockedErrant {
		t.Fatalf("got %s", p.Outcome)
	}
}
