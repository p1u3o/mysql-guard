package guard

import (
	"testing"
	"time"
)

func TestVoteMaxAgeCoversSlowTicks(t *testing.T) {
	if got := VoteMaxAge(2*time.Second, 3*time.Second); got != 13*time.Second {
		t.Fatalf("got %s", got)
	}
}
