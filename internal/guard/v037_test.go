package guard

import (
	"testing"
	"time"
)

func TestRecoveryHold(t *testing.T) {
	t0 := time.Unix(1000, 0)
	vote := 10 * time.Second
	h := primaryHold{since: t0}
	if !recoveryHold(h, t0.Add(30*time.Second), 20*time.Second, 10*time.Second, vote) {
		t.Fatal("want hold inside window")
	}
	if recoveryHold(h, t0.Add(41*time.Second), 20*time.Second, 10*time.Second, vote) {
		t.Fatal("hold must be bounded")
	}
	h = primaryHold{clearAt: t0}
	if !recoveryHold(h, t0.Add(5*time.Second), 20*time.Second, 10*time.Second, vote) || recoveryHold(h, t0.Add(11*time.Second), 20*time.Second, 10*time.Second, vote) {
		t.Fatal("post-recovery grace wrong")
	}
	if recoveryHold(primaryHold{}, t0, 20*time.Second, 10*time.Second, vote) {
		t.Fatal("healthy primary must not hold")
	}
}
