package consul

import (
	"testing"
	"time"
)

func TestMarkAliveSetsRenewWithoutLeadership(t *testing.T) {
	l := newLeaderLock(newFake(), "k", "witness", 10*time.Second, time.Second)
	if !l.LastRenew().IsZero() {
		t.Fatal("expected zero renew time before MarkAlive")
	}
	now := time.Now()
	l.MarkAlive(now)
	if !l.LastRenew().Equal(now) {
		t.Fatal("MarkAlive must set renew time")
	}
	if l.IsLeader() || l.VerifyLeaderBeforeAction(time.Minute) {
		t.Fatal("MarkAlive must never grant leadership")
	}
}
