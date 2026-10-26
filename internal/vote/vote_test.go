package vote

import (
	"testing"
	"time"
)

func TestStreakSlowBeforeDead(t *testing.T) {
	now := time.Unix(0, 0)
	deadAfter := 30 * time.Second
	s := Streak{}
	for i := 0; i < 3; i++ {
		s.Update(now.Add(time.Duration(i)*time.Second), ProbeSlow, 4, deadAfter)
	}
	if s.Failed {
		t.Fatal("should not fail before slow threshold")
	}
	for i := 0; i < 4; i++ {
		s.Update(now.Add(time.Duration(10+i)*time.Second), ProbeSlow, 4, deadAfter)
	}
	if !s.Failed {
		t.Fatal("expected failed after slow threshold")
	}
	if s.Dead {
		t.Fatal("not dead until dead-after elapsed")
	}
	s.Update(now.Add(35*time.Second), ProbeSlow, 4, deadAfter)
	if !s.Dead {
		t.Fatal("expected dead")
	}
}

func TestRefusedCountsAsFailure(t *testing.T) {
	s := Streak{}
	now := time.Now()
	s.Update(now, ProbeRefused, 4, 30*time.Second)
	if !s.Failed {
		t.Fatal("refused should count as failed immediately")
	}
}
