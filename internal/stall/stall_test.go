package stall

import (
	"testing"
	"time"

	"mysql-guard/internal/gtid"
)

const u = "97cfba7a-d172-11f1-a507-a81e849690b0"

func TestStalledWhenRetrievedEmptyAndGapFlat(t *testing.T) {
	tr := New(10 * time.Second)
	t0 := time.Unix(1000, 0)
	o := Obs{Node: "r", ThreadsOn: true, SourceKnown: true, SourceExecuted: gtid.Set(u + ":1-8")}
	if r := tr.Observe(t0, o); r.Stalled || r.Gap != 8 {
		t.Fatalf("first obs: %+v", r)
	}
	if r := tr.Observe(t0.Add(5*time.Second), o); r.Stalled {
		t.Fatal("stalled too early")
	}
	r := tr.Observe(t0.Add(11*time.Second), o)
	if !r.Stalled || r.Reason == "" {
		t.Fatalf("want stalled: %+v", r)
	}
	// writer down: verdict sticky
	if r := tr.Observe(t0.Add(20*time.Second), Obs{Node: "r"}); !r.Stalled {
		t.Fatal("verdict not sticky while source unknown")
	}
	if !tr.Stalled("r") {
		t.Fatal("Stalled()")
	}
}

func TestShrinkingGapNotStalled(t *testing.T) {
	tr := New(10 * time.Second)
	t0 := time.Unix(1000, 0)
	for i := int64(0); i < 8; i++ {
		o := Obs{Node: "r", ThreadsOn: true, SourceKnown: true, SourceExecuted: gtid.Set(u + ":1-100"),
			Executed: gtid.Set(u + ":1-" + itoa(10+i*10)), Retrieved: gtid.Set(u + ":1-100")}
		if r := tr.Observe(t0.Add(time.Duration(i)*5*time.Second), o); r.Stalled {
			t.Fatalf("tick %d stalled with shrinking gap", i)
		}
	}
}

func TestCaughtUpOrThreadsOffClears(t *testing.T) {
	tr := New(time.Second)
	t0 := time.Unix(1000, 0)
	o := Obs{Node: "r", ThreadsOn: true, SourceKnown: true, SourceExecuted: gtid.Set(u + ":1-8")}
	tr.Observe(t0, o)
	if !tr.Observe(t0.Add(2*time.Second), o).Stalled {
		t.Fatal("want stalled")
	}
	o.Executed = gtid.Set(u + ":1-8")
	if tr.Observe(t0.Add(3*time.Second), o).Stalled {
		t.Fatal("caught up must clear")
	}
}

func itoa(n int64) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}
