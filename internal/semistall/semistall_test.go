package semistall

import (
	"strings"
	"testing"
	"time"
)

var set = Settings{Probes: 3, Reenable: 2, MaxLag: 1}

func dead() Input {
	return Input{PrimaryReachable: true, PrimaryWritable: true, SourceEnabled: true, SourceClients: 0, ReplicaReachable: false, WitnessProbe: "dead"}
}

func healthy() Input {
	return Input{PrimaryReachable: true, PrimaryWritable: true, OffByGuard: true, ReplicaReachable: true, ReplKnown: true,
		IOThread: true, SQLThread: true, ReplicaRO: true, ReplicaInstall: true, ReplicaSemiOn: true}
}

func TestDisableNeedsNConsecutiveConfirmedTicks(t *testing.T) {
	var tr Tracker
	now := time.Unix(1000, 0)
	for i := 1; i <= 2; i++ {
		d := tr.Step(now, dead(), set)
		if d.Action != None || d.Skip != "confirming" {
			t.Fatalf("tick %d: %+v", i, d)
		}
	}
	d := tr.Step(now, dead(), set)
	if d.Action != Disable {
		t.Fatalf("want disable on 3rd tick, got %+v", d)
	}
	if tr.DeadTicks() != 0 {
		t.Fatal("counter must reset after the decision")
	}
}

func TestDisableResetOnAnyContraryTick(t *testing.T) {
	now := time.Unix(1000, 0)
	cases := map[string]func(*Input){
		"leader reaches replica": func(in *Input) { in.ReplicaReachable = true },
		"witness says ok":        func(in *Input) { in.WitnessProbe = "ok" },
		"no witness check":       func(in *Input) { in.WitnessProbe = "" },
		"client connected":       func(in *Input) { in.SourceClients = 1 },
	}
	for name, mod := range cases {
		var tr Tracker
		tr.Step(now, dead(), set)
		tr.Step(now, dead(), set)
		in := dead()
		mod(&in)
		d := tr.Step(now, in, set)
		if d.Action != None || d.Skip != "not_confirmed" || tr.DeadTicks() != 0 {
			t.Errorf("%s: %+v dead=%d", name, d, tr.DeadTicks())
		}
		if d := tr.Step(now, dead(), set); d.Action != None {
			t.Errorf("%s: confirmation must restart, got %+v", name, d)
		}
	}
}

func TestNoDisableWhenNothingToStall(t *testing.T) {
	now := time.Unix(1000, 0)
	for name, mod := range map[string]func(*Input){
		"source off":       func(in *Input) { in.SourceEnabled = false },
		"primary down":     func(in *Input) { in.PrimaryReachable = false },
		"primary readonly": func(in *Input) { in.PrimaryWritable = false },
	} {
		var tr Tracker
		in := dead()
		mod(&in)
		for i := 0; i < 10; i++ {
			if d := tr.Step(now, in, set); d.Action != None {
				t.Fatalf("%s: acted: %+v", name, d)
			}
		}
	}
}

func TestBlockedResetsAndNeverActs(t *testing.T) {
	var tr Tracker
	now := time.Unix(1000, 0)
	tr.Step(now, dead(), set)
	tr.Step(now, dead(), set)
	in := dead()
	in.Blocked = "failover in progress"
	d := tr.Step(now, in, set)
	if d.Action != None || !strings.Contains(d.Reason, "failover in progress") || tr.DeadTicks() != 0 {
		t.Fatalf("%+v", d)
	}
	// a blocked tick also stops a pending re-enable
	h := healthy()
	tr.Step(now, h, set)
	h.Blocked = "maintenance"
	tr.Step(now, h, set)
	if tr.HealthyTicks() != 0 {
		t.Fatal("blocked tick must reset healthy counter")
	}
}

func TestReenableNeedsCaughtUpReplica(t *testing.T) {
	now := time.Unix(1000, 0)
	bad := map[string]func(*Input){
		"unreachable":    func(in *Input) { in.ReplicaReachable = false },
		"repl unknown":   func(in *Input) { in.ReplKnown = false },
		"writable":       func(in *Input) { in.ReplicaRO = false },
		"io off":         func(in *Input) { in.IOThread = false },
		"sql off":        func(in *Input) { in.SQLThread = false },
		"io lost":        func(in *Input) { in.IOLost = true },
		"stalled":        func(in *Input) { in.Stalled = true },
		"lag":            func(in *Input) { in.LagSeconds = 2 },
		"gtid gap":       func(in *Input) { in.GapToWriter = 5 },
		"no plugin":      func(in *Input) { in.ReplicaInstall = false },
		"replica sync 0": func(in *Input) { in.ReplicaSemiOn = false },
	}
	for name, mod := range bad {
		var tr Tracker
		in := healthy()
		mod(&in)
		for i := 0; i < 5; i++ {
			if d := tr.Step(now, in, set); d.Action != None || d.Skip != "replica_not_ready" {
				t.Fatalf("%s: %+v", name, d)
			}
		}
	}
	var tr Tracker
	if d := tr.Step(now, healthy(), set); d.Action != None || d.Skip != "confirming" {
		t.Fatalf("%+v", d)
	}
	if d := tr.Step(now, healthy(), set); d.Action != Reenable {
		t.Fatalf("want reenable, got %+v", d)
	}
}

func TestReenableCounterResetsOnRelapse(t *testing.T) {
	var tr Tracker
	now := time.Unix(1000, 0)
	tr.Step(now, healthy(), set)
	lag := healthy()
	lag.LagSeconds = 30
	tr.Step(now, lag, set)
	if d := tr.Step(now, healthy(), set); d.Action != None {
		t.Fatalf("relapse must restart the count: %+v", d)
	}
}

func TestOnlyGuardDisableIsUndone(t *testing.T) {
	var tr Tracker
	now := time.Unix(1000, 0)
	// source off, replica healthy, but the guard did not turn it off: nothing to do
	in := healthy()
	in.OffByGuard = false
	in.SourceEnabled = false
	for i := 0; i < 5; i++ {
		if d := tr.Step(now, in, set); d.Action != None {
			t.Fatalf("must not re-enable an operator's OFF: %+v", d)
		}
	}
}

func TestClearWhenSomeoneTurnedItOn(t *testing.T) {
	var tr Tracker
	in := healthy()
	in.SourceEnabled = true
	if d := tr.Step(time.Unix(1, 0), in, set); d.Action != Clear {
		t.Fatalf("%+v", d)
	}
}

func TestBackoffHoldsReenable(t *testing.T) {
	var tr Tracker
	now := time.Unix(1000, 0)
	tr.Backoff(now.Add(time.Minute))
	for i := 0; i < 5; i++ {
		if d := tr.Step(now, healthy(), set); d.Action != None || d.Skip != "backoff" {
			t.Fatalf("%+v", d)
		}
	}
	later := now.Add(2 * time.Minute)
	tr.Step(later, healthy(), set)
	if d := tr.Step(later, healthy(), set); d.Action != Reenable {
		t.Fatalf("%+v", d)
	}
}

func TestSettingsFloor(t *testing.T) {
	var tr Tracker
	if d := tr.Step(time.Unix(1, 0), dead(), Settings{}); d.Action != Disable {
		t.Fatalf("zero settings behave as 1 probe: %+v", d)
	}
}
