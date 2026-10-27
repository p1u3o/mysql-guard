// Package stall detects replicas whose threads report ON but which make no
// progress towards their source (for example a duplicate server_id makes the
// IO thread silently skip every event, leaving Retrieved_Gtid_Set empty).
package stall

import (
	"fmt"
	"sync"
	"time"

	"mysql-guard/internal/gtid"
)

// Obs is one observation of a replica against the current writer.
type Obs struct {
	Node           string
	ThreadsOn      bool
	SourceKnown    bool // writer's executed set was read this tick
	SourceExecuted gtid.Set
	Executed       gtid.Set
	Retrieved      gtid.Set
}

// Result for one replica.
type Result struct {
	Stalled bool
	Gap     int64
	Reason  string
}

type entry struct {
	gap     int64
	exec    int64 // executed transaction count: growth means the applier progresses
	since   time.Time
	stalled bool
	reason  string
}

// Tracker remembers the gap per node across ticks.
type Tracker struct {
	After time.Duration
	mu    sync.Mutex
	m     map[string]*entry
}

func New(after time.Duration) *Tracker { return &Tracker{After: after, m: map[string]*entry{}} }

// Observe updates the node's state. A replica is stalled when its threads are ON,
// its GTID gap to the writer is non-zero and neither the gap has shrunk nor the
// replica's executed set has grown for After. When the
// writer cannot be read the previous verdict is kept (sticky), so a stalled node
// is still excluded while the writer is down.
func (t *Tracker) Observe(now time.Time, o Obs) Result {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.m[o.Node]
	if !o.SourceKnown {
		if e == nil {
			return Result{}
		}
		return Result{Stalled: e.stalled, Gap: e.gap, Reason: e.reason}
	}
	gap := gtid.Subtract(o.SourceExecuted, o.Executed).Count()
	if !o.ThreadsOn || gap == 0 {
		delete(t.m, o.Node)
		return Result{Gap: gap}
	}
	exec := o.Executed.Count()
	if e == nil {
		e = &entry{gap: gap, exec: exec, since: now}
		t.m[o.Node] = e
	} else if gap < e.gap || exec > e.exec {
		// v0.3.6: a replica that keeps applying under steady writes (or a delayed
		// replica) holds a constant gap but is not stalled.
		e.gap, e.exec, e.since, e.stalled = gap, exec, now, false
	}
	if now.Sub(e.since) >= t.After {
		e.stalled = true
		if o.Retrieved.Empty() {
			e.reason = fmt.Sprintf("threads ON, Retrieved_Gtid_Set empty, gap %d not shrinking for %s", gap, now.Sub(e.since).Round(time.Second))
		} else {
			e.reason = fmt.Sprintf("threads ON, gap %d not shrinking for %s", gap, now.Sub(e.since).Round(time.Second))
		}
	}
	return Result{Stalled: e.stalled, Gap: gap, Reason: e.reason}
}

// Stalled reports the last verdict for node.
func (t *Tracker) Stalled(node string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	e := t.m[node]
	return e != nil && e.stalled
}
