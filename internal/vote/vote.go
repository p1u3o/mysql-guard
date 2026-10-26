package vote

import "time"

// Kind is one of the three dead-detection votes.
type Kind int

const (
	V1DirectProbe Kind = 1
	V2SerfHealth  Kind = 2
	V3ReplicaLink Kind = 3
)

// ProbeResult classifies a single probe attempt.
type ProbeResult int

const (
	ProbeOK      ProbeResult = 0
	ProbeSlow    ProbeResult = 1
	ProbeFailed  ProbeResult = 2
	ProbeRefused ProbeResult = 3 // connection refused/reset counts as failure immediately
)

// Streak tracks slow vs failed probes for one vote channel.
type Streak struct {
	FirstFailAt time.Time
	SlowCount   int
	Failed      bool
	Dead        bool
}

// Update applies a probe result with thresholds.
func (s *Streak) Update(now time.Time, res ProbeResult, slowThreshold int, deadAfter time.Duration) {
	switch res {
	case ProbeOK:
		s.FirstFailAt = time.Time{}
		s.SlowCount = 0
		s.Failed = false
		s.Dead = false
	case ProbeSlow:
		s.SlowCount++
		if s.FirstFailAt.IsZero() {
			s.FirstFailAt = now
		}
		if s.SlowCount >= slowThreshold {
			s.Failed = true
		}
	case ProbeFailed, ProbeRefused:
		s.SlowCount = slowThreshold
		s.Failed = true
		if s.FirstFailAt.IsZero() {
			s.FirstFailAt = now
		}
	}
	if s.Failed && !s.FirstFailAt.IsZero() && now.Sub(s.FirstFailAt) >= deadAfter {
		s.Dead = true
	}
}

// Ballot holds the three votes for the primary.
type Ballot struct {
	V1 Streak
	V2 Streak
	V3 Streak
}

func (b *Ballot) DeadCount() int {
	n := 0
	if b.V1.Dead {
		n++
	}
	if b.V2.Dead {
		n++
	}
	if b.V3.Dead {
		n++
	}
	return n
}

func (b *Ballot) UpdateAll(now time.Time, r1, r2, r3 ProbeResult, slowThreshold int, deadAfter time.Duration) {
	b.V1.Update(now, r1, slowThreshold, deadAfter)
	b.V2.Update(now, r2, slowThreshold, deadAfter)
	b.V3.Update(now, r3, slowThreshold, deadAfter)
}

// PrimaryDead returns true when enough votes are dead.
func PrimaryDead(b Ballot, needed int) bool {
	return b.DeadCount() >= needed
}
