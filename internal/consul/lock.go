package consul

import (
	"fmt"
	"sync"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// LeaderLock acquires mysql/guard/leader with session renewal in a dedicated goroutine.
type LeaderLock struct {
	store      *Store
	sessionTTL time.Duration
	lockDelay  time.Duration
	node       string

	mu         sync.RWMutex
	leader     bool
	sessionID  string
	lastRenew  time.Time
	stopCh     chan struct{}
	lock       *capi.Lock
	leaderLost <-chan struct{}
}

func NewLeaderLock(store *Store, node string, ttl, delay time.Duration) *LeaderLock {
	return &LeaderLock{
		store:      store,
		sessionTTL: ttl,
		lockDelay:  delay,
		node:       node,
		stopCh:     make(chan struct{}),
	}
}

func (l *LeaderLock) IsLeader() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leader
}

func (l *LeaderLock) LastRenew() time.Time {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastRenew
}

func (l *LeaderLock) SessionID() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.sessionID
}

// RunRenewalLoop must not block on MySQL probes.
func (l *LeaderLock) RunRenewalLoop() {
	renewEvery := l.sessionTTL / 3
	if renewEvery < time.Second {
		renewEvery = time.Second
	}
	ticker := time.NewTicker(renewEvery)
	defer ticker.Stop()

	for {
		select {
		case <-l.stopCh:
			l.release()
			return
		case <-ticker.C:
			l.tick()
		}
	}
}

func (l *LeaderLock) Stop() { close(l.stopCh) }

func (l *LeaderLock) tick() {
	if l.leaderLost != nil {
		select {
		case <-l.leaderLost:
			l.setLeader(false, "")
			l.leaderLost = nil
		default:
		}
	}
	if l.lock == nil {
		lock, err := l.store.Client().LockOpts(&capi.LockOptions{
			Key:         l.store.Keys().LeaderLock(),
			SessionName: fmt.Sprintf("mysql-guard-%s", l.node),
			SessionTTL:  l.sessionTTL.String(),
			LockDelay:   l.lockDelay,
		})
		if err != nil {
			l.setLeader(false, "")
			return
		}
		l.lock = lock
	}
	ch, err := l.lock.Lock(l.stopCh)
	if err != nil || ch == nil {
		l.setLeader(false, "")
		return
	}
	l.leaderLost = ch
	l.setLeader(true, l.lockOptsSession())
}

func (l *LeaderLock) lockOptsSession() string {
	if l.lock == nil {
		return ""
	}
	// Session id is tracked internally on Lock after acquisition.
	return l.sessionID
}

func (l *LeaderLock) release() {
	if l.lock != nil && l.leader {
		_ = l.lock.Unlock()
	}
}

func (l *LeaderLock) setLeader(v bool, sess string) {
	l.mu.Lock()
	l.leader = v
	if sess != "" {
		l.sessionID = sess
	}
	l.lastRenew = time.Now()
	l.mu.Unlock()
}

// VerifyLeaderBeforeAction re-checks leadership freshness before mutating state.
func (l *LeaderLock) VerifyLeaderBeforeAction(maxStale time.Duration) bool {
	if !l.IsLeader() {
		return false
	}
	if time.Since(l.LastRenew()) > maxStale {
		return false
	}
	return true
}
