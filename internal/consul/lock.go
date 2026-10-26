package consul

import (
	"fmt"
	"sync"
	"time"

	capi "github.com/hashicorp/consul/api"
)

// lockBackend is the subset of Consul session/KV used by LeaderLock.
// Every call is non-blocking so the renewal loop never stalls while
// another session holds the lock.
type lockBackend interface {
	CreateSession(name string, ttl, delay time.Duration) (string, error)
	// RenewSession returns false when the session no longer exists.
	RenewSession(id string) (bool, error)
	DestroySession(id string) error
	// Acquire attempts KV acquire; returns false if held by another session.
	Acquire(key, session string, value []byte) (bool, error)
	Release(key, session string) error
	// Holder returns the session currently holding key ("" if none).
	Holder(key string) (string, error)
}

type consulBackend struct{ c *capi.Client }

func (b consulBackend) CreateSession(name string, ttl, delay time.Duration) (string, error) {
	id, _, err := b.c.Session().Create(&capi.SessionEntry{
		Name:      name,
		TTL:       ttl.String(),
		LockDelay: delay,
		Behavior:  capi.SessionBehaviorRelease,
	}, nil)
	return id, err
}

func (b consulBackend) RenewSession(id string) (bool, error) {
	e, _, err := b.c.Session().Renew(id, nil)
	if err != nil {
		return false, err
	}
	return e != nil, nil
}

func (b consulBackend) DestroySession(id string) error {
	_, err := b.c.Session().Destroy(id, nil)
	return err
}

func (b consulBackend) Acquire(key, session string, value []byte) (bool, error) {
	ok, _, err := b.c.KV().Acquire(&capi.KVPair{Key: key, Value: value, Session: session}, nil)
	return ok, err
}

func (b consulBackend) Release(key, session string) error {
	_, _, err := b.c.KV().Release(&capi.KVPair{Key: key, Session: session}, nil)
	return err
}

func (b consulBackend) Holder(key string) (string, error) {
	p, _, err := b.c.KV().Get(key, nil)
	if err != nil || p == nil {
		return "", err
	}
	return p.Session, nil
}

// LeaderLock holds mysql/<cluster>/guard/leader via a Consul session.
// Each tick renews (or recreates) the session and attempts a non-blocking
// acquire, so exactly one guard is leader and followers keep retrying.
type LeaderLock struct {
	backend    lockBackend
	key        string
	sessionTTL time.Duration
	lockDelay  time.Duration
	node       string

	mu        sync.RWMutex
	leader    bool
	sessionID string
	lastRenew time.Time
	stopCh    chan struct{}
	stopOnce  sync.Once
}

func NewLeaderLock(store *Store, node string, ttl, delay time.Duration) *LeaderLock {
	return newLeaderLock(consulBackend{c: store.Client()}, store.Keys().LeaderLock(), node, ttl, delay)
}

func newLeaderLock(b lockBackend, key, node string, ttl, delay time.Duration) *LeaderLock {
	return &LeaderLock{backend: b, key: key, sessionTTL: ttl, lockDelay: delay, node: node, stopCh: make(chan struct{})}
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

func (l *LeaderLock) renewEvery() time.Duration {
	d := l.sessionTTL / 3
	if d < time.Second {
		d = time.Second
	}
	return d
}

// RunRenewalLoop must not block on MySQL probes or on the lock itself.
func (l *LeaderLock) RunRenewalLoop() {
	l.Tick()
	ticker := time.NewTicker(l.renewEvery())
	defer ticker.Stop()
	for {
		select {
		case <-l.stopCh:
			l.release()
			return
		case <-ticker.C:
			l.Tick()
		}
	}
}

func (l *LeaderLock) Stop() { l.stopOnce.Do(func() { close(l.stopCh) }) }

// Tick renews the session and attempts a non-blocking acquire. Exported for tests.
func (l *LeaderLock) Tick() {
	l.mu.RLock()
	sess := l.sessionID
	l.mu.RUnlock()

	if sess != "" {
		alive, err := l.backend.RenewSession(sess)
		if err != nil {
			// Consul unreachable: we cannot prove we still hold the lock.
			l.set(false, sess, false)
			return
		}
		if !alive {
			sess = ""
		}
	}
	if sess == "" {
		id, err := l.backend.CreateSession(fmt.Sprintf("mysql-guard-%s", l.node), l.sessionTTL, l.lockDelay)
		if err != nil {
			l.set(false, "", false)
			return
		}
		sess = id
	}
	held, err := l.backend.Acquire(l.key, sess, []byte(l.node))
	if err != nil {
		l.set(false, sess, true)
		return
	}
	if !held {
		// Re-acquire by the current holder returns true; anything else means a follower.
		if h, herr := l.backend.Holder(l.key); herr == nil && h == sess {
			held = true
		}
	}
	l.set(held, sess, true)
}

func (l *LeaderLock) release() {
	l.mu.Lock()
	sess, leader := l.sessionID, l.leader
	l.leader = false
	l.mu.Unlock()
	if sess == "" {
		return
	}
	if leader {
		_ = l.backend.Release(l.key, sess)
	}
	_ = l.backend.DestroySession(sess)
}

func (l *LeaderLock) set(leader bool, sess string, renewed bool) {
	l.mu.Lock()
	l.leader = leader
	l.sessionID = sess
	if renewed {
		l.lastRenew = time.Now()
	}
	l.mu.Unlock()
}

// MarkAlive records liveness for guards that never hold a session (witness).
// It does not grant leadership.
func (l *LeaderLock) MarkAlive(t time.Time) {
	l.mu.Lock()
	l.lastRenew = t
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
