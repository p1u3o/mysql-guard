package consul

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

type fakeBackend struct {
	mu       sync.Mutex
	next     int
	sessions map[string]bool
	holder   map[string]string
}

func newFake() *fakeBackend {
	return &fakeBackend{sessions: map[string]bool{}, holder: map[string]string{}}
}

func (f *fakeBackend) CreateSession(string, time.Duration, time.Duration) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next++
	id := fmt.Sprintf("s%d", f.next)
	f.sessions[id] = true
	return id, nil
}

func (f *fakeBackend) RenewSession(id string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sessions[id], nil
}

func (f *fakeBackend) DestroySession(id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.sessions, id)
	for k, h := range f.holder {
		if h == id {
			delete(f.holder, k)
		}
	}
	return nil
}

func (f *fakeBackend) Acquire(key, s string, _ []byte) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.sessions[s] {
		return false, fmt.Errorf("invalid session")
	}
	if h, ok := f.holder[key]; ok && h != s {
		return false, nil
	}
	f.holder[key] = s
	return true, nil
}

func (f *fakeBackend) Release(key, s string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.holder[key] == s {
		delete(f.holder, key)
	}
	return nil
}

func (f *fakeBackend) Holder(key string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.holder[key], nil
}

func TestLeaderLockExactlyOneLeaderAndNonBlocking(t *testing.T) {
	f := newFake()
	a := newLeaderLock(f, "k", "db-a", 15*time.Second, 0)
	b := newLeaderLock(f, "k", "db-b", 15*time.Second, 0)

	done := make(chan struct{})
	go func() {
		for i := 0; i < 5; i++ {
			a.Tick()
			b.Tick()
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Tick blocked while lock held by another session")
	}
	if a.IsLeader() == b.IsLeader() {
		t.Fatalf("expected exactly one leader: a=%v b=%v", a.IsLeader(), b.IsLeader())
	}
	if !a.IsLeader() {
		t.Fatal("first acquirer should be leader")
	}
	if b.LastRenew().IsZero() {
		t.Fatal("follower should still record session renewals")
	}

	// Leader's session expires: follower takes over on its next tick.
	_ = f.DestroySession(a.SessionID())
	b.Tick()
	a.Tick()
	if !b.IsLeader() || a.IsLeader() {
		t.Fatalf("expected failover of lock to b: a=%v b=%v", a.IsLeader(), b.IsLeader())
	}
}

func TestLeaderLockReleaseOnStop(t *testing.T) {
	f := newFake()
	a := newLeaderLock(f, "k", "db-a", 3*time.Second, 0)
	a.Tick()
	if !a.IsLeader() {
		t.Fatal("expected leader")
	}
	a.release()
	if h, _ := f.Holder("k"); h != "" {
		t.Fatalf("lock not released: %q", h)
	}
}
