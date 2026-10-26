package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestStartRetrySucceedsAfterErrors(t *testing.T) {
	n := 0
	start := func(context.Context) error {
		n++
		if n < 3 {
			return errors.New("i/o timeout")
		}
		return nil
	}
	off := func(context.Context) (string, string, string, error) { return "OFF", "OFF", "", nil }
	got, ver, err := startRetry(context.Background(), start, off, 5, time.Millisecond)
	if err != nil || got != 3 || ver {
		t.Fatalf("got %d %v", got, err)
	}
}

func TestStartRetryDroppedButActuallyStarted(t *testing.T) {
	calls := 0
	start := func(context.Context) error { calls++; return errors.New("invalid connection") }
	on := func(context.Context) (string, string, string, error) { return "CONNECTING", "ON", "", nil }
	got, ver, err := startRetry(context.Background(), start, on, 5, time.Millisecond)
	if err != nil || got != 1 || calls != 1 || !ver {
		t.Fatalf("want verified on attempt 1, got %d %v calls=%d", got, err, calls)
	}
}

func TestStartRetryGivesUp(t *testing.T) {
	start := func(context.Context) error { return errors.New("boom") }
	off := func(context.Context) (string, string, string, error) { return "OFF", "OFF", "", nil }
	got, _, err := startRetry(context.Background(), start, off, 3, time.Millisecond)
	if err == nil || got != 3 || !strings.Contains(err.Error(), "3 attempts") {
		t.Fatalf("got %d %v", got, err)
	}
}

func TestVerifySourceRecovered(t *testing.T) {
	n := 0
	read := func(context.Context) (string, int, string, error) {
		n++
		if n == 1 {
			return "", 0, "", errors.New("invalid connection")
		}
		return "10.0.0.1", 18401, "repl", nil
	}
	ok, err := verifySource(context.Background(), errors.New("i/o timeout"), read, "10.0.0.1", 18401, "repl", 3, time.Millisecond)
	if !ok || err != nil {
		t.Fatalf("got %v %v", ok, err)
	}
}

func TestVerifySourceMismatchIsError(t *testing.T) {
	read := func(context.Context) (string, int, string, error) { return "10.0.0.9", 18401, "guard", nil }
	ok, err := verifySource(context.Background(), errors.New("i/o timeout"), read, "10.0.0.1", 18401, "repl", 3, time.Millisecond)
	if ok || err == nil || !strings.Contains(err.Error(), "actual source 10.0.0.9") {
		t.Fatalf("got %v %v", ok, err)
	}
}

func TestVerifySourceUnreadable(t *testing.T) {
	read := func(context.Context) (string, int, string, error) { return "", 0, "", errors.New("down") }
	ok, err := verifySource(context.Background(), errors.New("x"), read, "h", 1, "u", 2, time.Millisecond)
	if ok || err == nil || !strings.Contains(err.Error(), "unreadable") {
		t.Fatalf("got %v %v", ok, err)
	}
}

func TestWithIOTimeout(t *testing.T) {
	d, err := WithIOTimeout("u:p@tcp(h:1)/?timeout=3s&readTimeout=3s&writeTimeout=3s", 30*time.Second)
	if err != nil || !strings.Contains(d, "readTimeout=30s") || !strings.Contains(d, "writeTimeout=30s") || !strings.Contains(d, "timeout=3s") {
		t.Fatalf("got %q %v", d, err)
	}
}
