package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestWaitThreadsOK(t *testing.T) {
	n := 0
	r := func(context.Context) (string, string, string, error) {
		n++
		if n < 3 {
			return "CONNECTING", "ON", "", nil
		}
		return "ON", "ON", "", nil
	}
	if err := waitThreads(context.Background(), r, time.Second, time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestWaitThreadsTimeoutCarriesIOError(t *testing.T) {
	r := func(context.Context) (string, string, string, error) {
		return "CONNECTING", "ON", "Access denied for user 'guard'", nil
	}
	err := waitThreads(context.Background(), r, 20*time.Millisecond, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "Access denied") || !strings.Contains(err.Error(), "io=CONNECTING") {
		t.Fatalf("want timeout with io error, got %v", err)
	}
}

func TestWaitThreadsQueryError(t *testing.T) {
	r := func(context.Context) (string, string, string, error) { return "", "", "", errors.New("boom") }
	if err := waitThreads(context.Background(), r, 5*time.Millisecond, time.Millisecond); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("got %v", err)
	}
}
