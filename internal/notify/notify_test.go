package notify

import (
	"context"
	"testing"
)

type fakeConsul struct{ status string }

func (f *fakeConsul) SetCheckStatus(_ context.Context, _ string, status, _ string) error {
	f.status = status
	return nil
}

func TestFanout(t *testing.T) {
	log := &LogSender{}
	c := &fakeConsul{}
	f := Fanout{Ntfy: log, Consul: c, Prefix: "guard-"}
	if err := f.Alert(context.Background(), "repl_lag", "lag high", true); err != nil {
		t.Fatal(err)
	}
	if log.LastTitle == "" || c.status != "critical" {
		t.Fatalf("ntfy=%q consul=%q", log.LastTitle, c.status)
	}
}
