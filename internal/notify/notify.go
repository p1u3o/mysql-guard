package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Sender delivers alerts (ntfy by default).
type Sender interface {
	Send(ctx context.Context, title, message string, priority int) error
}

// Ntfy posts to an ntfy topic URL (placeholder in repo).
type Ntfy struct {
	URL    string
	Client *http.Client
}

func NewNtfy(url string) *Ntfy {
	return &Ntfy{URL: url, Client: &http.Client{Timeout: 10 * time.Second}}
}

func (n *Ntfy) Send(ctx context.Context, title, message string, priority int) error {
	if n.URL == "" {
		return nil
	}
	body := bytes.NewBufferString(message)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, n.URL, body)
	if err != nil {
		return err
	}
	req.Header.Set("Title", title)
	if priority > 0 {
		req.Header.Set("Priority", fmt.Sprintf("%d", priority))
	}
	resp, err := n.Client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("ntfy status %d", resp.StatusCode)
	}
	return nil
}

// LogSender is a test/no-op backend.
type LogSender struct {
	LastTitle   string
	LastMessage string
}

func (l *LogSender) Send(_ context.Context, title, message string, _ int) error {
	l.LastTitle = title
	l.LastMessage = message
	return nil
}

// ConsulCheckUpdater updates TTL check status (interface for tests).
type ConsulCheckUpdater interface {
	SetCheckStatus(ctx context.Context, checkID, status, output string) error
}

// Fanout sends to ntfy and Consul checks.
type Fanout struct {
	Ntfy   Sender
	Consul ConsulCheckUpdater
	Prefix string
}

func (f *Fanout) Alert(ctx context.Context, code, message string, critical bool) error {
	pri := 3
	if critical {
		pri = 5
	}
	if f.Ntfy != nil {
		_ = f.Ntfy.Send(ctx, "mysql-guard: "+code, message, pri)
	}
	if f.Consul != nil {
		st := "warning"
		if critical {
			st = "critical"
		}
		_ = f.Consul.SetCheckStatus(ctx, f.Prefix+code, st, message)
	}
	return nil
}

// JSON for testing payloads.
func MarshalAlert(code, msg string) string {
	b, _ := json.Marshal(map[string]string{"code": code, "message": msg})
	return string(b)
}
