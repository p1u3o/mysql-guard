package notify

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestNtfyHeadersAndToken(t *testing.T) {
	var title, auth, prio string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		title, auth, prio = r.Header.Get("Title"), r.Header.Get("Authorization"), r.Header.Get("Priority")
	}))
	defer srv.Close()
	n := NewNtfy(srv.URL + "/topic")
	n.Token = "tk"
	if err := n.Send(context.Background(), "t1", "m", 5); err != nil {
		t.Fatal(err)
	}
	if title != "t1" || auth != "Bearer tk" || prio != "5" {
		t.Fatal(title, auth, prio)
	}
}
