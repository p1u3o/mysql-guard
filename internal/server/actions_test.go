package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"mysql-guard/internal/actionlog"
	"mysql-guard/internal/consul"
)

func fakeConsul(t *testing.T, value []byte) *consul.Store {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if value == nil || !strings.HasPrefix(r.URL.Path, "/v1/kv/") {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Consul-Index", "1")
		fmt.Fprintf(w, `[{"Key":%q,"Value":%q,"ModifyIndex":1}]`, strings.TrimPrefix(r.URL.Path, "/v1/kv/"), base64.StdEncoding.EncodeToString(value))
	}))
	t.Cleanup(srv.Close)
	st, err := consul.NewStore(strings.TrimPrefix(srv.URL, "http://"), "", "tc")
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func getActions(t *testing.T, st *consul.Store) string {
	t.Helper()
	rec := httptest.NewRecorder()
	New(nil, st, nil).handleActions(rec, httptest.NewRequest(http.MethodGet, "/api/actions", nil))
	if rec.Code != 200 {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	return strings.TrimSpace(rec.Body.String())
}

func TestActionsEmptyLogIsEmptyArray(t *testing.T) {
	if got := getActions(t, fakeConsul(t, nil)); got != "[]" {
		t.Fatalf("empty action log: got %s, want []", got)
	}
}

func TestActionsReturnsArray(t *testing.T) {
	raw, _ := json.Marshal([]actionlog.Entry{{Action: "seed_primary", Result: "ok"}})
	body := getActions(t, fakeConsul(t, raw))
	var list []actionlog.Entry
	if err := json.Unmarshal([]byte(body), &list); err != nil {
		t.Fatalf("not a JSON array: %s (%v)", body, err)
	}
	if len(list) != 1 || list[0].Action != "seed_primary" {
		t.Fatalf("got %+v", list)
	}
}

func TestActionsBodyNeverNil(t *testing.T) {
	if b, _ := json.Marshal(actionsBody(nil)); string(b) != "[]" {
		t.Fatalf("got %s", b)
	}
}
