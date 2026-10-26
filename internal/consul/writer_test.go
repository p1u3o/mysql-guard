package consul

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

var eps = map[string]string{"kv01": "10.0.0.1:18401", "kv02": "10.0.0.2:18401"}

func TestEncodeDecodeMaster(t *testing.T) {
	v, err := EncodeMaster(FormatEpoch, eps, "kv01", 7)
	if err != nil || v != "kv01|7" {
		t.Fatal(v, err)
	}
	v, err = EncodeMaster(FormatLegacy, eps, "kv02", 7)
	if err != nil || v != "10.0.0.2:18401" {
		t.Fatal(v, err)
	}
	if _, err := EncodeMaster(FormatLegacy, eps, "nope", 1); err == nil {
		t.Fatal("expected error for unknown node")
	}
	n, e, legacy, err := DecodeMaster("kv01|9", eps)
	if n != "kv01" || e != 9 || legacy || err != nil {
		t.Fatal(n, e, legacy, err)
	}
	n, e, legacy, err = DecodeMaster("10.0.0.2:18401", eps)
	if n != "kv02" || e != 0 || !legacy || err != nil {
		t.Fatal(n, e, legacy, err)
	}
	if _, _, _, err = DecodeMaster("10.9.9.9:1", eps); err == nil {
		t.Fatal("unknown legacy endpoint must error, never guess")
	}
	n, _, _, _ = DecodeMaster("kv02", eps)
	if n != "kv02" {
		t.Fatal(n)
	}
}

// fakeConsul is a minimal KV + catalog server for store tests.
type fakeConsul struct {
	mu   sync.Mutex
	kv   map[string]string
	idx  map[string]uint64
	n    uint64
	svc  map[string]any
	regs int
}

func newFakeConsul() *fakeConsul {
	return &fakeConsul{kv: map[string]string{}, idx: map[string]uint64{}, svc: map[string]any{}}
}

func (f *fakeConsul) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch {
	case strings.HasPrefix(r.URL.Path, "/v1/kv/"):
		k := strings.TrimPrefix(r.URL.Path, "/v1/kv/")
		switch r.Method {
		case http.MethodGet:
			v, ok := f.kv[k]
			if !ok {
				w.WriteHeader(404)
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{{"Key": k, "Value": base64.StdEncoding.EncodeToString([]byte(v)), "ModifyIndex": f.idx[k]}})
		case http.MethodPut:
			buf := new(strings.Builder)
			b := make([]byte, 4096)
			nr, _ := r.Body.Read(b)
			buf.Write(b[:nr])
			if cas := r.URL.Query().Get("cas"); cas != "" {
				want := cas
				have := "0"
				if i, ok := f.idx[k]; ok {
					have = jsonNum(i)
				}
				if want != have {
					_, _ = w.Write([]byte("false"))
					return
				}
			}
			f.n++
			f.kv[k], f.idx[k] = buf.String(), f.n
			_, _ = w.Write([]byte("true"))
		}
	case r.URL.Path == "/v1/catalog/register":
		var reg map[string]any
		_ = json.NewDecoder(r.Body).Decode(&reg)
		f.svc[reg["Node"].(string)] = reg
		f.regs++
		_, _ = w.Write([]byte("true"))
	case strings.HasPrefix(r.URL.Path, "/v1/catalog/service/"):
		out := []map[string]any{}
		for node, raw := range f.svc {
			reg := raw.(map[string]any)
			s := reg["Service"].(map[string]any)
			out = append(out, map[string]any{"Node": node, "Address": reg["Address"], "ServiceAddress": s["Address"], "ServicePort": s["Port"], "ServiceName": s["Service"]})
		}
		_ = json.NewEncoder(w).Encode(out)
	default:
		w.WriteHeader(404)
	}
}

func jsonNum(i uint64) string { b, _ := json.Marshal(i); return string(b) }

func testStore(t *testing.T, f *fakeConsul, o Options) *Store {
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	s, err := NewStore(strings.TrimPrefix(srv.URL, "http://"), "", "tc")
	if err != nil {
		t.Fatal(err)
	}
	s.SetOptions(o)
	return s
}

func TestLegacySetMasterAndWriterService(t *testing.T) {
	f := newFakeConsul()
	s := testStore(t, f, Options{Format: FormatLegacy, Endpoints: eps, Writer: WriterService{Name: "tc-writer", Tags: []string{"mysql", "writer"}}})
	ctx := context.Background()
	f.kv["mysql/tc/writer/epoch"], f.idx["mysql/tc/writer/epoch"] = "4", 1
	if err := s.SetMaster(ctx, "kv02", 4); err != nil {
		t.Fatal(err)
	}
	if f.kv["mysql/master/tc"] != "10.0.0.2:18401" || f.kv["mysql/tc/writer/current"] != "kv02|4" {
		t.Fatal(f.kv)
	}
	n, e, err := s.GetPrimary(ctx)
	if n != "kv02" || e != 4 || err != nil {
		t.Fatal(n, e, err)
	}
	if tgt, _ := s.WriterServiceTarget(ctx); tgt != "10.0.0.2:18401" {
		t.Fatal(tgt)
	}
	// idempotent: no second registration when already correct
	if wrote, err := s.SyncWriterService(ctx, "kv02", 4, "x"); wrote || err != nil {
		t.Fatal(wrote, err)
	}
	if foreign, ok, _ := s.ForeignWriter(ctx); ok {
		t.Fatal("guard write flagged foreign", foreign)
	}
	// repman/bash post-script writes the other node without an epoch bump
	f.kv["mysql/master/tc"] = "10.0.0.1:18401"
	if _, ok, _ := s.ForeignWriter(ctx); !ok {
		t.Fatal("foreign write not detected")
	}
}

func TestMigrateMaster(t *testing.T) {
	f := newFakeConsul()
	s := testStore(t, f, Options{Format: FormatEpoch, Endpoints: eps})
	ctx := context.Background()
	f.kv["mysql/master/tc"], f.idx["mysql/master/tc"] = "10.0.0.1:18401", 3
	// legacy cluster without epoch key: migration creates epoch 1
	old, nv, err := s.MigrateMaster(ctx, FormatEpoch)
	if err != nil || old != "10.0.0.1:18401" || nv != "kv01|1" || f.kv["mysql/tc/writer/epoch"] != "1" {
		t.Fatal(old, nv, err, f.kv)
	}
	old, nv, err = s.MigrateMaster(ctx, FormatLegacy)
	if err != nil || old != "kv01|1" || nv != "10.0.0.1:18401" || f.kv["mysql/tc/writer/current"] != "kv01|1" {
		t.Fatal(old, nv, err, f.kv)
	}
	// unknown legacy value is refused
	f.kv["mysql/master/tc"] = "10.9.9.9:1"
	if _, _, err := s.MigrateMaster(ctx, FormatEpoch); err == nil {
		t.Fatal("expected refusal")
	}
}
