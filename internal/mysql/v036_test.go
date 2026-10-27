package mysql

import (
	"strings"
	"testing"
	"time"
)

func TestLostSource(t *testing.T) {
	st := 35 * time.Second
	cases := []struct {
		h    IOHealth
		lost bool
	}{
		{IOHealth{}, false},
		{IOHealth{Known: true, State: "CONNECTING"}, true},
		{IOHealth{Known: true, State: "OFF", LastErrNo: 2003}, true},
		{IOHealth{Known: true, State: "OFF"}, false},
		{IOHealth{Known: true, State: "ON", SilenceSec: 2}, false},
		{IOHealth{Known: true, State: "ON", SilenceSec: 40}, true},
	}
	for i, c := range cases {
		if got, _ := c.h.LostSource(st); got != c.lost {
			t.Errorf("%d: %+v got %v", i, c.h, got)
		}
	}
}

func TestParseSemiSync(t *testing.T) {
	s := parseSemiSync(map[string]string{"rpl_semi_sync_source_enabled": "ON", "rpl_semi_sync_source_status": "OFF", "rpl_semi_sync_source_clients": "0"})
	if !s.Installed || !s.FellBack() || s.Mode() != "async" {
		t.Fatalf("%+v", s)
	}
	s = parseSemiSync(map[string]string{"rpl_semi_sync_master_enabled": "1", "rpl_semi_sync_master_status": "ON", "rpl_semi_sync_master_clients": "1"})
	if s.Mode() != "sync" || s.SourceClients != 1 {
		t.Fatalf("legacy %+v", s)
	}
	if s := parseSemiSync(map[string]string{}); s.Installed || s.Mode() != "off" {
		t.Fatalf("none %+v", s)
	}
}

func TestChangeSourceSSL(t *testing.T) {
	if q := changeSourceSQL("h", 3306, "u", "p'w", true); !strings.HasSuffix(q, "SOURCE_SSL=1") || !strings.Contains(q, "p\\'w") && !strings.Contains(q, "p''w") {
		t.Fatal(q)
	}
	if q := changeSourceSQL("h", 3306, "u", "p", false); !strings.HasSuffix(q, "SOURCE_SSL=0") {
		t.Fatal(q)
	}
}
