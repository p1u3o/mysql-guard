package actions

import (
	"testing"

	"mysql-guard/internal/consul"
	"mysql-guard/internal/mysql"
)

func TestSemiWanted(t *testing.T) {
	on := mysql.SemiSync{Known: true, SourceEnabled: true}
	off := mysql.SemiSync{Known: true}
	cases := []struct {
		name      string
		src, join mysql.SemiSync
		rec       *consul.SemiSyncRecord
		want      bool
	}{
		{"async cluster", off, off, nil, false},
		{"source enabled", on, off, nil, true},
		{"switchback: old primary had source on", off, on, nil, true},
		{"switchback after rejoin: record enabled", off, off, &consul.SemiSyncRecord{Enabled: true, Mode: "sync"}, true},
		{"pending after failover", off, off, &consul.SemiSyncRecord{Pending: true}, true},
		{"record says disabled", off, off, &consul.SemiSyncRecord{}, false},
	}
	for _, c := range cases {
		if got := SemiWanted(c.src, c.join, c.rec); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}
