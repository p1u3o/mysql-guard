package metrics

import (
	"regexp"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricNamesValidAndPrefixed(t *testing.T) {
	reg := prometheus.NewRegistry()
	r := NewWith(reg) // panics on invalid metric names
	r.DeadVotes.WithLabelValues("total").Set(1)
	r.ProbeSeconds.Observe(0.01)
	fams, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	valid := regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)
	if len(fams) < 10 {
		t.Fatalf("expected metrics gathered, got %d", len(fams))
	}
	for _, f := range fams {
		n := f.GetName()
		if !valid.MatchString(n) {
			t.Errorf("invalid metric name %q", n)
		}
		if !strings.HasPrefix(n, "mysql_guard_") {
			t.Errorf("metric %q lacks mysql_guard_ prefix", n)
		}
	}
}
