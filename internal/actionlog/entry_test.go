package actionlog

import (
	"testing"
	"time"
)

func TestAppendCap(t *testing.T) {
	var list []Entry
	for i := 0; i < 10; i++ {
		list = Append(list, Entry{Timestamp: time.Now(), Action: "a"}, 5)
	}
	if len(list) != 5 {
		t.Fatalf("len=%d", len(list))
	}
}
