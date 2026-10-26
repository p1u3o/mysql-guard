package consul

import "testing"

func TestParseListenPort(t *testing.T) {
	if parseListenPort(":9470") != 9470 {
		t.Fatal()
	}
	if parseListenPort("127.0.0.1:8081") != 8081 {
		t.Fatal()
	}
}
