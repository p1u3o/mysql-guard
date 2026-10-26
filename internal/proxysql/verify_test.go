package proxysql

import "testing"

func TestWriterMismatch(t *testing.T) {
	if m := WriterMismatch([]string{"10.0.0.1:3306"}, "10.0.0.1:3306"); m != "" {
		t.Fatal(m)
	}
	for _, on := range [][]string{nil, {"10.0.0.2:3306"}, {"10.0.0.1:3306", "10.0.0.2:3306"}} {
		if WriterMismatch(on, "10.0.0.1:3306") == "" {
			t.Fatal("mismatch not reported", on)
		}
	}
	c := &Client{endpoints: map[string]string{"kv01": "10.0.0.1:18401"}}
	if c.ExpectedEndpoint("kv01") != "10.0.0.1:18401" || c.ExpectedEndpoint("kv02") != "" {
		t.Fatal("expected endpoint")
	}
}
