package consul

import (
	"testing"
)

func TestMaintenanceKeyPath(t *testing.T) {
	k := KeyPaths{Cluster: "example-cluster"}
	if k.Maintenance() != "mysql/example-cluster/guard/maintenance" {
		t.Fatal(k.Maintenance())
	}
}
