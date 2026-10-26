//go:build faultinject

package consul

import (
	"errors"
	"os"
)

// Drill-only: while /tmp/fault-setmaster exists, SetMaster fails before writing.
func init() {
	faultSetMaster = func() error {
		if _, err := os.Stat("/tmp/fault-setmaster"); err == nil {
			return errors.New("fault injected: SetMaster")
		}
		return nil
	}
}
