package errant

import (
	"fmt"

	"mysql-guard/internal/gtid"
)

// Result of comparing a node's executed set to an authoritative source.
type Result struct {
	Errant    bool
	ErrantSet gtid.Set
	Reason    string
}

// Detect returns errant transactions on local that are not on source (gtid_subtract(local, source)).
func Detect(local, source gtid.Set) Result {
	diff := gtid.Subtract(local, source)
	if diff.Empty() {
		return Result{Reason: "no errant transactions"}
	}
	return Result{
		Errant:    true,
		ErrantSet: diff,
		Reason:    fmt.Sprintf("errant gtid: %s", diff),
	}
}

// BlocksPromotion is true when promotion or rejoin must not proceed.
func BlocksPromotion(local, primaryExecuted gtid.Set) Result {
	return Detect(local, primaryExecuted)
}
