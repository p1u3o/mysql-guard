package actionlog

import (
	"encoding/json"
	"time"
)

// Entry is one row in the cluster action log.
type Entry struct {
	Timestamp time.Time `json:"timestamp"`
	Epoch     uint64    `json:"epoch"`
	Guard     string    `json:"guard"`
	Action    string    `json:"action"`
	Reason    string    `json:"reason"`
	Result    string    `json:"result"`
	Step      string    `json:"step,omitempty"`
}

// Append caps the list to max entries (newest last).
func Append(existing []Entry, e Entry, max int) []Entry {
	if max <= 0 {
		max = 500
	}
	out := append(existing, e)
	if len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// Decode parses JSON action log bytes.
func Decode(b []byte) ([]Entry, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var list []Entry
	if err := json.Unmarshal(b, &list); err != nil {
		return nil, err
	}
	return list, nil
}

// Encode serializes the log.
func Encode(list []Entry) ([]byte, error) {
	return json.Marshal(list)
}
