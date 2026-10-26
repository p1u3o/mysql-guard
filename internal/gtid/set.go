package gtid

import (
	"sort"
	"strings"
)

// Set is a simplified GTID executed set (uuid:1-5,uuid2:1-3).
type Set string

func Parse(s string) Set {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", "")
	return Set(s)
}

// Subtract returns transactions in a that are not in b (gtid_subtract semantics, simplified).
// Empty result means b has applied everything from a.
func Subtract(a, b Set) Set {
	if a == "" || a == b {
		return ""
	}
	if b == "" {
		return a
	}
	am := parseMap(string(a))
	bm := parseMap(string(b))
	var parts []string
	for uuid, ar := range am {
		br := bm[uuid]
		missing := intervalSubtract(ar, br)
		if len(missing) > 0 {
			parts = append(parts, uuid+":"+formatIntervals(missing))
		}
	}
	sort.Strings(parts)
	return Set(strings.Join(parts, ","))
}

func (s Set) Empty() bool { return strings.TrimSpace(string(s)) == "" }

type interval struct {
	start, end int64
}

func parseMap(set string) map[string][]interval {
	out := map[string][]interval{}
	for _, part := range strings.Split(set, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		uuid, seq, ok := strings.Cut(part, ":")
		if !ok {
			continue
		}
		// a UUID may appear more than once (e.g. a concatenated union): merge, never overwrite
		out[uuid] = mergeIntervals(append(out[uuid], parseIntervals(seq)...))
	}
	return out
}

func parseIntervals(seq string) []interval {
	var ivs []interval
	for _, chunk := range strings.Split(seq, ":") {
		chunk = strings.TrimSpace(chunk)
		if chunk == "" {
			continue
		}
		if strings.Contains(chunk, "-") {
			lo, hi, _ := strings.Cut(chunk, "-")
			ivs = append(ivs, interval{parseInt(lo), parseInt(hi)})
		} else {
			n := parseInt(chunk)
			ivs = append(ivs, interval{n, n})
		}
	}
	return mergeIntervals(ivs)
}

func parseInt(s string) int64 {
	s = strings.TrimSpace(s)
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			break
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

func mergeIntervals(ivs []interval) []interval {
	if len(ivs) == 0 {
		return nil
	}
	sort.Slice(ivs, func(i, j int) bool { return ivs[i].start < ivs[j].start })
	var out []interval
	cur := ivs[0]
	for _, iv := range ivs[1:] {
		if iv.start <= cur.end+1 {
			if iv.end > cur.end {
				cur.end = iv.end
			}
			continue
		}
		out = append(out, cur)
		cur = iv
	}
	out = append(out, cur)
	return out
}

func intervalSubtract(a, b []interval) []interval {
	if len(b) == 0 {
		return a
	}
	var out []interval
	for _, iv := range a {
		out = append(out, subtractOne(iv, b)...)
	}
	return mergeIntervals(out)
}

func subtractOne(iv interval, b []interval) []interval {
	cur := []interval{iv}
	for _, bv := range b {
		var next []interval
		for _, c := range cur {
			next = append(next, splitInterval(c, bv)...)
		}
		cur = next
	}
	return cur
}

func splitInterval(iv, cut interval) []interval {
	if cut.end < iv.start || cut.start > iv.end {
		return []interval{iv}
	}
	var out []interval
	if cut.start > iv.start {
		out = append(out, interval{iv.start, cut.start - 1})
	}
	if cut.end < iv.end {
		out = append(out, interval{cut.end + 1, iv.end})
	}
	return out
}

func flatten(ivs [][]interval) []interval {
	var out []interval
	for _, x := range ivs {
		out = append(out, x...)
	}
	return out
}

func formatIntervals(ivs []interval) string {
	parts := make([]string, 0, len(ivs))
	for _, iv := range ivs {
		if iv.start == iv.end {
			parts = append(parts, formatInt(iv.start))
		} else {
			parts = append(parts, formatInt(iv.start)+"-"+formatInt(iv.end))
		}
	}
	return strings.Join(parts, ":")
}

func formatInt(n int64) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// Ahead returns true if other is strictly ahead of self (has transactions self lacks).
func (self Set) AheadOf(other Set) bool {
	return !Subtract(other, self).Empty()
}

// ComparePeers picks the most complete replica (no peer strictly ahead), tie by lowest name.
func ComparePeers(candidates map[string]Set) (winner string, ok bool) {
	names := make([]string, 0, len(candidates))
	for n := range candidates {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, name := range names {
		self := candidates[name]
		behind := false
		for otherName, other := range candidates {
			if otherName == name {
				continue
			}
			if !Subtract(other, self).Empty() {
				behind = true
				break
			}
		}
		if behind {
			continue
		}
		return name, true
	}
	return "", false
}

// Count returns the number of transactions in the set.
func (s Set) Count() int64 {
	var n int64
	for _, ivs := range parseMap(string(s)) {
		for _, iv := range ivs {
			n += iv.end - iv.start + 1
		}
	}
	return n
}

// Superset reports whether s contains every transaction of other.
func (s Set) Superset(other Set) bool { return Subtract(other, s).Empty() }

// Union returns a set containing every transaction of a and b.
func Union(a, b Set) Set {
	switch {
	case a.Empty():
		return b
	case b.Empty():
		return a
	}
	m := parseMap(string(a) + "," + string(b))
	parts := make([]string, 0, len(m))
	for uuid, ivs := range m {
		parts = append(parts, uuid+":"+formatIntervals(ivs))
	}
	sort.Strings(parts)
	return Set(strings.Join(parts, ","))
}
