package server

import (
	"os/exec"
	"regexp"
	"testing"
)

// TestAppJSAsList runs the dashboard's asList helper under node (skipped when
// node is not installed): the actions panel must accept an array, the old
// {entries: ...} object, and null without throwing.
func TestAppJSAsList(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	src, err := staticFS.ReadFile("static/app.js")
	if err != nil {
		t.Fatal(err)
	}
	fn := regexp.MustCompile(`(?s)function asList\(v\) \{.*?\n  \}`).Find(src)
	if fn == nil {
		t.Fatal("asList not found in app.js")
	}
	script := string(fn) + `
const assert = require('assert');
assert.deepStrictEqual(asList([1, 2]), [1, 2]);
assert.deepStrictEqual(asList({entries: [3]}), [3]);
assert.deepStrictEqual(asList({entries: null}), []);
assert.deepStrictEqual(asList(null), []);
assert.deepStrictEqual(asList({}), []);
asList({entries: null}).slice().reverse();
`
	if out, err := exec.Command(node, "-e", script).CombinedOutput(); err != nil {
		t.Fatalf("node: %v\n%s", err, out)
	}
}
