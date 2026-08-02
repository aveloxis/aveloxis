// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

// 2026-08-02 tripwire — the defer-vs-t.Cleanup ordering trap that
// broke CI on PR #171: Go runs a test function's DEFERRED calls
// BEFORE its t.Cleanup callbacks, so a test that does
//
//	store, ctx := v0251Connect(t)
//	defer store.Close()          // runs FIRST at test end
//	t.Cleanup(func() { ...pool.Exec(DELETE ...)... }) // runs against a CLOSED pool
//
// silently strands every fixture row its cleanups were supposed to
// delete (the Exec errors are conventionally discarded). The
// collections e2e leaked 3 queue rows with synthetic cached counts
// this way, and the data-verify battery's "cached counts vs actual"
// probe — running later in the same package — failed the build with
// "CompleteJob's cumulative-count contract is broken" on perfectly
// healthy code. Integration tests must register the pool close as
// the FIRST t.Cleanup (`t.Cleanup(store.Close)`) so it runs LAST,
// after every data cleanup.

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var deferPoolClose = regexp.MustCompile(`\bdefer\s+(store|pool|s)\.(pool\.)?Close\(\)`)

func TestNoDeferPoolCloseInTests(t *testing.T) {
	root := repoRootDir(t)
	var offenders []string
	var scanned int
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.Walk(filepath.Join(root, dir), func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				if strings.Contains(path, ".claude") {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			scanned++
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			// Strip // comments so prose describing the pattern (e.g.
			// scheduler/shutdown_test.go's runServe discussion) can't
			// false-match.
			var code []string
			for _, line := range strings.Split(string(raw), "\n") {
				if i := strings.Index(line, "//"); i >= 0 {
					line = line[:i]
				}
				code = append(code, line)
			}
			if loc := deferPoolClose.FindString(strings.Join(code, "\n")); loc != "" {
				rel, _ := filepath.Rel(root, path)
				offenders = append(offenders, rel+" ("+loc+")")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if scanned < 100 {
		t.Fatalf("only scanned %d _test.go files — the walk is broken, fix the tripwire before trusting it", scanned)
	}
	for _, o := range offenders {
		t.Errorf("%s: `defer <store>.Close()` in a test — deferred calls run BEFORE t.Cleanup callbacks, so any pool-using cleanup silently fails against a closed pool and strands fixture residue (the PR #171 CI failure). Use `t.Cleanup(store.Close)` immediately after connecting instead.", o)
	}
}

// repoRootDir walks up from the working directory to the go.mod root.
func repoRootDir(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found walking up from the test working directory")
		}
		dir = parent
	}
}
