// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestPhaseCompletionLogsIncludeOwnerRepo — v0.22.4 item 1.
//
// Every per-phase "X staged" completion log line in internal/collector/
// staged.go MUST include the "owner" and "repo" slog keys. Production
// log on 2026-05-16 showed zephyr+pytorch silently staging ~660
// review_comments/min with NO owner/repo on the completion lines,
// indistinguishable from a stuck repo. Pinning the keys here prevents
// any future refactor from anonymizing the lines again.
func TestPhaseCompletionLogsIncludeOwnerRepo(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Each of these completion log lines should carry "owner" and "repo"
	// keys within its argument list. Whitespace-tolerant scan window:
	// from the literal log message to the matching closing paren.
	completionLines := []string{
		`"contributors staged"`,
		`"issues staged"`,
		`"pull requests staged"`,
		`"events staged"`,
		`"messages staged"`,
	}

	for _, msg := range completionLines {
		idx := strings.Index(code, msg)
		if idx < 0 {
			t.Errorf("staged.go: completion log %s not found", msg)
			continue
		}
		// Slice from the message forward to the next closing paren on
		// its own logical line. The log call is a single statement so
		// the closing paren of the Info(...) call is the right window.
		end := strings.Index(code[idx:], ")")
		if end < 0 {
			t.Errorf("staged.go: could not find end of log call for %s", msg)
			continue
		}
		window := code[idx : idx+end]
		if !strings.Contains(window, `"owner"`) {
			t.Errorf("staged.go: log %s must include slog key \"owner\" — "+
				"production diagnosis on 2026-05-16 showed silent per-repo "+
				"completion lines were indistinguishable from hangs", msg)
		}
		if !strings.Contains(window, `"repo"`) {
			t.Errorf("staged.go: log %s must include slog key \"repo\"", msg)
		}
	}
}

// TestPhaseCompletionLogsHaveOwnerRepoAvailable — sanity check that the
// owner and repo locals are in scope at each of the call sites. If a
// future refactor moves one of these log calls outside the
// owner/repo-bearing function, the previous test passes against a
// literal but the code won't compile. This catches the in-scope
// requirement explicitly.
func TestPhaseCompletionLogsHaveOwnerRepoAvailable(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Match a `func (sc *StagedCollector) NAME(... owner, repo string ...)`
	// or `func (sc *StagedCollector) NAME(... owner string, repo string ...)`
	// signature so we know the locals are in scope.
	// We don't enforce signature shape here — just check that each
	// completion log appears within a function that takes owner/repo.
	completionLines := []string{
		`"contributors staged"`,
		`"issues staged"`,
		`"pull requests staged"`,
		`"events staged"`,
		`"messages staged"`,
	}

	funcStart := regexp.MustCompile(`(?m)^func\s+\(\s*\w+\s+\*\w+\s*\)\s+\w+\s*\(`)
	funcStarts := funcStart.FindAllStringIndex(code, -1)
	if len(funcStarts) == 0 {
		t.Fatal("could not find any method declarations in staged.go")
	}

	for _, msg := range completionLines {
		idx := strings.Index(code, msg)
		if idx < 0 {
			continue // covered by the other test
		}
		// Find the enclosing function: the latest funcStart whose
		// offset is <= idx.
		enclosing := -1
		for _, fs := range funcStarts {
			if fs[0] <= idx {
				enclosing = fs[0]
			} else {
				break
			}
		}
		if enclosing < 0 {
			t.Errorf("staged.go: %s is not inside any method body", msg)
			continue
		}
		// Read up to ~400 chars of the function signature to find the
		// parameter list and check for owner/repo there.
		sigEnd := strings.Index(code[enclosing:], "{")
		if sigEnd < 0 || sigEnd > 800 {
			continue
		}
		sig := code[enclosing : enclosing+sigEnd]
		if !strings.Contains(sig, "owner") || !strings.Contains(sig, "repo") {
			t.Errorf("staged.go: completion log %s is inside a function "+
				"whose signature does not declare owner/repo: %q", msg, sig)
		}
	}
}
