// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestClaudeMdStaysWithinContextBudget pins the 2026-09-12 split of the
// private operator file: CLAUDE.md had grown to 24,076 lines (~350K tokens)
// by inlining every release ledger entry, which exceeded the context window of
// every subagent model and made background reviews die on launch. The ledger
// now lives in summary/changelog/ and CLAUDE.md carries pointers.
//
// Both files are gitignored, so the test soft-skips when CLAUDE.md is absent
// (public checkouts, CI). Any other read error fails (SR-5: absence is the
// only expected miss).
//
// Contract: no `### Changes in v` heading returns to CLAUDE.md, the file stays
// under claudeMdMaxLines, and every rules file stays under rulesMaxLines. The
// budgets are derived from the docs' "target under 200 lines" guidance with
// headroom for the SR registry (72 lines) that the standing-rules meta-test
// requires to stay in CLAUDE.md.
func TestClaudeMdStaysWithinContextBudget(t *testing.T) {
	const claudeMdMaxLines = 400
	const rulesMaxLines = 300

	root := repoRootFromScripts(t)
	claudePath := filepath.Join(root, "CLAUDE.md")
	b, err := os.ReadFile(claudePath)
	if errors.Is(err, os.ErrNotExist) {
		t.Skip("CLAUDE.md absent (public checkout) — nothing to budget")
	}
	if err != nil {
		t.Fatalf("reading CLAUDE.md: %v (only absence soft-skips)", err)
	}
	src := string(b)
	if n := lineCount(src); n > claudeMdMaxLines {
		t.Errorf("CLAUDE.md is %d lines (budget %d) — move the content to .claude/rules/ or summary/changelog/", n, claudeMdMaxLines)
	}
	if strings.Contains(src, "\n### Changes in v") {
		t.Error("CLAUDE.md carries a `### Changes in v` ledger entry — release entries go to summary/changelog/vX.Y.md; CLAUDE.md gets a pointer, not the story")
	}

	rules, err := filepath.Glob(filepath.Join(root, ".claude", "rules", "*.md"))
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rules {
		rb, err := os.ReadFile(r)
		if err != nil {
			t.Fatalf("reading %s: %v", r, err)
		}
		if n := lineCount(string(rb)); n > rulesMaxLines {
			t.Errorf("%s is %d lines (budget %d) — split it or move history to summary/changelog/", filepath.Base(r), n, rulesMaxLines)
		}
	}
}

// TestLineCountNormalizesTrailingNewline (Copilot review on PR #203):
// counting "\n" separators under-counts a file with no trailing newline
// by one, so a 401-line file passed a 400-line budget. The count is
// logical lines, trailing newline or not.
func TestLineCountNormalizesTrailingNewline(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want int
	}{
		{"", 0},
		{"a", 1},
		{"a\n", 1},
		{"a\nb", 2},
		{"a\nb\n", 2},
	} {
		if got := lineCount(tc.in); got != tc.want {
			t.Errorf("lineCount(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

// lineCount is the number of logical lines: a trailing newline does not
// add one, and a final line without one still counts (PR #203 review —
// counting separators let a 401-line file pass a 400-line budget).
func lineCount(s string) int {
	if s == "" {
		return 0
	}
	return len(strings.Split(strings.TrimSuffix(s, "\n"), "\n"))
}

// repoRootFromScripts resolves the checkout root from the scripts package
// directory without depending on the checkout's basename.
func repoRootFromScripts(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Dir(wd)
}
