// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The Phase 4 meta-test (v0.27.127): the standing-rules registry stays
// TRUE. Four checks:
//
//  1. every active, non-ProcessOnly rule's EnforcedBy names exist as
//     test functions somewhere in the tree (a renamed/deleted test
//     breaks the rule's enforcement silently otherwise);
//  2. reverse: every `SR-<n>` token cited in any *_test.go names an
//     existing rule (no dangling citations);
//  3. IDs are unique (they are never reused — a Retired rule keeps
//     its ID forever);
//  4. when CLAUDE.md is present (the private operator doc — absent in
//     public checkouts, so this soft-skips), every active SR-ID
//     appears in it: the full prose context lives there by operator
//     decision, and an ID missing from the prose means the private
//     and public halves have drifted.
func TestStandingRulesRegistry(t *testing.T) {
	root := srctest.Root(t)

	// Collect every test-function name + every SR citation in the tree.
	funcRe := regexp.MustCompile(`(?m)^func (Test\w+)\(`)
	srRe := regexp.MustCompile(`\bSR-(\d+)\b`)
	testFuncs := map[string]bool{}
	citations := map[string][]string{} // SR-ID -> citing files
	scanned := 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			src := string(b)
			for _, m := range funcRe.FindAllStringSubmatch(src, -1) {
				testFuncs[m[1]] = true
			}
			rel, _ := filepath.Rel(root, path)
			if filepath.ToSlash(rel) == "scripts/standing_rules_test.go" {
				return nil // the meta-test's own examples are not citations
			}
			for _, m := range srRe.FindAllStringSubmatch(src, -1) {
				id := "SR-" + m[1]
				citations[id] = append(citations[id], filepath.ToSlash(rel))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "test files scanned by the standing-rules meta-test", scanned, 300)

	ids := map[string]bool{}
	for _, r := range standingRules {
		// (3) unique IDs.
		if ids[r.ID] {
			t.Errorf("%s: duplicate rule ID — IDs are never reused", r.ID)
		}
		ids[r.ID] = true
		if strings.TrimSpace(r.Statement) == "" {
			t.Errorf("%s: rules REQUIRE a one-line statement", r.ID)
		}
		if r.Retired {
			continue
		}
		// (1) enforcement exists.
		if !r.ProcessOnly && len(r.EnforcedBy) == 0 {
			t.Errorf("%s: non-ProcessOnly rules must name at least one enforcing test", r.ID)
		}
		for _, fn := range r.EnforcedBy {
			if !testFuncs[fn] {
				t.Errorf("%s: enforcing test %q does not exist in the tree — the rule's enforcement silently died (renamed test? update EnforcedBy)", r.ID, fn)
			}
		}
	}
	// (2) no dangling citations.
	for id, files := range citations {
		if !ids[id] {
			t.Errorf("%s is cited in %v but names no registered rule — add the rule or fix the citation", id, files)
		}
	}
	// (4) CLAUDE.md carries every active ID (soft-skip when absent).
	claudePath := filepath.Join(root, "CLAUDE.md")
	b, err := os.ReadFile(claudePath)
	if err != nil {
		t.Logf("CLAUDE.md absent (%v) — soft-skipping the private-half drift check (expected in public checkouts)", err)
		return
	}
	prose := string(b)
	for _, r := range standingRules {
		if r.Retired {
			continue
		}
		if !strings.Contains(prose, r.ID) {
			t.Errorf("%s missing from CLAUDE.md — the private prose half must carry every active SR-ID (operator decision: full context stays private, IDs bridge the two halves)", r.ID)
		}
	}
}
