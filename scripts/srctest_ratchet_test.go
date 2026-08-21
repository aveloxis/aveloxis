// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The srctest migration RATCHET (v0.27.118) — the operator's
// "fix when touched, and aggressively validate" mechanism, in the
// house tripwire style aimed at the test suite itself.
//
// The 2026-08-20 inventory found ~40 near-identical read helpers, ~23
// function-body extractors (five incompatible variants), duplicated
// comment strippers and repo-root finders scattered across test files.
// internal/srctest is now the single home for those operations, with
// STRANGLER adoption (no bulk migration — refactoring working pins is
// itself a regression risk; see docs/contributing/testing.md).
//
// srctest_migration_baseline.txt freezes the legacy sites that existed
// at ratchet introduction. This test enforces both directions:
//   - a detected legacy-helper definition NOT in the baseline FAILS:
//     new duplication is banned — use internal/srctest;
//   - a baseline entry whose site no longer exists FAILS: delete the
//     line (the stale-allowlist reverse check). The baseline can only
//     SHRINK — burn-down progress is `wc -l` on one file, and touching
//     a legacy test file pairs naturally with deleting its lines in
//     the same diff.
//
// Regenerate after a migration wave: AVELOXIS_UPDATE_BASELINE=1
// go test ./scripts/ -run TestSrctestMigrationRatchet
// (the golden-file pattern from the manifest corpus). Review the diff:
// it must ONLY DELETE lines.

// legacyHelperPatterns detect DEFINITIONS of the duplicated helper
// classes in *_test.go files. Inline os.ReadFile calls are fine (plain
// reads of package-local files stay idiomatic); what ratchets down is
// defining NEW named helpers that duplicate srctest.
var legacyHelperPatterns = []*regexp.Regexp{
	// Read helpers: a named func taking testing.T/TB, returning
	// string/[]byte, whose nearby body calls os.ReadFile.
	regexp.MustCompile(`(?ms)^func (\w+)\(\w+ \*?testing\.(?:T|TB)[^)]*\) (?:string|\[\]byte) \{.{0,500}?os\.ReadFile`),
	// Function-body extractors.
	regexp.MustCompile(`(?m)^func (extract\w+)\(`),
	// Comment strippers.
	regexp.MustCompile(`(?m)^func (strip\w*[Cc]omments?\w*)\(`),
	// Repo-root finders.
	regexp.MustCompile(`(?m)^func (\w*[Rr]epoRoot\w*)\(`),
}

func detectLegacySites(t *testing.T, root string) map[string]bool {
	t.Helper()
	sites := map[string]bool{}
	scanned := 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if strings.HasSuffix(filepath.ToSlash(path), "internal/srctest") {
					return filepath.SkipDir // the replacement itself
				}
				return nil
			}
			if !strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			if filepath.ToSlash(rel) == "scripts/srctest_ratchet_test.go" {
				return nil // this file mentions the patterns it detects
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			scanned++
			for _, re := range legacyHelperPatterns {
				for _, m := range re.FindAllStringSubmatch(string(b), -1) {
					sites[filepath.ToSlash(rel)+" "+m[1]] = true
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "test files scanned by the ratchet", scanned, 300)
	return sites
}

func TestSrctestMigrationRatchet(t *testing.T) {
	root := srctest.Root(t)
	baselinePath := filepath.Join(root, "scripts", "srctest_migration_baseline.txt")
	detected := detectLegacySites(t, root)

	if os.Getenv("AVELOXIS_UPDATE_BASELINE") == "1" {
		var lines []string
		for s := range detected {
			lines = append(lines, s)
		}
		sort.Strings(lines)
		content := "# srctest migration baseline — legacy helper sites frozen at ratchet\n" +
			"# introduction (v0.27.118). SHRINK-ONLY: entries are deleted as files\n" +
			"# migrate to internal/srctest; new entries are a build failure.\n" +
			"# Regenerate: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run Ratchet\n" +
			strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(baselinePath, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("baseline regenerated: %d sites", len(lines))
		return
	}

	raw, err := os.ReadFile(baselinePath)
	if err != nil {
		t.Fatalf("baseline missing (%v) — seed it: AVELOXIS_UPDATE_BASELINE=1 go test ./scripts/ -run Ratchet", err)
	}
	baseline := map[string]bool{}
	for _, line := range strings.Split(string(raw), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		baseline[line] = true
	}
	srctest.MinCount(t, "baseline entries", len(baseline), 1)

	var newDup, stale []string
	for s := range detected {
		if !baseline[s] {
			newDup = append(newDup, s)
		}
	}
	for s := range baseline {
		if !detected[s] {
			stale = append(stale, s)
		}
	}
	sort.Strings(newDup)
	sort.Strings(stale)
	for _, s := range newDup {
		t.Errorf("NEW legacy helper %q — duplicating srctest is banned; use internal/srctest (Read/FuncBody/Strip*/Root) instead", s)
	}
	for _, s := range stale {
		t.Errorf("stale baseline entry %q — the site is gone (migrated?); delete its line from %s (the baseline only shrinks)", s, "scripts/srctest_migration_baseline.txt")
	}
	if len(newDup) == 0 && len(stale) == 0 {
		t.Logf("ratchet: %d legacy sites remain", len(baseline))
	}
}
