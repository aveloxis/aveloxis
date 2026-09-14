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

// v0.30.0 (multi-instance GitLab): every GitLab instance has its own
// platform_id, so model.PlatformGitLab now names ONE instance — the
// historical one. Code that means "any GitLab" asks Platform.IsGitLab (or
// IsForge); code that writes a platform_id asks the client or registry for
// the instance's id. A new bare use of the constant is almost always the
// old meaning and silently excludes every other instance.
//
// gitLabConstantUses is a burn-down RATCHET over non-test Go sources: a file
// may not use the constant more often than its entry allows, and an entry
// larger than the file's actual use fails too (shrink it). The goal state
// is the permanent residue below — the model definition, the URL parser's
// family result, and the few decisions that really are about instance 2.
var gitLabConstantUses = map[string]int{
	// Permanent: the definition, String/IsGitLab.
	"internal/model/repo.go": 2,
	// Permanent: the parser has no registry and returns the GitLab family.
	"internal/platform/repourl.go": 4,
	// Permanent: the SQL twin of IsForge lists the fixed forge ids.
	"internal/db/forge_platform_sql.go": 1,
	// Permanent: the registry stamps, reads and keeps the seeded name of the
	// historical instance.
	// Permanent: classification and adoption apply platform 2's own rules
	// (unsynced window, historical web base, misrouted rows).
	"internal/db/gitlab_classification.go": 6,
	"internal/db/gitlab_instances.go":      5,
	// Permanent: gl_id is written only for instance 2.
	"internal/db/postgres.go": 1,
	// Permanent (v0.30.0 Phase C): before any serve report names the main
	// instance, the admin key list treats instance 2 as main (its registry
	// row is stamped from the main instance's web URL), and only platform-2
	// rows appear in the misrouted list it counts.
	"internal/api/api_keys.go": 2,
	// Permanent: enrichment falls back to the historical instance only
	// (thin logins carry no instance), and only platform 2 rows appear in the
	// misrouted list the fix hint points at.
	"internal/scheduler/scheduler.go": 2,
}

var gitLabConstantRe = regexp.MustCompile(`\bPlatformGitLab\b`)

func TestGitLabHistoricalInstanceConstantRatchet(t *testing.T) {
	root := srctest.Root(t)
	got := map[string]int{}
	examined := 0
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			examined++
			if n := len(gitLabConstantRe.FindAllString(srctest.StripGoComments(srctest.Read(t, rel)), -1)); n > 0 {
				got[rel] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files under internal/ and cmd/", examined, 300)

	files := map[string]bool{}
	for f := range got {
		files[f] = true
	}
	for f := range gitLabConstantUses {
		files[f] = true
	}
	var sorted []string
	for f := range files {
		sorted = append(sorted, f)
	}
	sort.Strings(sorted)
	for _, f := range sorted {
		switch have, allowed := got[f], gitLabConstantUses[f]; {
		case have > allowed:
			t.Errorf("%s uses model.PlatformGitLab %d times (ratchet allows %d). It names only the historical GitLab instance since v0.30.0: use Platform.IsGitLab/IsForge for \"any GitLab\", or the client's/registry's instance id for a platform_id.", f, have, allowed)
		case have < allowed:
			t.Errorf("%s now uses model.PlatformGitLab %d times; shrink its ratchet entry from %d to %d.", f, have, allowed, have)
		}
	}
}

// The SQL half of the same drift class: `platform_id IN (1, 2)` and
// `platform_id = 2` meant "the forges" and "GitLab" when there was one
// GitLab. New SQL uses db.ForgePlatformPredicate (or the registry). The
// allowlisted statements are historical and frozen on purpose: the
// uq_repos_repo_git_ci predicate lives with its index name (SR-4), and two
// ledgered one-shots ran when only instance 2 existed — the v0.27.37 GitLab
// force-full step and the releases data_source backfill
// (CASE r.platform_id WHEN 2). Spellings caught: = 2, <> 2, != 2, WHEN 2,
// IN (2), IN (1, 2) and IN (2, 1).
var literalGitLabSQLAllowed = map[string]int{
	"internal/db/migrate.go": 3,
}

var literalGitLabSQLRe = regexp.MustCompile(`(?i)platform_id\s*(=|<>|!=|WHEN)\s*2\b|platform_id\s+IN\s*\(\s*(2|1\s*,\s*2|2\s*,\s*1)\s*\)`)

func TestLiteralGitLabSQLSpellings(t *testing.T) {
	cases := map[string]bool{
		"platform_id = 2":                 true,
		"r.platform_id=2":                 true,
		"platform_id <> 2":                true,
		"platform_id != 2":                true,
		"CASE r.platform_id WHEN 2 THEN":  true,
		"platform_id IN (2)":              true,
		"platform_id IN (1, 2)":           true,
		"PLATFORM_ID in (2,1)":            true,
		"platform_id = 20":                false,
		"platform_id = 1":                 false,
		"platform_id == 2":                false,
		"platform_id IN (1, 2, 3)":        false,
		"platform_id BETWEEN 100 AND 199": false,
		"platform_id = $2":                false,
	}
	for s, want := range cases {
		if got := literalGitLabSQLRe.MatchString(s); got != want {
			t.Errorf("literalGitLabSQLRe.MatchString(%q) = %v, want %v", s, got, want)
		}
	}
}

func TestNoLiteralGitLabPlatformPredicates(t *testing.T) {
	root := srctest.Root(t)
	got := map[string]int{}
	examined := 0
	for _, top := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, err := filepath.Rel(root, path)
			if err != nil {
				return err
			}
			rel = filepath.ToSlash(rel)
			examined++
			if n := len(literalGitLabSQLRe.FindAllString(srctest.StripGoComments(srctest.Read(t, rel)), -1)); n > 0 {
				got[rel] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files under internal/ and cmd/", examined, 300)
	for f, n := range got {
		if n > literalGitLabSQLAllowed[f] {
			t.Errorf("%s has %d literal GitLab platform predicates (allowed %d): use db.ForgePlatformPredicate for \"any forge\"; a GitLab instance is not always platform_id 2 since v0.30.0", f, n, literalGitLabSQLAllowed[f])
		}
	}
	for f, allowed := range literalGitLabSQLAllowed {
		if got[f] < allowed {
			t.Errorf("%s now has %d literal GitLab platform predicates; shrink its allowance from %d", f, got[f], allowed)
		}
	}
}
