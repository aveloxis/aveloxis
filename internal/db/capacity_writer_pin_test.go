// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// userReposInsert matches an INSERT into the group-link table in any case
// and spacing (comments are stripped first, so prose cannot satisfy or
// escape it).
var userReposInsert = regexp.MustCompile(`(?i)insert\s+into\s+aveloxis_ops\.user_repos\b`)

// TestOneWriterOfNewGroupLinks pins summary/53 A8 (SR-18): a NEW group link
// is written only by linkWithinCap, inside the owner's repository
// allocation. The two other INSERTs re-point existing links from a
// duplicate repository to its survivor (the count of distinct
// repositories an account reaches can only fall). Exact counts: a new
// writer anywhere in the module, or a re-pointer that moved, fails.
func TestOneWriterOfNewGroupLinks(t *testing.T) {
	want := map[string]int{
		"internal/db/capacity_store.go":        1, // linkWithinCap
		"internal/db/repo_dedup.go":            1, // repoint user_repos (case-variant merge)
		"internal/db/rename_duplicate_heal.go": 1, // repoint group links (rename duplicate)
	}
	root := srctest.Root(t)
	got := map[string]int{}
	examined := 0
	for _, dir := range []string{"internal", "cmd", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			rel, _ := filepath.Rel(root, path)
			if n := len(userReposInsert.FindAllString(srctest.StripGoComments(string(b)), -1)); n > 0 {
				got[filepath.ToSlash(rel)] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files examined", examined, 300)
	for f, n := range got {
		if want[f] != n {
			t.Errorf("%s has %d INSERT(s) into aveloxis_ops.user_repos; want %d — a new group link goes through linkWithinCap (the repository allocation, summary/53)", f, n, want[f])
		}
	}
	for f, n := range want {
		if got[f] != n {
			t.Errorf("%s: want %d INSERT(s) into aveloxis_ops.user_repos, found %d", f, n, got[f])
		}
	}
	src := srctest.Read(t, "internal/db/capacity_store.go")
	stmt := srctest.ConstBody(t, src, "insertLinksSQL")
	for _, needle := range []string{"INSERT INTO aveloxis_ops.user_repos", "ON CONFLICT DO NOTHING"} {
		if !strings.Contains(stmt, needle) {
			t.Errorf("insertLinksSQL lost %q: the one writer is idempotent", needle)
		}
	}
	// Every insert linkWithinCap makes is that statement (the fast path and
	// the decided path), and it reports the links it inserted.
	link := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) linkWithinCap("))
	if n := strings.Count(link, "Exec(ctx, insertLinksSQL,"); n != 2 {
		t.Errorf("linkWithinCap inserts through insertLinksSQL at %d sites; want 2 (the fast path and the decided path)", n)
	}
	if strings.Count(link, "RowsAffected()") < 2 {
		t.Error("linkWithinCap must report the links each insert inserted (RowsAffected)")
	}
}

// TestLinkHeldHasOneCaller: linkHeld asks for no new place, so it is only
// for a repository whose place an approved add-request item holds — the
// approval pass's link in ensureRepoCollectedInGroup. Any other caller
// would bypass the allocation.
func TestLinkHeldHasOneCaller(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 100)
	sites := 0
	for name, src := range files {
		n := strings.Count(srctest.StripGoComments(src), "linkHeld)")
		if n == 0 {
			continue
		}
		sites += n
		if name != "internal/db/add_requests.go" ||
			!strings.Contains(srctest.FuncBody(t, src, "func (s *PostgresStore) ensureRepoCollectedInGroup("), "linkHeld)") {
			t.Errorf("%s passes linkHeld outside ensureRepoCollectedInGroup", name)
		}
	}
	if sites != 1 {
		t.Errorf("linkHeld is passed at %d sites; want exactly 1 (ensureRepoCollectedInGroup)", sites)
	}
}

// Closing review r3 F7 (SR-18): AddOrgRepoToGroupByID is the door that does
// not count toward repo_links_per_day, so only reviewed organization-driven
// sites may call it (an administrator approved the organization). A new
// caller — a user-facing handler above all — fails here until reviewed.
func TestOrgLinkDoorHasOnlyReviewedCallers(t *testing.T) {
	reviewed := map[string]int{
		"internal/scheduler/scheduler.go":    3, // the GitHub org, GitLab group and demand scans
		"internal/web/server.go":             2, // scanOrgRepos: existing and new repositories
		"cmd/aveloxis/main.go":               1, // add-repo's org expansion
		"cmd/aveloxis/import_foundations.go": 1, // the foundation loader's dashboard groups
	}
	root := srctest.Root(t)
	got := map[string]int{}
	examined := 0
	for _, dir := range []string{"internal", "cmd", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			if n := strings.Count(srctest.StripGoComments(string(b)), ".AddOrgRepoToGroupByID("); n > 0 {
				rel, _ := filepath.Rel(root, path)
				got[filepath.ToSlash(rel)] = n
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files examined", examined, 300)
	for f, n := range got {
		if reviewed[f] != n {
			t.Errorf("%s calls AddOrgRepoToGroupByID %d time(s); reviewed %d — the organization door bypasses the daily additions quota: use AddRepoToGroupByID, or review the site and add it here with the reason", f, n, reviewed[f])
		}
	}
	for f, n := range reviewed {
		if got[f] != n {
			t.Errorf("%s: %d reviewed AddOrgRepoToGroupByID call(s), %d found — update the list", f, n, got[f])
		}
	}
}
