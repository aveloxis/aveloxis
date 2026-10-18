// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"go/ast"
	"go/parser"
	"go/token"
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
// is written only through insertLinksSQL, by linkWithinCap or
// addPasteWithinCap (v0.29.90); new repositories are admitted by
// decideWithinCap (linkWithinCap's fast path links nothing new; linkHeld
// takes a place holdRepoPlaces already decided; the decide → refuse →
// insert order is pinned for the paste writer). The two other INSERTs
// re-point existing links from a
// duplicate repository to its survivor (the count of distinct
// repositories an account reaches can only fall). Exact counts: a new
// writer anywhere in the module, or a re-pointer that moved, fails.
func TestOneWriterOfNewGroupLinks(t *testing.T) {
	want := map[string]int{
		"internal/db/capacity_store.go":        1, // insertLinksSQL (linkWithinCap ×3, addPasteWithinCap ×1)
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
			t.Errorf("%s has %d INSERT(s) into aveloxis_ops.user_repos; want %d — a new group link goes through insertLinksSQL behind decideWithinCap (the repository allocation, summary/53)", f, n, want[f])
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
	// Every insert linkWithinCap makes is that statement — linkHeld's
	// (v0.29.91: one plain insert, no policy read or transaction; the place
	// is held), the fast path and the decided path — and it reports the
	// links it inserted.
	link := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) linkWithinCap("))
	if n := strings.Count(link, "Exec(ctx, insertLinksSQL,"); n != 3 {
		t.Errorf("linkWithinCap inserts through insertLinksSQL at %d sites; want 3 (linkHeld, the fast path, the decided path)", n)
	}
	if strings.Count(link, "RowsAffected()") < 3 {
		t.Error("linkWithinCap must report the links each insert inserted (RowsAffected)")
	}
	// linkHeld's insert returns before anything else is read: the branch
	// opens the body, and its return comes before the owner lookup, the
	// policy read and the transaction.
	held := strings.Index(link, "if mode == linkHeld {")
	heldInsert := strings.Index(link, "tag, err := s.pool.Exec(ctx, insertLinksSQL, groupID, repoIDs)")
	heldReturn := strings.Index(link, "return int(tag.RowsAffected()), nil")
	for name, at := range map[string]int{"the owner lookup": strings.Index(link, "SELECT user_id FROM aveloxis_ops.user_groups"),
		"the policy read": strings.Index(link, "s.readOwnerPolicy("), "the transaction": strings.Index(link, "s.pool.Begin(")} {
		if held < 0 || heldInsert < held || heldReturn < heldInsert || at < heldReturn {
			t.Errorf("linkHeld must insert and return before %s (positions %d, %d, %d, %d)", name, held, heldInsert, heldReturn, at)
		}
	}
	// v0.29.90 (L10 review F1): the statement's users are exactly these
	// sites, package-wide — a third user, with or without a decision,
	// fails here instead of passing the per-file INSERT count.
	users := 0
	for name, f := range srctest.PackageFiles(t, "internal/db", 100) {
		n := strings.Count(srctest.StripGoComments(f), "insertLinksSQL")
		if name == "internal/db/capacity_store.go" {
			n-- // the const declaration
		}
		users += n
	}
	paste := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) addPasteWithinCap("))
	if got := strings.Count(link, "insertLinksSQL") + strings.Count(paste, "insertLinksSQL"); users != 4 || got != 4 {
		t.Errorf("insertLinksSQL is used at %d sites (%d in linkWithinCap + addPasteWithinCap); want 4, all there (linkWithinCap ×3: linkHeld, fast path, decided; addPasteWithinCap ×1)", users, got)
	}
	// The paste inserts only after its decision and the refusal's return:
	// the return must come before the insert, not just exist.
	decide := strings.Index(paste, "s.decideWithinCap(")
	refused := strings.Index(paste, "if d.refused != nil {\n\t\treturn 0, 0, d.refused")
	insert := strings.Index(paste, "Exec(ctx, insertLinksSQL,")
	if decide < 0 || refused < decide || insert < refused {
		t.Errorf("addPasteWithinCap must decide (decideWithinCap), return the refusal, then insert; positions %d, %d, %d", decide, refused, insert)
	}
}

// TestLinkHeldHasOneCaller: linkHeld asks for no new place, so it is only
// for a repository whose place an approved add-request item holds — the
// approval pass's link in ensureRepoCollectedInGroup (plus linkWithinCap's
// own dispatch on it). Any other use would bypass the allocation. Counted
// as identifiers with go/ast (v0.29.91 round 3 R3-3: a token count missed
// a multi-line call, a variable or an alias).
func TestLinkHeldHasOneCaller(t *testing.T) {
	assertModeUses(t, "linkHeld", map[string]int{"ensureRepoCollectedInGroup": 1, "linkWithinCap": 1})
}

// linkModeMethods are linkMode's reviewed methods, which name the modes
// to classify them.
var linkModeMethods = map[string]bool{"fill": true, "org": true, "countsAdds": true}

// assertModeUses requires the linkMode constant mode to be used exactly at
// want (function name → uses) across internal/db's non-test files, apart
// from its declaration and linkMode's three reviewed methods (fill, org,
// countsAdds). linkMode is a pointer identity (round 4 R4-1(b)), so no
// arithmetic, conversion or literal reaches a mode without naming it.
// Every identifier is counted, wherever it sits: a call argument, a
// variable, an alias constant, a comparison.
func assertModeUses(t *testing.T, mode string, want map[string]int) {
	t.Helper()
	got := map[string]int{}
	fset := token.NewFileSet()
	examined := 0
	for name, src := range srctest.PackageFiles(t, "internal/db", 100) {
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		examined++
		for _, decl := range f.Decls {
			var where string
			switch d := decl.(type) {
			case *ast.FuncDecl:
				// The mode's three reviewed methods only (round 4 R4-1(a): every
				// linkMode method was skipped, so a new one returning linkHeld
				// escaped).
				if d.Recv != nil && len(d.Recv.List) == 1 {
					if id, ok := d.Recv.List[0].Type.(*ast.Ident); ok && id.Name == "linkMode" && linkModeMethods[d.Name.Name] {
						continue
					}
				}
				where = d.Name.Name
			case *ast.GenDecl:
				where = srctest.PackageLevel
			}
			ast.Inspect(decl, func(n ast.Node) bool {
				if vs, ok := n.(*ast.ValueSpec); ok {
					// The declaration itself is not a use; its values are.
					for _, v := range vs.Values {
						ast.Inspect(v, func(m ast.Node) bool {
							if id, ok := m.(*ast.Ident); ok && id.Name == mode {
								got[where]++
							}
							return true
						})
					}
					return false
				}
				if id, ok := n.(*ast.Ident); ok && id.Name == mode {
					got[where]++
				}
				return true
			})
		}
	}
	srctest.MinCount(t, "internal/db files parsed", examined, 100)
	for fn, n := range got {
		if want[fn] != n {
			t.Errorf("%s is used %d time(s) in %s; reviewed %d — a new use bypasses what the mode decides: review it and add it here", mode, n, fn, want[fn])
		}
	}
	for fn, n := range want {
		if got[fn] != n {
			t.Errorf("%s: %d reviewed use(s) in %s, %d found", mode, n, fn, got[fn])
		}
	}
}

// Closing review r3 F7 (SR-18), revised v0.29.91: AddOrgRepoToGroupByID
// counts toward repo_links_per_day unless an administrator decided the
// registration (orgLinksCounted), and AddOperatorRepoToGroupByID never
// counts (the operator's own command decided). Only reviewed
// organization-driven sites may call the first, and only the operator's
// loader the second: a new caller — a user-facing handler above all —
// fails here until reviewed.
func TestOrgLinkDoorHasOnlyReviewedCallers(t *testing.T) {
	reviewed := map[string]map[string]int{
		".AddOrgRepoToGroupByID(": {
			srctest.SiteKey("internal/scheduler", "refreshGitHubOrg"):   1, // the GitHub org scan
			srctest.SiteKey("internal/scheduler", "refreshGitLabGroup"): 1, // the GitLab group scan
			srctest.SiteKey("internal/scheduler", "refreshUserOrgs"):    1, // the demand scan
			srctest.SiteKey("internal/web", "scanOrgRepos"):             2, // existing and new repositories (stage 1 deletes it)
			srctest.SiteKey("cmd/aveloxis", "runAddRepo"):               1, // add-repo's org expansion
		},
		".AddOperatorRepoToGroupByID(": {
			srctest.SiteKey("cmd/aveloxis", "runImportFoundations"): 1, // the foundation loader's dashboard groups (an operator command)
		},
	}
	root := srctest.Root(t)
	got := map[string]map[string]int{}
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
			src := srctest.StripGoComments(string(b))
			rel, _ := filepath.Rel(root, path)
			// Keyed dir|func (srctest.SiteKey, v0.29.98): a split of the
			// file holding a door does not churn the registry.
			for name, part := range srctest.FuncParts(t, src) {
				for door := range reviewed {
					if n := strings.Count(part, door); n > 0 {
						if got[door] == nil {
							got[door] = map[string]int{}
						}
						got[door][srctest.SiteKey(srctest.SiteDir(filepath.ToSlash(rel)), name)] += n
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files examined", examined, 300)
	for door, want := range reviewed {
		for f, n := range got[door] {
			if want[f] != n {
				t.Errorf("%s calls %s %d time(s); reviewed %d — an organization or operator door decides the daily additions quota differently: use AddRepoToGroupByID, or review the site and add it here with the reason", f, door, n, want[f])
			}
		}
		for f, n := range want {
			if got[door][f] != n {
				t.Errorf("%s: %d reviewed %s call(s), %d found — update the list", f, n, door, got[door][f])
			}
		}
	}
}

// v0.29.91 review round 2 (R2-5), round 3 (R3-3): the exported doors are
// pinned above, but a new caller could pass the mode itself to
// linkWithinCap. Each organization or operator mode is used at exactly one
// reviewed site, counted as identifiers (go/ast): linkOrg in
// AddOrgRepoToGroupByID, linkOrgFill in ReconcileOrgRepoLinks,
// linkOperator in AddOperatorRepoToGroupByID.
func TestOrgAndOperatorModesHaveOneCallerEach(t *testing.T) {
	assertModeUses(t, "linkOrg", map[string]int{"AddOrgRepoToGroupByID": 1})
	assertModeUses(t, "linkOrgFill", map[string]int{"ReconcileOrgRepoLinks": 1})
	assertModeUses(t, "linkOperator", map[string]int{"AddOperatorRepoToGroupByID": 1})
}
