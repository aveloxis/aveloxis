package db

import (
	"os"
	"strings"
	"testing"
)

// TestLinkContributorDetectsRenameAndMerges pins the v0.20.2 multi-row
// detection in LinkContributorToGitHubUser. When search-resolve finds
// the (login, ghUserID) it's about to link, and a DIFFERENT contributor
// row already holds either the gh_user_id or the cntrb_login, that's
// the rename edge case (R3 in `docs/architecture/contributor-resolution.md`).
//
// Pre-v0.20.2, search-resolve just updated the row it knew about and
// left the duplicate intact — two contributor rows for one person.
// v0.20.2 detects the multi-row case, picks a winner, and marks the
// loser(s) `cntrb_deleted = 1`. R2 (identity-key immutability) and
// R10 (FK integrity) are preserved: cntrb_id never changes, child
// rows still resolve.
func TestLinkContributorDetectsRenameAndMerges(t *testing.T) {
	data, err := os.ReadFile("contributor_search_resolve.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The merge logic may live in LinkContributorToGitHubUser
	// directly OR in a helper called from it (loadMergeCandidates).
	// Pin behavior across the whole file rather than just the
	// LinkContributorToGitHubUser body.
	if !strings.Contains(src, "func (s *PostgresStore) LinkContributorToGitHubUser(") {
		t.Fatal("cannot find LinkContributorToGitHubUser")
	}
	if !strings.Contains(src, "gh_user_id = $") || !strings.Contains(src, "cntrb_login = $") {
		t.Error("contributor_search_resolve.go must SELECT candidate rows " +
			"matching `gh_user_id = $... OR cntrb_login = $...` to detect " +
			"the rename edge case before linking. Without this, two rows " +
			"for the same person continue to coexist.")
	}
	if !strings.Contains(src, "cntrb_deleted = 1") {
		t.Error("contributor_search_resolve.go must mark loser rows " +
			"`cntrb_deleted = 1` after copying their non-empty fields " +
			"into the winner. Per R2 (identity-key immutability) the " +
			"row itself stays — only the deleted flag changes.")
	}
}

// TestPickMergeWinnerHelperExists pins the v0.20.2 winner-selection
// helper. The winner is preferred to be the row whose cntrb_id matches
// PlatformUUID(1, ghUserID) — i.e., the deterministic-UUID row per R1.
// If no such row exists, the older row (smallest data_collection_date)
// wins, since it's been referenced longer by FK columns.
func TestPickMergeWinnerHelperExists(t *testing.T) {
	data, err := os.ReadFile("contributor_search_resolve.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "pickMergeWinner") {
		t.Error("contributor_search_resolve.go must define a pickMergeWinner " +
			"helper that selects the deterministic-UUID row when present " +
			"(per R1), falling back to the oldest row otherwise.")
	}
}

// TestLookupReadPathsFilterDeletedContributors pins that every "find
// the active contributor for this email/login" read path filters
// `cntrb_deleted = 0` (or its IS DISTINCT FROM 1 equivalent). Without
// these filters, a merged-loser row would still be returned by lookup
// queries, defeating the v0.20.2 deduplication.
func TestLookupReadPathsFilterDeletedContributors(t *testing.T) {
	cases := []struct {
		file    string
		funcSig string
		hint    string
	}{
		{
			file:    "commit_resolver_store.go",
			funcSig: "func (s *PostgresStore) FindLoginByEmail(",
			hint:    "FindLoginByEmail must skip deleted contributors when matching by email.",
		},
		{
			file:    "commit_resolver_store.go",
			funcSig: "func (s *PostgresStore) FindContributorIDByLogin(",
			hint:    "FindContributorIDByLogin must skip deleted contributors when matching by gh_login.",
		},
		{
			file:    "commit_resolver_store.go",
			funcSig: "func (s *PostgresStore) GetContributorsMissingCanonical(",
			hint:    "GetContributorsMissingCanonical must skip deleted rows so we don't re-enrich them.",
		},
		{
			file:    "contributors.go",
			funcSig: "func (r *ContributorResolver) GetThinContributorLogins(",
			hint:    "GetThinContributorLogins must skip deleted rows so the periodic enrichment ticker doesn't waste API calls on merged-loser rows.",
		},
		{
			file:    "contributor_search_resolve.go",
			funcSig: "func (s *PostgresStore) GetContributorsNeedingSearch(",
			hint:    "GetContributorsNeedingSearch must skip deleted rows so search-resolve doesn't re-attempt them.",
		},
	}
	for _, tc := range cases {
		t.Run(tc.file+":"+tc.funcSig, func(t *testing.T) {
			data, err := os.ReadFile(tc.file)
			if err != nil {
				t.Fatal(err)
			}
			src := string(data)
			fnIdx := strings.Index(src, tc.funcSig)
			if fnIdx < 0 {
				t.Fatalf("cannot find %q in %s", tc.funcSig, tc.file)
			}
			rest := src[fnIdx:]
			end := strings.Index(rest[1:], "\nfunc ")
			var body string
			if end < 0 {
				body = rest
			} else {
				body = rest[:end+1]
			}
			if !strings.Contains(body, "cntrb_deleted") {
				t.Error(tc.hint + " — add a WHERE clause filtering " +
					"`cntrb_deleted = 0` (or COALESCE(cntrb_deleted, 0) = 0 " +
					"for legacy rows where the column might be NULL).")
			}
		})
	}
}

// TestResolveLookupByLoginFiltersDeleted pins the v0.20.2 addition to
// ContributorResolver.Resolve's lookup-by-login step. If a merged-loser
// row's cntrb_login still matches an incoming UserRef's login (the
// rename happened to swap login strings), Resolve would otherwise
// return the loser cntrb_id. Filtering on cntrb_deleted = 0 ensures
// it returns the winner instead.
func TestResolveLookupByLoginFiltersDeleted(t *testing.T) {
	data, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// Find the lookup-by-login query inside Resolve. It's the SELECT
	// from contributors WHERE cntrb_login = $1 added in v0.19.2.
	idx := strings.Index(src, "WHERE cntrb_login = $1")
	if idx < 0 {
		t.Fatal("cannot find Resolve's lookup-by-login query")
	}
	// Look ~250 chars before+after for the deleted filter.
	start := max(idx-200, 0)
	end := min(idx+200, len(src))
	window := src[start:end]
	if !strings.Contains(window, "cntrb_deleted") {
		t.Error("ContributorResolver.Resolve's lookup-by-login query " +
			"must filter cntrb_deleted = 0 so a merged-loser row's " +
			"login doesn't shadow the active winner. Per v0.20.2 R3 " +
			"merge semantics.")
	}
}

// TestRenameMergeDocumentedInArchitectureDoc pins that the public
// contract doc reflects the v0.20.2 soft-delete merge semantics. R3
// previously documented the rename edge case as an "intentional
// limitation"; v0.20.2 lifts that limitation and the doc must explain
// the new logical-merge behavior.
func TestRenameMergeDocumentedInArchitectureDoc(t *testing.T) {
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatal(err)
	}
	doc := strings.ToLower(string(data))
	if !strings.Contains(doc, "cntrb_deleted") {
		t.Error("docs/architecture/contributor-resolution.md must explain " +
			"`cntrb_deleted` semantics — when it gets set, what it means, " +
			"how operators query around it. Per Phase D / v0.20.2.")
	}
	if !strings.Contains(doc, "soft delete") && !strings.Contains(doc, "logical merge") {
		t.Error("docs/architecture/contributor-resolution.md must describe " +
			"the v0.20.2 rename merge as a logical-merge / soft-delete " +
			"operation, distinguishing it from physical row deletion.")
	}
}
