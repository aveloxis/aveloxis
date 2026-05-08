package db

import (
	"os"
	"strings"
	"testing"
)

// TestMigrationCreatesGhLoginIndex pins that RunMigrations creates a
// partial index on aveloxis_data.contributors(gh_login). Pre-v0.19.9
// every `SELECT cntrb_id FROM contributors WHERE gh_login = $1` was
// a full sequential scan of the contributors table — at fleet scale
// (~5M rows) and call rate (FindContributorIDByLogin fires once per
// resolved commit during commit resolution, so 25+ concurrent backends
// each scanning 5M rows = 125M tuples examined per moment). The
// pg_stat_activity snapshot captured on 2026-05-08 showed this as
// the dominant CPU consumer once v0.19.7 removed the
// PopulateAffiliations / ResolveEmailsToCanonical noise that was
// masking it.
//
// The same query is the join probe for BackfillCommitAuthorIDs at
// the end of every CommitResolver.ResolveCommits, so the index
// speeds up that UPDATE too.
//
// Partial: WHERE gh_login != '' — empty gh_login is the email-only
// contributor cohort and shouldn't bloat the index. Mirrors the
// existing idx_contributors_login partial-index pattern.
func TestMigrationCreatesGhLoginIndex(t *testing.T) {
	data, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, "idx_contributors_gh_login") {
		t.Error("migrate.go must create idx_contributors_gh_login " +
			"(partial index on contributors(gh_login) WHERE gh_login != '') " +
			"so FindContributorIDByLogin and BackfillCommitAuthorIDs use " +
			"the index instead of sequential scanning the entire " +
			"contributors table. Without it, every commit-resolution call " +
			"on a fleet-scale DB reads 5M+ rows. Discovered in the v0.19.8 " +
			"diagnostic on 2026-05-08 once v0.19.7's per-job global-state " +
			"writers were removed and this cost surfaced.")
	}

	// Pin that the index creation goes through execMigrationStep —
	// per the v0.19.4 fail-closed contract, every schema-changing
	// statement in RunMigrations must log+collect errors so a
	// permission failure surfaces instead of being swallowed.
	idxPos := strings.Index(src, "idx_contributors_gh_login")
	if idxPos < 0 {
		return
	}
	// Look back ~600 chars for the call wrapper. execMigrationStep
	// is on the line above the inline SQL string most of the time.
	lookback := idxPos - 600
	if lookback < 0 {
		lookback = 0
	}
	preceding := src[lookback:idxPos]
	if !strings.Contains(preceding, "execMigrationStep") {
		t.Error("idx_contributors_gh_login creation must be wrapped in " +
			"execMigrationStep so a permission failure (e.g., role lacking " +
			"CREATE on the schema) surfaces as a fatal migration error " +
			"per the v0.19.4 fail-closed contract instead of being " +
			"silently swallowed.")
	}

	// Pin partial-index condition. Without WHERE gh_login != '' the
	// index would also cover every email-only contributor (cntrb_id
	// generated with userID == 0, gh_login = ''), which is the
	// largest single bucket on most fleets.
	if !strings.Contains(src, "WHERE gh_login != ''") {
		t.Error("idx_contributors_gh_login must be a partial index with " +
			"WHERE gh_login != '' so the empty-login cohort (email-only " +
			"contributors with no platform identity) doesn't bloat the " +
			"index. Mirrors the existing idx_contributors_login pattern.")
	}
}
