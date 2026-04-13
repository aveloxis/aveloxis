package db

import (
	"os"
	"strings"
	"testing"
)

// TestGetContributorsMissingCanonicalHasLimit verifies the query has a LIMIT
// clause to prevent unbounded API calls in ResolveEmailsToCanonical.
// Without a limit, every contributor with gh_login but no canonical email
// is queried — thousands of API calls per pass, many for users with private
// emails that will never return data.
func TestGetContributorsMissingCanonicalHasLimit(t *testing.T) {
	src, err := os.ReadFile("commit_resolver_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the GetContributorsMissingCanonical function.
	idx := strings.Index(code, "func (s *PostgresStore) GetContributorsMissingCanonical(")
	if idx < 0 {
		t.Fatal("cannot find GetContributorsMissingCanonical function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	// Must have a LIMIT clause.
	if !strings.Contains(fnBody, "LIMIT") {
		t.Error("GetContributorsMissingCanonical must have a LIMIT clause to prevent " +
			"unbounded API calls for canonical email resolution. Without this, users " +
			"with private emails get re-queried every pass with no bound.")
	}
}

// TestGetContributorsMissingCanonicalExcludesRecentlyEnriched verifies
// the query excludes contributors that were recently enriched (and therefore
// already had their canonical email set if available).
func TestGetContributorsMissingCanonicalExcludesRecentlyEnriched(t *testing.T) {
	src, err := os.ReadFile("commit_resolver_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (s *PostgresStore) GetContributorsMissingCanonical(")
	if idx < 0 {
		t.Fatal("cannot find GetContributorsMissingCanonical function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	// Must exclude recently enriched contributors to avoid duplicate
	// GET /users/{login} calls for users whose canonical was already
	// attempted by EnrichThinContributors.
	if !strings.Contains(fnBody, "cntrb_last_enriched_at") {
		t.Error("GetContributorsMissingCanonical must filter on cntrb_last_enriched_at " +
			"to skip contributors already enriched (where canonical was already attempted)")
	}
}

// TestCanonicalBatchSizeExists verifies a batch size constant exists for
// canonical email resolution.
func TestCanonicalBatchSizeExists(t *testing.T) {
	src, err := os.ReadFile("commit_resolver_store.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "CanonicalBatchSize") {
		t.Error("commit_resolver_store.go must define CanonicalBatchSize to limit " +
			"the number of canonical email resolutions per pass")
	}
}

// TestResolveEmailsToCanonicalMarksEnriched verifies that
// ResolveEmailsToCanonical marks contributors after attempting enrichment.
func TestResolveEmailsToCanonicalMarksEnriched(t *testing.T) {
	src, err := os.ReadFile("../collector/commit_resolver.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (r *CommitResolver) ResolveEmailsToCanonical(")
	if idx < 0 {
		t.Fatal("cannot find ResolveEmailsToCanonical function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	if !strings.Contains(fnBody, "MarkContributorEnriched") {
		t.Error("ResolveEmailsToCanonical must call MarkContributorEnriched after " +
			"attempting canonical resolution to prevent re-querying users with private emails")
	}
}
