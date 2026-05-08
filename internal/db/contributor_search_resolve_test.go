// Source-contract tests for v0.19.2 Fix #4: the search-resolve
// background task that takes contributors with cntrb_email !=  ''
// AND gh_user_id IS NULL, calls GitHub /search/users?q=email, and
// upserts gh_user_id onto the existing row WITHOUT changing
// cntrb_id or cntrb_login.
//
// This is the offline / rate-limited path that converts random-UUID
// lazy rows into rows with proper platform identity over time. Runs
// on a scheduler ticker (default 1 hour) at controlled rate.

package db

import (
	"strings"
	"testing"
)

// TestSchemaAddsLastSearchAttemptedColumn pins the new audit column
// that lets the task skip rows it has already tried (and found no
// match for, or that errored).
func TestSchemaAddsLastSearchAttemptedColumn(t *testing.T) {
	src := mustReadStoreSource(t, "migrate.go")
	if !strings.Contains(src, `"aveloxis_data.contributors", "cntrb_last_search_attempted_at"`) {
		t.Error("migrate.go must add cntrb_last_search_attempted_at TIMESTAMPTZ to contributors so the " +
			"search-resolve background task knows which rows to skip — without this, every cycle would " +
			"re-search the same email addresses indefinitely.")
	}
}

// TestGetContributorsNeedingSearchExists pins the store method.
func TestGetContributorsNeedingSearchExists(t *testing.T) {
	src := mustReadStoreSource(t, "contributor_search_resolve.go")
	if !strings.Contains(src, "func (s *PostgresStore) GetContributorsNeedingSearch(") {
		t.Error("contributor_search_resolve.go must define GetContributorsNeedingSearch(ctx, limit) — " +
			"returns rows with cntrb_email != '' AND gh_user_id IS NULL AND " +
			"(cntrb_last_search_attempted_at IS NULL OR < cooldown).")
	}
}

// TestGetContributorsNeedingSearchExcludesNoreply pins that the
// query filters out noreply emails (users.noreply.github.com etc.).
// Searching those is guaranteed to fail and wastes the rate limit.
func TestGetContributorsNeedingSearchExcludesNoreply(t *testing.T) {
	src := mustReadStoreSource(t, "contributor_search_resolve.go")
	body := extractBatchFunc(src, "GetContributorsNeedingSearch")
	if body == "" {
		t.Skip("GetContributorsNeedingSearch not yet defined")
	}
	if !strings.Contains(body, "noreply") {
		t.Error("GetContributorsNeedingSearch must filter out noreply email addresses — they're " +
			"guaranteed search misses and they waste the 30/min/token search quota.")
	}
}

// TestLinkContributorToGitHubUserExists pins the store method that
// applies the search hit. Critical contract: it must NOT change
// cntrb_id or cntrb_login on the existing row — only fill in
// gh_user_id and gh_login.
func TestLinkContributorToGitHubUserExists(t *testing.T) {
	src := mustReadStoreSource(t, "contributor_search_resolve.go")
	if !strings.Contains(src, "func (s *PostgresStore) LinkContributorToGitHubUser(") {
		t.Error("contributor_search_resolve.go must define LinkContributorToGitHubUser(ctx, cntrbID, login, ghUserID)")
	}
}

// TestLinkContributorToGitHubUserDoesNotChangeCntrbIdOrLogin is the
// load-bearing invariant: the function must NOT modify cntrb_id or
// cntrb_login — those would either invalidate FK references or
// trip idx_contributors_login.
func TestLinkContributorToGitHubUserDoesNotChangeCntrbIdOrLogin(t *testing.T) {
	src := mustReadStoreSource(t, "contributor_search_resolve.go")
	body := extractBatchFunc(src, "LinkContributorToGitHubUser")
	if body == "" {
		t.Skip("LinkContributorToGitHubUser not yet defined")
	}

	// Extract just the SQL strings so doc comments mentioning the
	// columns don't trip the check.
	for _, sql := range allBackticks(body) {
		// SET cntrb_login is forbidden — would re-enter the very
		// collision class this fix is meant to avoid.
		if strings.Contains(sql, "SET cntrb_login") || strings.Contains(sql, "cntrb_login =") {
			// Allow `cntrb_login = ` only inside WHERE clauses, not SET.
			// Heuristic: if "SET" appears before a "cntrb_login =" reference, fail.
			setIdx := strings.Index(sql, "SET ")
			whereIdx := strings.Index(sql, "WHERE")
			loginIdx := strings.Index(sql, "cntrb_login")
			if setIdx >= 0 && loginIdx >= 0 && (whereIdx < 0 || loginIdx < whereIdx) {
				t.Errorf("LinkContributorToGitHubUser SQL must NOT update cntrb_login. SQL: %s", strings.TrimSpace(sql))
			}
		}
		// SET cntrb_id is forbidden — would orphan FK references in
		// 16+ child tables.
		if strings.Contains(sql, "SET cntrb_id") {
			t.Errorf("LinkContributorToGitHubUser SQL must NOT update cntrb_id. SQL: %s", strings.TrimSpace(sql))
		}
	}
}

// TestLinkContributorToGitHubUserStampsAttempted pins that even on
// success, the function stamps cntrb_last_search_attempted_at so the
// row is excluded from the next cycle's batch.
func TestLinkContributorToGitHubUserStampsAttempted(t *testing.T) {
	src := mustReadStoreSource(t, "contributor_search_resolve.go")
	body := extractBatchFunc(src, "LinkContributorToGitHubUser")
	if body == "" {
		t.Skip("LinkContributorToGitHubUser not yet defined")
	}
	if !strings.Contains(body, "cntrb_last_search_attempted_at") {
		t.Error("LinkContributorToGitHubUser must SET cntrb_last_search_attempted_at = NOW() so the row is " +
			"excluded from the next cycle's GetContributorsNeedingSearch batch")
	}
}

// TestMarkContributorSearchAttemptedExists pins the store method
// the task calls when search returns no hit (or errors). Without
// this, the same emails would be re-searched every cycle.
func TestMarkContributorSearchAttemptedExists(t *testing.T) {
	src := mustReadStoreSource(t, "contributor_search_resolve.go")
	if !strings.Contains(src, "func (s *PostgresStore) MarkContributorSearchAttempted(") {
		t.Error("contributor_search_resolve.go must define MarkContributorSearchAttempted(ctx, cntrbID) " +
			"for the no-hit / error path so the row is excluded from future batches until the cooldown.")
	}
}

// allBackticks returns every backtick-delimited string in the source.
// Used for SQL extraction without false positives from doc comments.
func allBackticks(src string) []string {
	var out []string
	for {
		start := strings.Index(src, "`")
		if start < 0 {
			break
		}
		rest := src[start+1:]
		end := strings.Index(rest, "`")
		if end < 0 {
			break
		}
		out = append(out, rest[:end])
		src = rest[end+1:]
	}
	return out
}
