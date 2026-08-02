// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// ============================================================
// Source-contract: both store methods exist + share the CTE
// ============================================================

// TestContributionsStoreMethodsExist pins the public API of the
// contributions endpoints' store layer. A future rename or removal
// fails the build before merge. Covers v0.23.10 identities +
// affiliations and v0.23.11 coverage.
func TestContributionsStoreMethodsExist(t *testing.T) {
	body, err := os.ReadFile("contributions.go")
	if err != nil {
		t.Fatalf("read contributions.go: %v", err)
	}
	src := string(body)
	for _, needle := range []string{
		"func (s *PostgresStore) GetRepoContributors(",
		"func (s *PostgresStore) GetRepoAffiliationCounts(",
		"func (s *PostgresStore) GetRepoContributionsCoverage(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("contributions.go missing %q", needle)
		}
	}
}

// TestContributionsCoverageStructHasAllFields pins the response shape.
// If a future refactor renames a field or drops one, the integration
// test stops catching the regression — this source-contract pin
// catches it earlier. Fields listed match docs/guide/api.md.
func TestContributionsCoverageStructHasAllFields(t *testing.T) {
	body, err := os.ReadFile("contributions.go")
	if err != nil {
		t.Fatalf("read contributions.go: %v", err)
	}
	src := string(body)
	for _, needle := range []string{
		`"total_contributors"`,
		`"enriched"`,
		`"canonical_email"`,
		`"gh_user_id_resolved"`,
		`"search_resolve_attempted"`,
		`"breadth_attempted"`,
		`"affiliation_resolved"`,
		`"affiliation_unknown"`,
		`"enrichment_oldest_pending,omitempty"`,
		`"enrichment_stalest,omitempty"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("ContributionsCoverage struct missing JSON tag %s", needle)
		}
	}
}

// TestContributionsShareCTE pins that the two methods both use the
// contributorsInWindowCTE constant. Centralizing the contribution
// definition is load-bearing: if the contributors endpoint and the
// affiliations endpoint ever disagree on "what counts as a
// contribution," a per-affiliation total will differ from the
// contributor count for unobvious reasons. This test forces a future
// refactor to either keep using the constant or update both call sites
// at the same time.
func TestContributionsShareCTE(t *testing.T) {
	body, err := os.ReadFile("contributions.go")
	if err != nil {
		t.Fatalf("read contributions.go: %v", err)
	}
	src := string(body)
	if strings.Count(src, "contributorsInWindowCTE") < 4 {
		// 1 declaration + 3 use sites (GetRepoContributors,
		// GetRepoAffiliationCounts, GetRepoContributionsCoverage) = at
		// least 4 occurrences. If a future refactor splits the CTE or
		// drops a use site, the endpoints can silently drift on what
		// counts as a "contribution" — fail the build before merge.
		t.Error("contributions.go must reference contributorsInWindowCTE in all three " +
			"endpoint store methods so the three endpoints can't drift on " +
			"contribution-kind coverage")
	}
}

// TestContributionsCTECoversAllKinds pins that every contribution kind
// the docs say is included actually appears in the SQL. This is the
// counterpart to the doc that operators read — if the doc says
// "commits, issues opened, issues closed, issue events, PRs opened, PR
// reviews, PR events, messages" the SQL had better filter on all of
// them or the count is wrong.
func TestContributionsCTECoversAllKinds(t *testing.T) {
	body, err := os.ReadFile("contributions.go")
	if err != nil {
		t.Fatalf("read contributions.go: %v", err)
	}
	src := string(body)
	for _, needle := range []string{
		"aveloxis_data.commits",
		"aveloxis_data.issues",
		"aveloxis_data.issue_events",
		"aveloxis_data.pull_requests",
		"aveloxis_data.pull_request_reviews",
		"aveloxis_data.pull_request_events",
		"aveloxis_data.messages",
		"cmt_author_timestamp",
		"reporter_id",
		"closed_by_id",
		"author_id",
		"submitted_at",
		"msg_timestamp",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("contributions.go CTE missing required token %q "+
				"(docs/guide/api.md claims this kind is included)", needle)
		}
	}
}

// TestContributionsCTEExcludesSoftDeletedContributors pins the v0.20.2
// soft-merge contract: loser rows (cntrb_deleted != 0) must not surface
// in either endpoint or analytics will double-count merged identities.
func TestContributionsCTEExcludesSoftDeletedContributors(t *testing.T) {
	body, err := os.ReadFile("contributions.go")
	if err != nil {
		t.Fatalf("read contributions.go: %v", err)
	}
	if !strings.Contains(string(body), "COALESCE(c.cntrb_deleted, 0) = 0") {
		t.Error("both endpoints must filter cntrb_deleted = 0 to honor the v0.20.2 " +
			"soft-merge contract")
	}
}

// ============================================================
// Integration tier — live Postgres, gated on AVELOXIS_TEST_DB
// ============================================================

func contributionsConnect(t *testing.T) (*PostgresStore, context.Context) {
	t.Helper()
	conn := os.Getenv("AVELOXIS_TEST_DB")
	if conn == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, conn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	return store, ctx
}

func seedContribRepo(ctx context.Context, t *testing.T, store *PostgresStore) int64 {
	t.Helper()
	slug := fmt.Sprintf("_avcontrib-%d", time.Now().UnixNano())
	id, err := store.UpsertRepo(ctx, &model.Repo{
		Owner:    "_avcontrib",
		Name:     slug,
		GitURL:   fmt.Sprintf("https://github.com/_avcontrib/%s", slug),
		Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	return id
}

// insertContributor inserts a minimal contributor row and returns its
// cntrb_id. cntrb_canonical drives affiliation lookup, so callers
// override it when they want a specific email domain.
func insertContributor(ctx context.Context, t *testing.T, store *PostgresStore,
	login, canonical, company string) string {
	t.Helper()
	var id string
	err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors
			(cntrb_login, cntrb_canonical, cntrb_company)
		VALUES ($1, $2, $3)
		ON CONFLICT (cntrb_login) WHERE cntrb_login != ''
		DO UPDATE SET cntrb_canonical = EXCLUDED.cntrb_canonical,
		              cntrb_company = EXCLUDED.cntrb_company
		RETURNING cntrb_id::text`,
		login, canonical, company).Scan(&id)
	if err != nil {
		t.Fatalf("insertContributor(%s): %v", login, err)
	}
	return id
}

// TestGetRepoContributors_FiltersByWindow inserts two issues by the same
// contributor — one inside the window, one outside — and asserts that
// the contributor is returned exactly once (DISTINCT works) and that
// out-of-window contributors don't leak in.
func TestGetRepoContributors_FiltersByWindow(t *testing.T) {
	store, ctx := contributionsConnect(t)
	t.Cleanup(store.Close)

	repoID := seedContribRepo(ctx, t, store)
	insider := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_in_%d", time.Now().UnixNano()),
		"insider@example.com", "")
	outsider := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_out_%d", time.Now().UnixNano()),
		"outsider@example.com", "")

	insideTime := time.Now().AddDate(0, -3, 0)  // 3 months ago
	outsideTime := time.Now().AddDate(-5, 0, 0) // 5 years ago

	// Two issues by insider (test DISTINCT) + one by outsider.
	for i, when := range []time.Time{insideTime, insideTime.Add(time.Hour)} {
		_, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issues
				(repo_id, platform_issue_id, issue_number, reporter_id, created_at)
			VALUES ($1, $2, $3, $4::uuid, $5)`,
			repoID, time.Now().UnixNano()+int64(i), i+1, insider, when)
		if err != nil {
			t.Fatalf("seed inside issue %d: %v", i, err)
		}
	}
	_, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues
			(repo_id, platform_issue_id, issue_number, reporter_id, created_at)
		VALUES ($1, $2, 999, $3::uuid, $4)`,
		repoID, time.Now().UnixNano()+999, outsider, outsideTime)
	if err != nil {
		t.Fatalf("seed outside issue: %v", err)
	}

	// Window: last 2 years. insider should be in, outsider out.
	since := time.Now().AddDate(-2, 0, 0)
	got, err := store.GetRepoContributors(ctx, repoID, since, time.Time{})
	if err != nil {
		t.Fatalf("GetRepoContributors: %v", err)
	}

	ids := map[string]bool{}
	for _, c := range got {
		ids[c.CntrbID] = true
	}
	if !ids[insider] {
		t.Errorf("insider %s should be in result; got %d contributors: %+v",
			insider, len(got), got)
	}
	if ids[outsider] {
		t.Errorf("outsider %s should be filtered out by window", outsider)
	}
}

// TestGetRepoAffiliationCounts_DerivationPriority pins the documented
// affiliation derivation order: contributor_affiliations[domain] beats
// cntrb_company beats "(unknown)".
func TestGetRepoAffiliationCounts_DerivationPriority(t *testing.T) {
	store, ctx := contributionsConnect(t)
	t.Cleanup(store.Close)

	repoID := seedContribRepo(ctx, t, store)

	// Three contributors with deliberately different signal strengths:
	// 1. domain-mapped: canonical email under a domain we'll insert
	//    into contributor_affiliations.
	// 2. profile-only: cntrb_company set but canonical email under an
	//    unmapped domain.
	// 3. neither: no canonical, no company.
	suffix := time.Now().UnixNano()
	domain := fmt.Sprintf("_avtest-%d.org", suffix)
	mapped := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_mapped_%d", suffix),
		"alice@"+domain, "WrongCompanyShouldBeOverridden")
	profileOnly := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_profile_%d", suffix),
		"bob@somewhere-else-"+domain, "@SomeOrg")
	noneCntrb := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_none_%d", suffix), "", "")

	// Map the domain to "TestCorp" in contributor_affiliations.
	_, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_affiliations
			(ca_domain, ca_affiliation, ca_active)
		VALUES ($1, 'TestCorp', 1)
		ON CONFLICT (ca_domain) DO UPDATE SET
			ca_affiliation = EXCLUDED.ca_affiliation,
			ca_active = 1`, domain)
	if err != nil {
		t.Fatalf("seed affiliation: %v", err)
	}

	inWindow := time.Now().AddDate(0, -1, 0)
	for i, cntrb := range []string{mapped, profileOnly, noneCntrb} {
		_, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issues
				(repo_id, platform_issue_id, issue_number, reporter_id, created_at)
			VALUES ($1, $2, $3, $4::uuid, $5)`,
			repoID, time.Now().UnixNano()+int64(i*7919), 100+i, cntrb, inWindow)
		if err != nil {
			t.Fatalf("seed issue %d: %v", i, err)
		}
	}

	since := time.Now().AddDate(-2, 0, 0)
	got, err := store.GetRepoAffiliationCounts(ctx, repoID, since, time.Time{})
	if err != nil {
		t.Fatalf("GetRepoAffiliationCounts: %v", err)
	}

	byAffil := map[string]int{}
	for _, ac := range got {
		byAffil[ac.Affiliation] = ac.ContributorCount
	}

	if byAffil["TestCorp"] < 1 {
		t.Errorf("TestCorp should have at least 1 contributor (domain-mapped); got %+v", byAffil)
	}
	if byAffil["SomeOrg"] < 1 {
		t.Errorf("SomeOrg should have at least 1 contributor (profile, @-stripped); got %+v", byAffil)
	}
	if byAffil["(unknown)"] < 1 {
		t.Errorf("(unknown) bucket should hold the no-info contributor; got %+v", byAffil)
	}
}

// TestGetRepoContributionsCoverage_ExerciseAllFields seeds a small
// cohort whose enrichment state is deliberately varied so every
// coverage field has a non-default value to assert against. Catches
// regressions like "SELECT column position swapped" or "FILTER
// predicate flipped" that the source-contract pin can't see.
func TestGetRepoContributionsCoverage_ExerciseAllFields(t *testing.T) {
	store, ctx := contributionsConnect(t)
	t.Cleanup(store.Close)

	repoID := seedContribRepo(ctx, t, store)

	// Three contributors with distinct enrichment states:
	//   alice: fully enriched + canonical + gh_user_id + breadth +
	//          search-attempted + domain affiliation. Hits every field.
	//   bob:   profile-only company "@SomeOrg", no canonical email,
	//          no enrichment markers. Hits affiliation_resolved but
	//          NOT enriched/canonical_email/gh_user_id.
	//   carol: nothing populated. Counts toward total only.
	suffix := time.Now().UnixNano()
	domain := fmt.Sprintf("_avcov-%d.org", suffix)

	alice := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_alice_%d", suffix),
		"alice@"+domain, "")
	bob := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_bob_%d", suffix), "", "@SomeOrg")
	carol := insertContributor(ctx, t, store,
		fmt.Sprintf("_av_carol_%d", suffix), "", "")

	// Map the domain so alice's canonical resolves.
	_, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_affiliations
			(ca_domain, ca_affiliation, ca_active)
		VALUES ($1, 'AliceCorp', 1)
		ON CONFLICT (ca_domain) DO UPDATE SET ca_affiliation = EXCLUDED.ca_affiliation`,
		domain)
	if err != nil {
		t.Fatalf("seed affiliation: %v", err)
	}

	// Populate alice's enrichment markers + gh_user_id.
	stampAt := time.Now().AddDate(0, 0, -2) // 2 days ago
	_, err = store.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors
		SET cntrb_last_enriched_at = $2,
		    cntrb_last_search_attempted_at = $2,
		    cntrb_last_breadth_at = $2,
		    gh_user_id = $3
		WHERE cntrb_id = $1::uuid`,
		alice, stampAt, int64(12345))
	if err != nil {
		t.Fatalf("populate alice enrichment: %v", err)
	}

	// All three contribute via an issue inside the window.
	inWindow := time.Now().AddDate(0, -1, 0)
	for i, cntrb := range []string{alice, bob, carol} {
		_, err := store.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.issues
				(repo_id, platform_issue_id, issue_number, reporter_id, created_at)
			VALUES ($1, $2, $3, $4::uuid, $5)`,
			repoID, time.Now().UnixNano()+int64(i*1009), 100+i, cntrb, inWindow)
		if err != nil {
			t.Fatalf("seed issue %d: %v", i, err)
		}
	}

	since := time.Now().AddDate(-2, 0, 0)
	cov, err := store.GetRepoContributionsCoverage(ctx, repoID, since, time.Time{})
	if err != nil {
		t.Fatalf("GetRepoContributionsCoverage: %v", err)
	}

	if cov.TotalContributors != 3 {
		t.Errorf("total = %d, want 3", cov.TotalContributors)
	}
	if cov.Enriched != 1 {
		t.Errorf("enriched = %d, want 1 (only alice)", cov.Enriched)
	}
	if cov.CanonicalEmail != 1 {
		t.Errorf("canonical_email = %d, want 1 (only alice)", cov.CanonicalEmail)
	}
	if cov.GHUserIDResolved != 1 {
		t.Errorf("gh_user_id_resolved = %d, want 1 (only alice)", cov.GHUserIDResolved)
	}
	if cov.SearchResolveAttempted != 1 {
		t.Errorf("search_resolve_attempted = %d, want 1 (only alice)", cov.SearchResolveAttempted)
	}
	if cov.BreadthAttempted != 1 {
		t.Errorf("breadth_attempted = %d, want 1 (only alice)", cov.BreadthAttempted)
	}
	if cov.AffiliationResolved != 2 {
		t.Errorf("affiliation_resolved = %d, want 2 (alice via domain + bob via @SomeOrg)",
			cov.AffiliationResolved)
	}
	if cov.AffiliationUnknown != 1 {
		t.Errorf("affiliation_unknown = %d, want 1 (only carol); derived = total - resolved",
			cov.AffiliationUnknown)
	}
	// EnrichmentOldestPending: oldest data_collection_date among
	// contributors with NULL cntrb_last_enriched_at. Bob + carol qualify;
	// pgx populates data_collection_date with NOW() at insert. So we just
	// assert it's non-nil and within the last minute.
	if cov.EnrichmentOldestPending == nil {
		t.Error("EnrichmentOldestPending should be non-nil — bob and carol are unenriched")
	} else if time.Since(*cov.EnrichmentOldestPending) > time.Minute {
		t.Errorf("EnrichmentOldestPending = %v, expected within last minute",
			*cov.EnrichmentOldestPending)
	}
	// EnrichmentStalest: oldest cntrb_last_enriched_at among the cohort.
	// Only alice was enriched, and we stamped her at -2 days, so this
	// should equal stampAt (within timestamp microsecond precision).
	if cov.EnrichmentStalest == nil {
		t.Error("EnrichmentStalest should be non-nil — alice was enriched")
	} else if diff := cov.EnrichmentStalest.Sub(stampAt); diff > time.Second || diff < -time.Second {
		t.Errorf("EnrichmentStalest = %v, want ~%v (stampAt)", *cov.EnrichmentStalest, stampAt)
	}
}

// TestGetRepoContributionsCoverage_EmptyCohort pins the
// no-contributors case: zero counts everywhere, nil timestamp pointers
// so the JSON omits the fields entirely rather than emitting zero-time.
func TestGetRepoContributionsCoverage_EmptyCohort(t *testing.T) {
	store, ctx := contributionsConnect(t)
	t.Cleanup(store.Close)

	repoID := seedContribRepo(ctx, t, store)

	since := time.Now().AddDate(-2, 0, 0)
	cov, err := store.GetRepoContributionsCoverage(ctx, repoID, since, time.Time{})
	if err != nil {
		t.Fatalf("GetRepoContributionsCoverage: %v", err)
	}

	if cov.TotalContributors != 0 {
		t.Errorf("total = %d, want 0 for empty cohort", cov.TotalContributors)
	}
	if cov.AffiliationUnknown != 0 {
		t.Errorf("affiliation_unknown should be 0 - 0 = 0; got %d", cov.AffiliationUnknown)
	}
	if cov.EnrichmentOldestPending != nil {
		t.Errorf("EnrichmentOldestPending should be nil on empty cohort, got %v",
			*cov.EnrichmentOldestPending)
	}
	if cov.EnrichmentStalest != nil {
		t.Errorf("EnrichmentStalest should be nil on empty cohort, got %v",
			*cov.EnrichmentStalest)
	}
}
