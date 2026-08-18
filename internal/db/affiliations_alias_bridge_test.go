// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// The 2026-08-16 host-OOM incident: PopulateAffiliations' second candidate
// query joined the ~500M-row commits table against contributors on
// LOWER(cmt_author_email) = LOWER(cntrb_email) with SELECT DISTINCT — an
// unindexable hash join plus a giant parallel tuplesort, every hour. On
// 2026-08-16 14:11 CDT its parallel workers exhausted the host's memory
// ("Failed on request of size 68 in memory context \"Caller tuples\""),
// the postmaster could no longer fork backends, and aveloxis-serve died
// parsing the postmaster's bare-string fork-failure error as a 1.66 GB
// protocol frame. The join was also provably zero-value: equality with
// cntrb_email means it could only return domains query 1 already fetched.
//
// The intended signal ("domains seen only in git log") is now served by the
// contributors_aliases bridge (R5): every commit-author email the resolver
// links to a contributor lands there, including emails that differ from the
// profile email. These tests pin the replacement.

// TestPopulateAffiliationsNeverTouchesCommitsTable is the incident tripwire:
// affiliations_populate.go must never reference aveloxis_data.commits again.
// Comment-stripped so the incident documentation in the source can name the
// banned table without false-matching.
func TestPopulateAffiliationsNeverTouchesCommitsTable(t *testing.T) {
	data, err := os.ReadFile("affiliations_populate.go")
	if err != nil {
		t.Fatalf("read affiliations_populate.go: %v", err)
	}
	src := stripLineComments(string(data))
	if strings.Contains(src, "aveloxis_data.commits") {
		t.Fatal("affiliations_populate.go references aveloxis_data.commits — " +
			"the commits-table join OOM'd the production host on 2026-08-16 " +
			"(hourly ~500M-row LOWER=LOWER hash join + parallel DISTINCT sort). " +
			"Git-log-only domains come from the contributors_aliases bridge; " +
			"a commits join is banned here")
	}
}

// TestPopulateAffiliationsUsesAliasBridge pins the replacement shape: the
// second candidate source is contributors_aliases, and BOTH candidate
// queries exclude v0.20.2 merge losers (the contract doc's R3 section has
// claimed this filter since v0.20.2; the code never had it until now).
func TestPopulateAffiliationsUsesAliasBridge(t *testing.T) {
	data, err := os.ReadFile("affiliations_populate.go")
	if err != nil {
		t.Fatalf("read affiliations_populate.go: %v", err)
	}
	src := stripLineComments(string(data))

	if !strings.Contains(src, "aveloxis_data.contributors_aliases") {
		t.Error("PopulateAffiliations must gather git-log-only domains from " +
			"aveloxis_data.contributors_aliases (the R5 bridge)")
	}
	if got := strings.Count(src, "COALESCE(cntrb_deleted, 0) = 0") +
		strings.Count(src, "COALESCE(co.cntrb_deleted, 0) = 0"); got < 2 {
		t.Errorf("expected both candidate queries to filter merge losers via "+
			"COALESCE(cntrb_deleted, 0) = 0 (found %d of 2) — the "+
			"contributor-resolution contract (R3 section) lists "+
			"PopulateAffiliations among the cntrb_deleted-filtered candidate "+
			"selections", got)
	}
	// v0.27.92 (Copilot round on PR #178): the alias scan is best-effort
	// but never SILENT — a failed Query or a mid-stream iteration error
	// must be logged, or an aliases-side outage would invisibly shrink the
	// affiliations map (the v0.27.19 silent-error-drop lesson).
	if !strings.Contains(src, "aliasRows.Err()") {
		t.Error("PopulateAffiliations must check aliasRows.Err() after the " +
			"alias loop — a mid-stream iteration failure silently drops " +
			"candidates otherwise")
	}
}

// TestPopulateAffiliationsHarvestsAliasDomainsEndToEnd proves the original
// intent is now actually served: a contributor whose PROFILE email is on a
// public provider (excluded from the map) but whose commit-alias email is on
// an institutional domain contributes that institutional domain. The
// pre-2026-08-18 code structurally could not produce this row — its commits
// join required the commit email to EQUAL the profile email.
func TestPopulateAffiliationsHarvestsAliasDomainsEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.pool.Close)

	const (
		activeLogin  = "avtest-affil-alias-active"
		loserLogin   = "avtest-affil-alias-loser"
		activeDomain = "avtest-affil-bridge.edu"
		loserDomain  = "avtest-affil-loser.edu"
	)
	cleanup := func() {
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributor_affiliations WHERE ca_domain IN ($1, $2)`, activeDomain, loserDomain)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors_aliases WHERE alias_email LIKE 'avtest-affil-%'`)
		_, _ = store.pool.Exec(ctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login IN ($1, $2)`, activeLogin, loserLogin)
	}
	cleanup()
	t.Cleanup(cleanup)

	// Active contributor: public-provider profile email (gmail — excluded
	// from the map) + a known company + an institutional commit alias.
	var activeID string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_email, cntrb_company, cntrb_deleted)
		VALUES ($1, 'avtest-affil-person@gmail.com', 'Aveloxis Test University', 0)
		RETURNING cntrb_id::text`, activeLogin).Scan(&activeID); err != nil {
		t.Fatalf("seed active contributor: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		VALUES ($1::uuid, 'avtest-affil-person@gmail.com', 'avtest-affil-person@'||$2)
		ON CONFLICT (alias_email) DO NOTHING`, activeID, activeDomain); err != nil {
		t.Fatalf("seed active alias: %v", err)
	}

	// Merge loser (cntrb_deleted = 1): its alias domain must NOT enter the map.
	var loserID string
	if err := store.pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.contributors (cntrb_login, cntrb_email, cntrb_company, cntrb_deleted)
		VALUES ($1, 'avtest-affil-loser@gmail.com', 'Aveloxis Loser Corp', 1)
		RETURNING cntrb_id::text`, loserLogin).Scan(&loserID); err != nil {
		t.Fatalf("seed loser contributor: %v", err)
	}
	if _, err := store.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors_aliases (cntrb_id, canonical_email, alias_email)
		VALUES ($1::uuid, 'avtest-affil-loser@gmail.com', 'avtest-affil-loser@'||$2)
		ON CONFLICT (alias_email) DO NOTHING`, loserID, loserDomain); err != nil {
		t.Fatalf("seed loser alias: %v", err)
	}

	if _, err := store.PopulateAffiliations(ctx); err != nil {
		t.Fatalf("PopulateAffiliations: %v", err)
	}

	var company string
	err := store.pool.QueryRow(ctx, `
		SELECT ca_affiliation FROM aveloxis_data.contributor_affiliations
		WHERE ca_domain = $1`, activeDomain).Scan(&company)
	if err != nil {
		t.Fatalf("alias-only institutional domain %q did not land in "+
			"contributor_affiliations — the aliases bridge is not being "+
			"harvested (the pre-2026-08-18 commits join could never see it): %v",
			activeDomain, err)
	}
	if company != "Aveloxis Test University" {
		t.Errorf("domain %q mapped to %q, want %q", activeDomain, company, "Aveloxis Test University")
	}

	var loserCount int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributor_affiliations
		WHERE ca_domain = $1`, loserDomain).Scan(&loserCount); err != nil {
		t.Fatalf("query loser domain: %v", err)
	}
	if loserCount != 0 {
		t.Errorf("merge-loser (cntrb_deleted=1) alias domain %q entered the "+
			"affiliations map — candidate queries must filter "+
			"COALESCE(cntrb_deleted, 0) = 0", loserDomain)
	}
}
