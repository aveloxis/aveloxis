// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.126 — regression-infrastructure Phase 3: the column
// write-policy registry. The founding incident (PR #184 round 11,
// v0.27.117): UpsertRepo's ON CONFLICT and UpdateRepoMetadata both
// carried prefer-nonempty-INCOMING on platform_repo_id while the
// intended policy was fill-empty-only — a non-empty incoming ID
// overwrote a different stored one, silently erasing the round-10
// delete-and-recreate mismatch signal. Each registered column declares
// its ONE policy here; the sqlscan engine verifies every UPDATE SET
// assignment in the store corpus conforms.
//
// TRIAGE PROTOCOL for a red run (never weaken a matcher):
//  1. fix the SQL (the write really is wrong), or
//  2. correct the registered policy (the registry was wrong), or
//  3. add an Exception WITH a reason (the deviation is deliberate).
package db

import (
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// columnWritePolicies — the registry. Reasons are REQUIRED and cite the
// release that established the policy.
var columnWritePolicies = []sqlscan.Registered{
	{Table: "aveloxis_data.repos", Column: "platform_repo_id", Policy: sqlscan.FillEmptyOnly,
		Reason: "the forge numeric ID never changes for a repo; an overwrite would erase the delete-and-recreate mismatch signal (v0.27.116/117)"},
	{Table: "aveloxis_data.repos", Column: "created_at", Policy: sqlscan.FillEmptyOnly,
		Reason: "a repository's creation date is an immutable fact (v0.27.104)"},
	{Table: "aveloxis_data.repos", Column: "updated_at", Policy: sqlscan.GreatestNonNull,
		Reason: "the forge's last-update time only increases; out-of-order refreshes must not regress it (v0.27.122)"},
	{Table: "aveloxis_data.repos", Column: "added_at", Policy: sqlscan.InsertOnly,
		Reason: "fleet-entry timestamp — INSERT-only so it can never degrade into last-touched (v0.27.60)"},
	{Table: "aveloxis_data.repos", Column: "forked_from", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "an id-less re-upsert must not wipe captured lineage; empty incoming preserves stored (v0.27.78)"},
	{Table: "aveloxis_data.repos", Column: "vuln_scan_last_run", Policy: sqlscan.AlwaysRefresh,
		Reason: "completed-scan stamp — every completed vulnerability scan re-stamps NOW(); never written on error paths (v0.28.1 A4)"},
	{Table: "aveloxis_data.messages", Column: "msg_text_clean", Policy: sqlscan.AlwaysRefresh,
		Reason: "quote-stripped body — recomputed from the incoming raw body on every email upsert (Part B); a stale clean beside a refreshed raw would silently serve old text through the COALESCE read path"},
	{Table: "aveloxis_data.messages", Column: "msg_text_clean_rule", Policy: sqlscan.AlwaysRefresh,
		Reason: "the pattern-library version that produced msg_text_clean — travels with it on every write so --rule-rerun can find stale-rule rows"},
	{Table: "aveloxis_data.messages", Column: "msg_updated", Policy: sqlscan.GreatestNonNull,
		Reason: "the provider's comment edit time only increases; a stale replayed envelope must not regress it — it is the comparator the msg_text freshness guard reads (round 14)"},
	{Table: "aveloxis_data.contributors", Column: "cntrb_login", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "refreshed ONLY on cntrb_id-keyed conflicts (same person, deterministic UUID) so renames pick up the current login; never keyed by login itself (v0.18.29 Fix 3, v0.22.0)"},
	{Table: "aveloxis_data.contributor_identities", Column: "node_id", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "pre-v0.27.103 GraphQL-rail rows carry empty node_id; a non-empty observation heals them, empty never clobbers (v0.27.103)"},
	{Table: "aveloxis_data.contributor_identities", Column: "user_type", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "same healing contract as node_id (v0.27.103)"},
}

// columnWriteExceptions — deliberate, reviewed deviations. An entry
// that suppresses nothing fails the run as STALE. These five are the
// FIRST-RUN findings (v0.27.126), each triaged as a deliberate
// deviation rather than a bug or a wrong policy:
var columnWriteExceptions = []sqlscan.Exception{
	{Table: "aveloxis_data.repos", Column: "added_at", File: "internal/db/migrate.go",
		Match:  "SET added_at = COALESCE(data_collection_date, created_at, NOW())",
		Reason: "the v0.27.60 one-shot backfill for pre-column rows — WHERE added_at IS NULL makes it self-disabling, so it can never degrade a real stamp"},
	{Table: "aveloxis_data.repos", Column: "forked_from", File: "internal/db/repo_metadata.go",
		Match:  "forked_from = $6",
		Reason: "Phase 0's FetchRepoInfo is AUTHORITATIVE for fork lineage — a repo detaching from its upstream honestly clears the column (v0.27.78); the prefer-nonempty policy protects the id-less org-scan re-upsert path only"},
	{Table: "aveloxis_data.contributors", Column: "cntrb_login", File: "internal/db/commit_resolver_store.go",
		Match:  "SET gh_login = $2, cntrb_login = $2",
		Reason: "the row-exists-by-deterministic-ID branch: keyed by cntrb_id (same person by construction), the freshly-resolved login IS the truth and callers only reach this with a non-empty resolved login (v0.19.2 rename path)"},
	{Table: "aveloxis_data.contributor_identities", Column: "node_id", File: "internal/db/contributors.go",
		Match:  "SET node_id = COALESCE(NULLIF(node_id, ''), $3)",
		Reason: "the v0.27.112 step-2 IN-PLACE heal deliberately fills empties only — node_id never changes for a platform user, so fill-empty is a strictly more conservative shape than the registered prefer-nonempty (which governs the full upserts)"},
	{Table: "aveloxis_data.contributor_identities", Column: "user_type", File: "internal/db/contributors.go",
		Match:  "user_type = COALESCE(NULLIF(user_type, ''), $4)",
		Reason: "same in-place-heal rationale as node_id (v0.27.112)"},
}

// Enforces SR-11 (scripts/standing_rules.go).
func TestColumnWritePolicies(t *testing.T) {
	stmts := sqlscan.Statements(srctest.PackageFiles(t, "internal/db", 30))
	rep := sqlscan.Check(stmts, columnWritePolicies, columnWriteExceptions)
	for _, v := range rep.Violations {
		t.Errorf("%s.%s in %s violates %s:\n  expr: %s\n  %s\n  TRIAGE (never weaken a matcher): fix the SQL | correct the registered policy | add an Exception WITH a reason",
			v.Table, v.Column, v.File, v.Want, v.Expr, v.Detail)
	}
	for _, ex := range rep.StaleExceptions {
		t.Errorf("stale exception %s.%s (%s: %q) — it suppressed nothing this run; delete it (reason was: %s)",
			ex.Table, ex.Column, ex.File, ex.Match, ex.Reason)
	}
	for _, u := range rep.Unwritten {
		t.Errorf("registered column %s.%s has NO writer statement — registry rot (renamed column?) or a dark column (the column-writer tripwire's territory)",
			u.Table, u.Column)
	}
	// Registry hygiene: every entry carries a reason.
	for _, r := range columnWritePolicies {
		if r.Reason == "" {
			t.Errorf("%s.%s: registry entries REQUIRE a reason", r.Table, r.Column)
		}
	}
	for _, ex := range columnWriteExceptions {
		if ex.Reason == "" {
			t.Errorf("exception %s.%s: exceptions REQUIRE a reason", ex.Table, ex.Column)
		}
	}
}
