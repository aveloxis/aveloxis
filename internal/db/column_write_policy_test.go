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
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

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
	// v0.29.68 (worklist follow-up 8, review rounds 1-2): the three
	// collection-side metadata columns are INSERT-only for the identity
	// writer. UpsertRepo's conflict arm wrote them from its callers' zero
	// values (no caller carries them from the forge), blanking a collected
	// repository on every re-add; Phase 0's UpdateRepoMetadata is the
	// collection writer and the lifecycle writers (SidelineRepo, the
	// v0.27.50 migrate step) set repo_archived — each an Exception.
	{Table: "aveloxis_data.repos", Column: "repo_description", Policy: sqlscan.InsertOnly,
		Reason: "collected by Phase 0 (UpdateRepoMetadata) every cycle; the identity upsert never carries it, so its conflict arm must not touch it (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "primary_language", Policy: sqlscan.InsertOnly,
		Reason: "same as repo_description (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", Policy: sqlscan.InsertOnly,
		Reason: "collected by Phase 0 every cycle, both directions; set by the lifecycle writers on their own paths; the identity upsert never carries it (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repo_deps_vulnerabilities", Column: "first_detected_at", Policy: sqlscan.EarliestOfStoredAndIncoming,
		Reason: "v0.29.166: a finding's first detection never moves forward; a successor is handed its predecessor's date (summary/70, 4.3), and a plain re-observation offers NOW(), which is never earlier"},
	{Table: "aveloxis_data.repo_deps_vulnerabilities", Column: "entered_exposure_at", Policy: sqlscan.KeepCarryOrNow,
		Reason: "v0.29.166: a write may keep the stored time, take a predecessor's carried one, or stamp now, by the finding upsert's class-transition CASE; WHEN each applies is pinned by TestEnteredExposureIsSetWhenTheClassBecomesExposure and TestEnteredExposureIsCarried"},
	{Table: "aveloxis_data.repo_deps_libyear", Column: "package_key", Policy: sqlscan.FillEmptyOnly,
		Reason: "v0.29.173: written on insert (0.29.172) and never changed; the fill's UPDATE is guarded on the key being empty, so it cannot overwrite a key or race the insert"},
	{Table: "aveloxis_data.repo_lockfile_packages", Column: "package_key", Policy: sqlscan.FillEmptyOnly,
		Reason: "v0.29.173: as repo_deps_libyear.package_key"},
	{Table: "aveloxis_data.repos", Column: "vuln_scan_last_run", Policy: sqlscan.AlwaysRefresh,
		Reason: "completed-scan stamp — every completed vulnerability scan re-stamps NOW(); never written on error paths (v0.28.1 A4)"},
	{Table: "aveloxis_data.messages", Column: "msg_text_clean", Policy: sqlscan.AlwaysRefresh,
		Reason: "quote-stripped body — recomputed from the incoming raw body on every email upsert (Part B); a stale clean beside a refreshed raw would silently serve old text through the COALESCE read path"},
	{Table: "aveloxis_data.messages", Column: "msg_text_clean_rule", Policy: sqlscan.AlwaysRefresh,
		Reason: "the pattern-library version that produced msg_text_clean — travels with it on every write so --rule-rerun can find stale-rule rows"},
	{Table: "aveloxis_data.messages", Column: "msg_updated", Policy: sqlscan.GreatestNonNull,
		Reason: "the provider's comment edit time only increases; a stale replayed envelope must not regress it — it is the comparator the msg_text freshness guard reads (round 14)"},
	{Table: "aveloxis_data.contributors", Column: "cntrb_login", Policy: sqlscan.FillEmptyOnly,
		Reason: "R2 audit trail — the login as FIRST observed, so an established value never changes. " +
			"Tightened from PreferNonemptyIncoming on 2026-09-11 (F4): the commit-resolver branch that " +
			"re-wrote it was the sole source of all 555 idx_contributors_login violations in the " +
			"2026-09-06..09-11 production log and is gone, and the two remaining writers — the " +
			"ON CONFLICT (cntrb_id) race arms in commit_resolver_store.go and contributors.go — now " +
			"preserve the stored login and fill only an empty one, which also makes them structurally " +
			"unable to trip the partial unique index (v0.18.29 Fix 3, v0.22.0)"},
	{Table: "aveloxis_data.contributor_identities", Column: "node_id", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "pre-v0.27.103 GraphQL-rail rows carry empty node_id; a non-empty observation heals them, empty never clobbers (v0.27.103)"},
	{Table: "aveloxis_data.contributor_identities", Column: "user_type", Policy: sqlscan.PreferNonemptyIncoming,
		Reason: "same healing contract as node_id (v0.27.103)"},
	// v0.29.118 GitHub traffic (aveloxis/aveloxis#234): the latest read
	// wins. GitHub can still revise the newest day of a read, so a day,
	// a 14-day window or a week read again takes the later read's
	// figures and the later collection date.
	{Table: "aveloxis_data.repo_clones", Column: "total_clones", Policy: sqlscan.AlwaysRefresh,
		Reason: "latest read of the day wins: GitHub revises the newest day after the first read (v0.29.118)"},
	{Table: "aveloxis_data.repo_clones", Column: "unique_clones", Policy: sqlscan.AlwaysRefresh,
		Reason: "as total_clones; a read without a distinct count (NULL, never 0) replaces a stored number too, since the later read is the one that counts (v0.29.118)"},
	{Table: "aveloxis_data.repo_clones", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "the day's figures are those of the read made at this date (v0.29.118)"},
	{Table: "aveloxis_data.repo_views", Column: "total_views", Policy: sqlscan.AlwaysRefresh,
		Reason: "latest read of the day wins, as repo_clones (v0.29.118)"},
	{Table: "aveloxis_data.repo_views", Column: "unique_views", Policy: sqlscan.AlwaysRefresh,
		Reason: "as total_views (v0.29.118)"},
	{Table: "aveloxis_data.repo_views", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "as repo_clones.data_collection_date (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_windows", Column: "uniques", Policy: sqlscan.AlwaysRefresh,
		Reason: "a window read again takes the later read's distinct count (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_windows", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "as repo_clones.data_collection_date (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_weeks", Column: "uniques", Policy: sqlscan.AlwaysRefresh,
		Reason: "a complete week lies in the window of 7 to 8 daily reads; the later read's distinct count wins (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_weeks", Column: "data_collection_date", Policy: sqlscan.AlwaysRefresh,
		Reason: "as repo_clones.data_collection_date (v0.29.118)"},
	// The path and referrer snapshot of a date is replaced as a whole
	// (delete the date's rows, then insert the read's), never edited in
	// place: an UPDATE would keep an entry that dropped out of the top
	// 10 between two reads of one day (v0.29.118).
	{Table: "aveloxis_data.repo_traffic_paths", Column: "title", Policy: sqlscan.InsertOnly,
		Reason: "snapshot row: replaced with its date's snapshot, never updated in place (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_paths", Column: "total_views", Policy: sqlscan.InsertOnly,
		Reason: "as title (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_paths", Column: "unique_views", Policy: sqlscan.InsertOnly,
		Reason: "as title (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_referrers", Column: "total_views", Policy: sqlscan.InsertOnly,
		Reason: "snapshot row, as repo_traffic_paths (v0.29.118)"},
	{Table: "aveloxis_data.repo_traffic_referrers", Column: "unique_views", Policy: sqlscan.InsertOnly,
		Reason: "as total_views (v0.29.118)"},
}

// columnWriteExceptions — deliberate, reviewed deviations. An entry
// that suppresses nothing fails the run as STALE. These five are the
// FIRST-RUN findings (v0.27.126), each triaged as a deliberate
// deviation rather than a bug or a wrong policy:
var columnWriteExceptions = []sqlscan.Exception{
	{Table: "aveloxis_data.repos", Column: "platform_repo_id", File: "internal/db/repo_forge_id_changes.go",
		Match:  "UPDATE aveloxis_data.repos SET platform_repo_id = $3",
		Reason: "v0.29.62 AdoptForgeID — the operator's explicit adoption of a re-created upstream (2026-09-23 decision), guarded WHERE platform_repo_id = the expected old ID in the same transaction that records the change in repo_forge_id_changes; the mismatch signal the fill-empty policy protects is preserved there, not erased"},
	{Table: "aveloxis_data.repos", Column: "added_at", File: "internal/db/migrate.go",
		Match:  "SET added_at = COALESCE(data_collection_date, created_at, NOW())",
		Reason: "the v0.27.60 one-shot backfill for pre-column rows — WHERE added_at IS NULL makes it self-disabling, so it can never degrade a real stamp"},
	// v0.29.68 (follow-up 8): the three collection-side columns are
	// InsertOnly for the identity upsert; these are their real writers.
	{Table: "aveloxis_data.repos", Column: "repo_description", File: "internal/db/repo_metadata.go",
		Match:  "SET repo_description = $2",
		Reason: "Phase 0's UpdateRepoMetadata is the collection writer: the forge's current description every cycle, an emptied one included (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "primary_language", File: "internal/db/repo_metadata.go",
		Match:  "primary_language = $3",
		Reason: "Phase 0's UpdateRepoMetadata, as repo_description (v0.29.68, follow-up 8)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", File: "internal/db/repo_metadata.go",
		Match:  "repo_archived    = $5",
		Reason: "Phase 0's UpdateRepoMetadata propagates the forge's archived status both directions (v0.27.50)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", File: "internal/db/repo_gone.go",
		Match:  "SET repo_archived = TRUE, repo_gone_at = NOW()",
		Reason: "SidelineRepo — the gone lifecycle sets the flag with the gone stamp (v0.29.68, follow-up 8: a lifecycle writer)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", File: "internal/db/repo_gone.go",
		Match:  "repo_archived = COALESCE((SELECT ri.status = 'Archived'",
		Reason: "clearGoneCTE — every clearer of the gone stamp puts the flag back to the forge's latest snapshot (0.29.140, Copilot PR #252 eighth review: it stayed TRUE after a recovery)"},
	{Table: "aveloxis_data.repos", Column: "repo_archived", File: "internal/db/migrate.go",
		Match:  "SET repo_archived = (latest.status = 'Archived')",
		Reason: "the v0.27.50 one-shot backfill from repo_info.status for rows collected before Phase 0 propagated the flag"},
	{Table: "aveloxis_data.repos", Column: "forked_from", File: "internal/db/repo_metadata.go",
		Match:  "forked_from = $6",
		Reason: "Phase 0's FetchRepoInfo is AUTHORITATIVE for fork lineage — a repo detaching from its upstream honestly clears the column (v0.27.78); the prefer-nonempty policy protects the id-less org-scan re-upsert path only"},
	// RETIRED (2026-09-11, F4): the cntrb_login exception on
	// commit_resolver_store.go's row-exists-by-deterministic-ID branch
	// is gone because the WRITE is gone. Its reason argued the
	// freshly-resolved login "IS the truth" for a cntrb_id-keyed row —
	// true about the person, but cntrb_login is not the person's
	// current name, it is the first-observed one (R2), and gh_login
	// already carries the current name. Production settled the
	// argument: that statement produced all 555 idx_contributors_login
	// unique violations in the 2026-09-06..09-11 log, and its recovery
	// dropped the gh_login update too, so every affected rename was
	// lost outright. Deliberately NOT re-added as a differently-worded
	// exception — the code now agrees with the doc.
	{Table: "aveloxis_data.contributor_identities", Column: "node_id", File: "internal/db/contributors.go",
		Match:  "SET node_id = COALESCE(NULLIF(node_id, ''), $3)",
		Reason: "the v0.27.112 step-2 IN-PLACE heal deliberately fills empties only — node_id never changes for a platform user, so fill-empty is a strictly more conservative shape than the registered prefer-nonempty (which governs the full upserts)"},
	{Table: "aveloxis_data.contributor_identities", Column: "user_type", File: "internal/db/contributors.go",
		Match:  "user_type = COALESCE(NULLIF(user_type, ''), $4)",
		Reason: "same in-place-heal rationale as node_id (v0.27.112)"},
	// Operator decision, 2026-11-19 (option 1 of three): the pre-1970 repair
	// (cleanupBadTimestamps, a ledgered migrate step) clears a stored
	// created_at / updated_at, which each column's policy forbids. It was
	// first read by this check in 0.29.203 (its column is written quoted,
	// and its table comes from a list). Each Match is the WHOLE statement:
	// the exception holds only while the statement's own condition limits
	// it to a year before 1970, which no forge date has (the value is Go's
	// zero time, written by an old bug). The next collection then stores
	// the real date, which both policies allow over an empty value.
	{Table: "aveloxis_data.repos", Column: "created_at",
		Match:  `UPDATE aveloxis_data.repos SET "created_at" = NULL WHERE "created_at" IS NOT NULL AND EXTRACT(YEAR FROM "created_at") < 1970`,
		Reason: "one-time repair of a value that is not a date (year before 1970); the condition is in the statement and the whole statement is matched"},
	{Table: "aveloxis_data.repos", Column: "updated_at",
		Match:  `UPDATE aveloxis_data.repos SET "updated_at" = NULL WHERE "updated_at" IS NOT NULL AND EXTRACT(YEAR FROM "updated_at") < 1970`,
		Reason: "one-time repair of a value that is not a date (year before 1970); the condition is in the statement and the whole statement is matched"},
}

// columnWriteFills gives the real values of the parts of a write that the
// reader of assembled statements (sqlscan.Assemble) cannot follow, so that
// every instance is checked as a whole statement. The rows come from the
// variable the code itself reads: a value added there is checked with no
// one remembering to. v0.29.203 (summary/71, the 0.29.166 item).
func columnWriteFills(t testing.TB) []sqlscan.Fill {
	carried := strings.Join(libyearCarriedColumns, ", ")
	return []sqlscan.Fill{
		{Decl: "replaceLibyearDeleteAndCarrySQL", Match: "WITH gone AS ( DELETE FROM aveloxis_data.repo_deps_libyear",
			Values: [][]string{{carried, carried, carried}},
			Reason: "the three parts are one value, strings.Join(libyearCarriedColumns, \", \"), which a function computes from libyearRotatedColumns at start; the row is that variable"},

		// 0.29.208 (review round 1): the SQL argument of a call that writes
		// is read whatever its form, and one with no fixed text at all is a
		// violation. These are the store's: each is given the VALUES it
		// takes, from the code itself, so that each is checked whole.
		{Decl: "PostgresStore.ReplaceRepoLibyear", Match: sqlscan.Unknown, Arg: "rotateLibyearInsertSQL",
			Values: [][]string{{rotateLibyearInsertSQL}},
			Reason: "the argument is the package variable rotateLibyearInsertSQL, which a function computes once at start; the row is that variable"},
		{Decl: "CreateMaterializedViews", Match: sqlscan.Unknown, Arg: "matviewsSQL",
			Values: [][]string{{matviewsSQL}},
			Reason: "the argument is matviews.sql, embedded; the row is the embedded text"},
		{Decl: "RunMigrations", Match: sqlscan.Unknown, Arg: "schemaSQL",
			Values: [][]string{{schemaSQL}},
			Reason: "the argument is schema.sql, embedded; the row is the embedded text"},
		{Decl: "RunMigrations", Match: sqlscan.Unknown, Arg: "viewsSQL",
			Values: [][]string{{viewsSQL}},
			Reason: "the argument is views.sql, embedded; the row is the embedded text"},
		// 0.29.210 (review round 3, deny by default): every SQL argument
		// that is not read whole is registered, a read included, and no
		// part of a statement is judged harmless. These are the ones that
		// can be given their values.
		{Decl: "PostgresStore.ReplaceRepoLibyear", Match: sqlscan.Unknown, Arg: "replaceLibyearDeleteAndCarrySQL",
			Values: [][]string{{replaceLibyearDeleteAndCarrySQL}},
			Reason: "the argument is the package variable replaceLibyearDeleteAndCarrySQL (a DELETE ... RETURNING and an INSERT, run through Query for its rows); the row is that variable"},
		{Decl: "clearOnlySQL", Match: "WITH cleared AS",
			Values: goneClearPredicatesOf(t, clearOnlySQL),
			Reason: "the part is the predicate clearOnlySQL is called with: c.sideline + c.guard of a goneClear whose build it is; the rows are those of every goneClear literal in the source that builds with it"},
		{Decl: "resurrectSQL", Match: "WITH cleared AS",
			Values: goneClearPredicatesOf(t, resurrectSQL),
			Reason: "the part is the predicate resurrectSQL is called with: c.sideline + c.guard of a goneClear whose build it is; the rows are those of every goneClear literal in the source that builds with it"},
		{Decl: "clearInTxUnderRowLock", Match: "SELECT 1 FROM aveloxis_data.repos WHERE repo_id = $1 AND (" + sqlscan.Unknown + ") FOR NO KEY UPDATE",
			Values: goneClearSidelines(t),
			Reason: "the part is c.sideline of the goneClear a caller passes; the rows are the sidelines of every goneClear literal in the source"},
		{Decl: "clearInTxUnderRowLock", Match: sqlscan.Unknown, Arg: "sql",
			Values: goneClearStatements(t),
			Reason: "the argument is the local sql, which is c.build(c.sideline + c.guard) for the goneClear a caller passes (TestTheGoneClearStatementIsBuiltOnce pins that one definition); the rows are that expression for every goneClear literal in the store's source, read from the source itself (goneClearsInSource), and nothing changes a goneClear after it is written (TestAGoneClearIsNeverChangedAfterItIsWritten)"},
	}
}

// goneClearPredicatesOf are the predicates (sideline + guard) of the
// goneClear literals in the source that build with build, one row each.
func goneClearPredicatesOf(t testing.TB, build func(string) string) [][]string {
	const probe = "\x00probe\x00"
	seen := map[string]bool{}
	var out [][]string
	for _, c := range goneClearsInSource(t) {
		if c.build(probe) != build(probe) {
			continue
		}
		if p := c.sideline + c.guard; !seen[p] {
			seen[p] = true
			out = append(out, []string{p})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// goneClearSidelines are the sidelines of the goneClear literals the row
// lock gates on (those not locked always).
func goneClearSidelines(t testing.TB) [][]string {
	seen := map[string]bool{}
	var out [][]string
	for _, c := range goneClearsInSource(t) {
		if !seen[c.sideline] {
			seen[c.sideline] = true
			out = append(out, []string{c.sideline})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })
	return out
}

// packageKeyFillTablesChecked are the tables the package-key fill walks.
var packageKeyFillTablesChecked = []PackageKeyFillTable{PackageKeyFillInventory, PackageKeyFillLockfile}

// packageKeyFillSQL's statements are read whole since 0.29.212 (the reader
// follows the call and takes each return); this keeps the function to one
// return per table and the refusal.
func TestPackageKeyFillTablesAreTheFilledOnes(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func packageKeyFillSQL("))
	if n := strings.Count(body, "case "); n != len(packageKeyFillTablesChecked) {
		t.Errorf("packageKeyFillSQL has %d cases and the write-policy fill lists %d tables: add the table to packageKeyFillTablesChecked", n, len(packageKeyFillTablesChecked))
	}
	// One return per case and the refusal, and nothing that returns under
	// a condition: an `if` arm returning other SQL was not a case and went
	// unseen (review round 3).
	if n := strings.Count(body, "return "); n != len(packageKeyFillTablesChecked)+1 || regexp.MustCompile(`\bif\b`).MatchString(body) {
		t.Errorf("packageKeyFillSQL has %d returns (want one per table and the refusal) or an if: every statement it can return must be a case this test counts", n)
	}
	if _, _, _, err := packageKeyFillSQL("no-such-table"); err == nil {
		t.Error("packageKeyFillSQL accepted a table it does not name")
	}
}

// The fill names the local `writeSQL`; this pins what that local is: given
// once, by packageKeyFillSQL(table), and never changed. Without it the fill
// went on checking the function's constants while the step ran something
// else (review round 2: `writeSQL = rewrite(writeSQL)` passed every pin).
func TestThePackageKeyFillWriteIsTheFunctionsResult(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) FillPackageKeysStep("))
	if strings.Count(body, "boundSQL, pageSQL, writeSQL, err := packageKeyFillSQL(table)") != 1 {
		t.Error("FillPackageKeysStep must take its three statements once, from packageKeyFillSQL(table): the write-policy fill checks that function's results")
	}
	if m := regexp.MustCompile(`\bwriteSQL\s*(\+=|=[^=])|&writeSQL\b`).FindString(body); m != "" {
		t.Error("FillPackageKeysStep changes writeSQL after taking it, or takes its address: what it runs is then not what the fill checks")
	}
	// Given once: the one definition above is the only statement that has
	// writeSQL on its left.
	if n := len(regexp.MustCompile(`(?m)^[^\n]*\bwriteSQL\b[^\n=]*:?=[^=]`).FindAllString(body, -1)); n != 1 {
		t.Errorf("writeSQL stands on the left of %d assignments in FillPackageKeysStep, want the one", n)
	}
	if strings.Count(body, "b.Queue(writeSQL, ") != 1 || strings.Count(body, "writeSQL") != 2 {
		t.Error("FillPackageKeysStep must use writeSQL once, as the statement it queues")
	}
}

// Every name a fill stands for is given its value ONCE, and nothing else can
// give it another: by the type checker's OBJECT, not by the name's
// spelling (0.29.211). A fill says "this argument is that value"; the
// checks above pinned that for one of the names by matching text in one
// function, and review round 4 changed each of the others after its
// definition (`pageSQL = f(pageSQL)`; `replaceLibyearDeleteAndCarrySQL =
// f(x)` in another function) with every check green.
func TestEveryFilledNameIsGivenItsValueOnce(t *testing.T) {
	root, files := srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30)
	for _, c := range []struct{ decl, name, how, value string }{
		{"PostgresStore.FillPackageKeysStep", "boundSQL", "declared", "packageKeyFillSQL(table)"},
		{"PostgresStore.FillPackageKeysStep", "pageSQL", "declared", "packageKeyFillSQL(table)"},
		{"PostgresStore.FillPackageKeysStep", "writeSQL", "declared", "packageKeyFillSQL(table)"},
		{"clearInTxUnderRowLock", "sql", "declared", "c.build(c.sideline + c.guard)"},
		{"PostgresStore.ReplaceRepoLibyear", "rotateLibyearInsertSQL", "declared", ""},
		{"PostgresStore.ReplaceRepoLibyear", "replaceLibyearDeleteAndCarrySQL", "declared", ""},
		{"CreateMaterializedViews", "matviewsSQL", "declared", ""},
		{"RunMigrations", "schemaSQL", "declared", ""},
		{"RunMigrations", "viewsSQL", "declared", ""},
	} {
		got, err := sqlscan.Givings(root, files, c.decl, c.name)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != 1 || got[0].How != c.how || (c.value != "" && got[0].Value != c.value) {
			t.Errorf("%s: %s is given a value at %+v; the write-policy fill for it holds only if it is %s once%s, and nothing else can change it",
				c.decl, c.name, got, c.how, map[bool]string{true: " as " + c.value, false: ""}[c.value != ""])
		}
	}
}

// A goneClear's fields are never assigned, whoever holds it: by TYPE, so a
// copy of one (`other := gc; other.sideline = "TRUE"`), which the check by
// the holder's name below does not see, is caught too.
func TestNoFieldOfAGoneClearIsEverAssigned(t *testing.T) {
	writes, err := sqlscan.FieldWrites(srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30), "goneClear")
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range writes {
		t.Errorf("%s: a field of a goneClear is assigned or has its address taken: the fill reads each clear from its literal, and this one is no longer what its literal says", w)
	}
}

// The identifiers the comparison tools put into SQL are quoted as SQL
// quotes them (review round 4: Go's %q was used; a name with a double quote
// in it came out as \", which is not a quoted identifier).
func TestCatalogNamesAreQuotedAsSQLQuotesThem(t *testing.T) {
	for in, want := range map[string]string{"repos": `"repos"`, `we"ird`: `"we""ird"`, "Mixed Case": `"Mixed Case"`} {
		if got := quotedIdentifier(in); got != want {
			t.Errorf("quotedIdentifier(%q) = %s, want %s", in, got, want)
		}
	}
	if got := quotedIdentifier("aveloxis_data", `ta"ble`); got != `"aveloxis_data"."ta""ble"` {
		t.Errorf("a dotted path: %s", got)
	}
	for _, fn := range []string{"func tableRowCounts(", "func populatedCounts("} {
		body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", fn))
		if strings.Contains(body, "%q") || !strings.Contains(body, "quotedIdentifier(") {
			t.Errorf("%s must quote catalog names with quotedIdentifier, never with fmt's %%q", fn)
		}
	}
}

// Three reads take a value from an API request into their text, and each
// refuses any value that is not one of its own before it builds the
// statement. Their registrations rest on that refusal; round 4 removed
// each and no test failed. The store has no pool here: a call that got
// past the refusal would panic.
func TestTheReadsThatTakeARequestValueRefuseAnUnknownOne(t *testing.T) {
	s, ctx := &PostgresStore{}, context.Background()
	if _, _, err := s.ContributorRetentionSeries(ctx, []int64{1}, "week; DROP TABLE x", time.Time{}, time.Time{}, 1); err == nil {
		t.Error("ContributorRetentionSeries took a bucket that is neither week nor month")
	}
	if _, err := s.MetricWeeklySeries(ctx, []int64{1}, "commits", "day", time.Time{}, time.Time{}); err == nil {
		t.Error("MetricWeeklySeries took a bucket that is neither week nor month")
	}
	if _, err := s.MetricWeeklySeries(ctx, []int64{1}, "no-such-metric", "week", time.Time{}, time.Time{}); err == nil {
		t.Error("MetricWeeklySeries took a metric that has no statement")
	}
	if _, _, err := s.LatestCount(ctx, 1, "repo_name FROM x; --"); err == nil {
		t.Error("LatestCount took a column that is not one of snapshotColumns")
	}
}

// What a goneClear literal's fields may be given, by the name the source
// writes. A literal that uses anything else (another name, an expression)
// cannot be read here and fails the checks below until it is added.
var (
	goneClearPredicates = map[string]string{"goneSidelineSQL": goneSidelineSQL, "archivedSidelineSQL": archivedSidelineSQL, "listedGuardSQL": listedGuardSQL}
	goneClearBuilders   = map[string]func(string) string{"clearOnlySQL": clearOnlySQL, "resurrectSQL": resurrectSQL}
)

var (
	goneClearLiteralRe = regexp.MustCompile(`goneClear\{[^}]*\}`)
	goneClearFieldRe   = regexp.MustCompile(`^(sideline|guard|build|lockAlways): (\w+)$`)
)

// goneClearsInSource reads every goneClear literal of the store's source
// and gives, for each DISTINCT one, the clear it denotes: the fill's rows
// are built from the source itself, one to one, so a clear cannot be run
// that is not checked, and none is checked that is not run. (Until
// 0.29.209 a hand-kept list stood beside the source, tied to it by length
// only: one of its members could be replaced by a copy of another.)
func goneClearsInSource(t testing.TB) map[string]goneClear {
	t.Helper()
	all := srctest.StripGoComments(srctest.PackageSource(t, "internal/db", 30))
	out := map[string]goneClear{}
	for _, lit := range goneClearLiteralRe.FindAllString(all, -1) {
		var c goneClear
		seen := map[string]bool{}
		for _, field := range strings.Split(strings.TrimSuffix(strings.TrimPrefix(lit, "goneClear{"), "}"), ", ") {
			m := goneClearFieldRe.FindStringSubmatch(strings.TrimSpace(field))
			if m == nil || seen[m[1]] {
				t.Fatalf("%s: the field %q is not `name: identifier`, once: the write-policy check cannot tell which statement this clear runs. Write it with the named predicates and builders", lit, field)
			}
			seen[m[1]] = true
			pred, isPred := goneClearPredicates[m[2]]
			build, isBuild := goneClearBuilders[m[2]]
			switch {
			case m[1] == "sideline" && isPred:
				c.sideline = pred
			case m[1] == "guard" && isPred:
				c.guard = pred
			case m[1] == "build" && isBuild:
				c.build = build
			case m[1] == "lockAlways" && (m[2] == "true" || m[2] == "false"):
				c.lockAlways = m[2] == "true"
			default:
				t.Fatalf("%s: %s is given %s, which is not one of the predicates or builders this check knows (goneClearPredicates, goneClearBuilders): add it there, so that its statement is checked", lit, m[1], m[2])
			}
		}
		if c.sideline == "" || c.build == nil {
			t.Fatalf("%s: a clear has a sideline and a builder", lit)
		}
		out[lit] = c
	}
	// The denominator: the store has clears, and the type's own
	// declaration is not one (it has no braces after the name there).
	srctest.MinCount(t, "distinct goneClear literals in the store", len(out), 1)
	return out
}

func goneClearStatements(t testing.TB) [][]string {
	clears := goneClearsInSource(t)
	lits := make([]string, 0, len(clears))
	for lit := range clears {
		lits = append(lits, lit)
	}
	sort.Strings(lits)
	seen := map[string]bool{}
	var out [][]string
	for _, lit := range lits {
		c := clears[lit]
		if sql := c.build(c.sideline + c.guard); !seen[sql] {
			seen[sql] = true
			out = append(out, []string{sql})
		}
	}
	return out
}

// The fill names the local `sql`; this pins what that local is.
func TestTheGoneClearStatementIsBuiltOnce(t *testing.T) {
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func clearInTxUnderRowLock("))
	// Every statement that gives `sql` a value, in any form: alone, as one
	// of several targets (`sql, _ = ...` passed the substring check this
	// replaced, review round 3), declared again in an inner block, appended
	// to; and its address.
	gives := regexp.MustCompile(`(?m)(^|[\s,;{(])sql\b\s*(,[^=\n]*)?(:=|\+=|=[^=])`).FindAllString(body, -1)
	if strings.Count(body, "sql := c.build(c.sideline + c.guard)") != 1 || len(gives) != 1 || strings.Contains(body, "&sql") {
		t.Errorf("clearInTxUnderRowLock must give its statement once, as c.build(c.sideline + c.guard): the write-policy fill checks that expression's values (it is given a value %d times: %q)", len(gives), gives)
	}
}

// The rows the fill is given are the literals' own. Each named predicate
// and builder is used by some literal (a name here that no clear uses
// would be a statement checked and never run), and the statements differ
// where the literals' predicates do.
func TestEveryGoneClearIsInTheFill(t *testing.T) {
	clears := goneClearsInSource(t)
	usedPred, usedBuild := map[string]bool{}, map[string]bool{}
	for lit := range clears {
		for name := range goneClearPredicates {
			if strings.Contains(lit, ": "+name) {
				usedPred[name] = true
			}
		}
		for name := range goneClearBuilders {
			if strings.Contains(lit, "build: "+name) {
				usedBuild[name] = true
			}
		}
	}
	for name := range goneClearPredicates {
		if !usedPred[name] {
			t.Errorf("no goneClear in the store uses %s: remove it from goneClearPredicates", name)
		}
	}
	for name := range goneClearBuilders {
		if !usedBuild[name] {
			t.Errorf("no goneClear in the store uses %s: remove it from goneClearBuilders", name)
		}
	}
	// One statement per (sideline, guard, builder); lockAlways changes the
	// lock, not the statement.
	shapes := map[string]bool{}
	for _, c := range clears {
		shapes[c.build(c.sideline+c.guard)] = true
	}
	if got := len(goneClearStatements(t)); got != len(shapes) || got < 2 {
		t.Errorf("the fill is given %d statements for %d distinct clears", got, len(shapes))
	}
}

// A goneClear is what its literal says, from the moment it is written to
// the moment it is run: nothing assigns one of its fields. Otherwise a
// clear could be built, changed and run (`gc.sideline = ... + " OR TRUE"`,
// or `c.guard += ...` in a helper on its way down), and the fill, which
// reads the literals, would check a statement that is not the one run.
func TestAGoneClearIsNeverChangedAfterItIsWritten(t *testing.T) {
	all := srctest.StripGoComments(srctest.PackageSource(t, "internal/db", 30))
	// Every value of the type, by how the store names them: the parameter
	// `c` of the two helpers, and any local built from a literal.
	names := map[string]bool{"c": true}
	for _, m := range regexp.MustCompile(`(\w+)\s*:?=\s*&?goneClear\{`).FindAllStringSubmatch(all, -1) {
		names[m[1]] = true
	}
	for _, m := range regexp.MustCompile(`\bvar\s+(\w+)\s+\*?goneClear\b`).FindAllStringSubmatch(all, -1) {
		names[m[1]] = true
	}
	for _, body := range []string{
		srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) clearUnderRowLock(")),
		srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func clearInTxUnderRowLock(")),
	} {
		if m := regexp.MustCompile(`\bc\.(sideline|guard|build|lockAlways)\s*(\+=|=[^=])|\bc\s*=[^=]|&c\b`).FindString(body); m != "" {
			t.Errorf("a helper changes the goneClear it was handed (%q): the fill checks the clear as its literal wrote it", m)
		}
	}
	for name := range names {
		if name == "c" {
			continue
		}
		re := regexp.MustCompile(`\b` + regexp.QuoteMeta(name) + `\.(sideline|guard|build|lockAlways)\s*(\+=|=[^=])`)
		if m := re.FindString(all); m != "" {
			t.Errorf("a goneClear held in %s has a field assigned after it is written (%q)", name, m)
		}
	}
	// And no goneClear is held in a variable at all today: each is written
	// where it is passed. A new holder is looked at above; this says when
	// one appears.
	if len(names) != 1 {
		t.Logf("goneClear values are now held in variables %v; each is checked above for field assignments", names)
	}
}

// columnWriteUnreadableExceptions is the last resort for a write the
// reader cannot follow, cannot judge on its fixed text and cannot be given
// its values. There is none, and one is added only with the operator's
// decision (2026-11-19: "ensure robustness and data integrity").
//
// 0.29.210: under deny-by-default a READ whose text this reader cannot
// assemble is registered too. Each below is one SQL argument, named as
// written, with what a person found when they read how it is built. None
// is a write, and none could be given as a fill: the statement is chosen
// or composed at run time from the package's own constants.
var columnWriteUnreadableExceptions = []sqlscan.UnreadableException{
	// SQL TEXT FROM OUTSIDE THE PROCESS. An exception is the only
	// registration that covers a part of no verified kind (0.29.212), and
	// each says where that text comes from.
	{Decl: "ContentExpectations.wherePassing", Match: "SELECT jsonb_build_object(", Call: 1, Of: 1,
		Source: "the data-test rules file: the JSON key `where` of a rule (LoadContentExpectations), written by the operator who runs data-test",
		Reason: "BY DESIGN: r.Where is a predicate an operator writes in the rules file, and it is put into this SELECT's WHERE as written. It can hold any SQL a predicate can. What bounds it: the statement runs inside contentQuery's transaction, which is READ ONLY and pinned to UTC (content_diff.go), so it cannot write; and the rules file is read by the data-test command only, never by serve, web or api. Every other part of the statement is of a verified kind (an identifier, a quoted literal, a list built in a loop)"},
	{Decl: "execCreateIndexConcurrently", Match: "ANALYZE " + sqlscan.Unknown, Call: 4, Of: 4,
		Source: "the database's catalog: expressionIndexTableWithoutStatistics reads quote_ident(nspname) || '.' || quote_ident(relname) from pg_class for the index just built",
		Reason: "the table to ANALYZE is named by the database, already quoted there by quote_ident; this reader cannot see that a value read from a query is an identifier. A read of statistics: ANALYZE writes no row of any table"},
}

// columnWriteShapes are the SQL arguments that are not read whole and whose
// every unread part is of a kind the READER verified (0.29.212): an
// identifier quoted by pgx.Identifier.Sanitize, a value inside a quoted
// literal with its quotes doubled, or the mark after text a loop repeats.
// An entry names the argument (declaration, place, count) and those kinds;
// nothing here is a description. A part of any other origin is covered by
// no shape. The pattern says what the fixed text must be.
var columnWriteShapes = func() []sqlscan.Shape {
	hole := regexp.QuoteMeta(sqlscan.Unknown)
	// Words that begin a statement, or an arm of one, that changes rows or
	// schema. SET covers UPDATE ... SET and DO UPDATE SET; INTO covers
	// INSERT INTO, MERGE INTO and SELECT ... INTO.
	writes := regexp.MustCompile(`(?i)\b(INSERT|DELETE|MERGE|TRUNCATE|ALTER|DROP|CREATE|GRANT|REVOKE|COPY|CALL|DO|EXECUTE|PREPARE|REFRESH|SET|INTO)\b`)
	const (
		ident = sqlscan.KindIdentifier
		lit   = sqlscan.KindLiteral
		rep   = sqlscan.KindRepeated
	)
	arg := func(decl string, call, of int, kinds ...string) sqlscan.ShapeArg {
		return sqlscan.ShapeArg{Decl: decl, Call: call, Of: of, Kinds: strings.Join(kinds, ",")}
	}
	return []sqlscan.Shape{
		{Name: "a read", Text: regexp.MustCompile(`(?is)^(SELECT|WITH|SHOW)\b[^;]*$`), Forbid: writes,
			Args: []sqlscan.ShapeArg{
				arg("ContentExpectations.CheckContent", 1, 1, ident),
				arg("PostgresStore.RunDataVerification", 5, 10, lit),
				arg("contentCount", 1, 1, ident),
				arg("diffOneTable", 1, 4, ident),
				arg("diffOneTable", 2, 4, ident, lit, rep),
				arg("diffOneTable", 3, 4, ident, lit, rep),
				arg("diffOneTable", 4, 4, ident, lit, rep),
				arg("populatedCounts", 1, 1, ident, rep),
				arg("tableRowCounts", 2, 2, ident),
			},
			Reason: "ONE statement that begins SELECT, WITH or SHOW and holds none of the words that change rows or schema. Its unread parts are names of the compared databases' own tables and columns (quoted identifiers), key names inside quoted literals, and lists of those built in a loop: the data-test comparison tools, and the forge-account pattern of data-verify"},
		{Name: "schema change", Text: regexp.MustCompile(`^(ALTER TABLE|CREATE (UNIQUE )?INDEX)\b[^;]*$`),
			// ALTER COLUMN ... TYPE ... USING rewrites every row's value
			// of the column: it is not of this shape.
			Forbid: regexp.MustCompile(`(?i)\b(INSERT|MERGE|TRUNCATE|GRANT|COPY|CALL|DO|EXECUTE)\b|\bUPDATE\s+\S+\s+SET\b|\bDELETE\s+FROM\b|\bALTER\s+COLUMN\b[^,]*\b(TYPE|USING)\b|\bSET\s+DATA\s+TYPE\b`),
			Args: []sqlscan.ShapeArg{
				arg("ensureAllFKsDeferrable", 2, 2, ident),
				arg("execMigrationStep", 1, 1, ident),
				arg("execCreateIndexConcurrently", 3, 4, ident),
				arg("migrateStage4DedupAndIndexes", 3, 12, ident),
				arg("setToolVersionDefaults", 2, 2, ident),
			},
			Reason: "DDL the migrate runs with a name read from the catalog, quoted as an identifier: a constraint made deferrable, the schema pg_trgm's operator class lives in, a tool_version column's default (which applies to rows written later, by statements this check reads). ONE statement, no statement that writes rows, no change of a column's type"},
		{Name: "a batch insert's rows", Text: regexp.MustCompile(`^INSERT INTO aveloxis_data\.(commits|commit_messages) \([a-z_, ]*\) VALUES [^;]* ON CONFLICT [^;]*$`),
			Forbid: regexp.MustCompile(`ON CONFLICT.*` + hole),
			Args: []sqlscan.ShapeArg{
				arg("PostgresStore.UpsertCommitMessageBatch", 1, 1, rep),
			},
			Reason: "the VALUES rows are written by a loop over the batch, each a row of placeholders and two quoted constants; one round of it is read and stands in the text Check judges, with the mark that it repeats. (UpsertCommitBatch's rows are placeholders only, and it is read whole.) The column list and the whole ON CONFLICT arm are fixed text; no unread part stands in or after ON CONFLICT"},
	}
}()

// columnWriteStatements is every write statement of the store that the
// policy check judges: those written as one string, those assembled from
// fixed text (one per value where a part takes several), and those the
// unreadable check hands on (filled, or with holes that cannot matter).
func columnWriteStatements(t *testing.T) ([]sqlscan.Stmt, sqlscan.UnreadReport) {
	t.Helper()
	return columnWriteStatementsOf(t, srctest.PackageFiles(t, "internal/db", 30), columnWritePolicies)
}

// columnWriteStatementsOf is that reading, of the files and against the
// registry it is given: the ONE way statements reach the policy check.
func columnWriteStatementsOf(t *testing.T, files map[string]string, registry []sqlscan.Registered) ([]sqlscan.Stmt, sqlscan.UnreadReport) {
	t.Helper()
	whole, unread, err := sqlscan.AssembleTyped(srctest.Root(t), files)
	if err != nil {
		t.Fatal(err)
	}
	rep := sqlscan.CheckUnreadable(unread, registry, columnWriteFills(t), columnWriteUnreadableExceptions, columnWriteShapes)
	return append(append(sqlscan.Statements(files), whole...), rep.Statements...), rep
}

// A write to a table with a protected column that cannot be read is a
// failure, never a silent pass (the checker's blind spot until 0.29.203:
// "SQL assembled by string concatenation is invisible").
//
// 0.29.210, deny by default: the claim is now about every SQL argument of
// the store, a read included, and about every part of it. Nothing is
// passed because a part "cannot matter". (The name is kept: the
// contributor guide and the lens file cite it.)
func TestNoProtectedWriteGoesUnread(t *testing.T) {
	_, rep := columnWriteStatements(t)
	for _, u := range rep.Violations {
		arg := ""
		if u.Arg != "" {
			arg = "\n  the argument, as written: " + u.Arg
		}
		t.Errorf("%s:%d (%s): SQL the policy check cannot read whole (%s):\n  %s%s\n  TRIAGE, in this order:\n"+
			"  1. REWRITE it so every part is fixed text: a value belongs in a placeholder ($1), never in the text (fmt's %%d included); choose between whole statements (q := a; if x { q = b }) and not between pieces.\n"+
			"  2. FILL it: give its parts' real values in columnWriteFills, taken from the variable or function the code itself reads, so each instance is checked whole.\n"+
			"  3. SHAPE: if it is of a kind already in columnWriteShapes (a read, a savepoint, a schema change, a batch's rows) and you have read where its unread parts come from, add this declaration to that shape's list.\n"+
			"  4. EXCEPTION (last, with the operator's decision): columnWriteUnreadableExceptions, naming the argument and what you found.\n"+
			"  If the call is not a database call at all (a method that happens to be named Exec, Queue or Prepare): the reader has no type information and cannot tell; rename the method or take step 4.",
			u.File, u.Line, u.Decl, u.Why, strings.Join(strings.Fields(u.Text), " "), arg)
	}
	for _, b := range rep.BadFills {
		t.Errorf("fill: %s", b)
	}
	for _, st := range rep.StaleShapes {
		t.Errorf("stale shape entry: %s", st)
	}
	for _, f := range rep.StaleFills {
		t.Errorf("stale fill %s (%q): it matched no statement; delete or correct it (reason was: %s)", f.Decl, f.Match, f.Reason)
	}
	for _, ex := range rep.StaleExceptions {
		t.Errorf("stale exception %s (%q): it covered nothing; delete it (reason was: %s)", ex.Decl, ex.Match, ex.Reason)
	}
}

// The denominator, without a number to keep (0.29.209; until then a floor,
// 55, stood under a count of 63 by a margin somebody chose). "No violation"
// proves nothing if the reading stopped: so a file of the store's package
// is added, in memory, that writes a table registered for this test alone,
// once per way a statement reaches the check, each time with a value the
// policy forbids or a part that cannot be read, and once per call that
// runs SQL in the reader's own table (sqlscan.Runners). Every one must
// come back as a violation THROUGH columnWriteStatementsOf, the function
// the real checks use, beside the store's real files. A reader that
// stopped reading, a wiring that dropped one of the three sources, or a
// call the reader no longer knows, loses a canary. What each rule of the
// reader reads is pinned in sqlscan's own tests, not here.
func TestTheWritePolicyCheckSeesEveryWayAStatementArrives(t *testing.T) {
	const table = "aveloxis_canary.write_policy_canary"
	files := srctest.PackageFiles(t, "internal/db", 30)
	const name = "internal/db/zz_write_policy_canary.go"
	if _, taken := files[name]; taken {
		t.Fatalf("%s exists: this test adds an in-memory file of that name", name)
	}
	files[name] = "package db\n\nimport \"context\"\n\n" +
		"const canaryLiteral = `UPDATE " + table + " SET literal_col = $1 WHERE id = $2`\n\n" +
		"const canaryHead = `UPDATE " + table + " SET `\n\n" +
		"func canaryAssembled() string { return canaryHead + `assembled_col = $1 WHERE id = $2` }\n\n" +
		"func canaryArgument(ctx context.Context, tx rotationExecer) {\n\ttx.Exec(ctx, \"UPDATE " + table + " SET argument_col = $1 WHERE id = $2\")\n}\n\n" +
		"func canaryHole(ctx context.Context, tx rotationExecer, v string) {\n\ttx.Exec(ctx, canaryHead+`hole_col = LOWER(`+v+`) WHERE id = $1`)\n}\n\n" +
		"func canaryUnread(ctx context.Context, tx rotationExecer, j struct{ sql string }) {\n\ttx.Exec(ctx, j.sql)\n}\n" +
		"func canaryFragment(v string) string { return canaryHead + `fragment_col = ` + v }\n"
	// One more per call that runs SQL, from the reader's own table: an
	// argument at that call's SQL position that cannot be read. A runner
	// added to the table is covered here without anyone remembering to,
	// and one the reader stopped knowing loses its canary.
	// The table is the LIBRARY's (0.29.211): every method of pgx, pgxpool,
	// pgconn and database/sql that takes SQL text, with the position its
	// signature gives. The canary calls each through an interface of the
	// package's own that has a method of that name with a string there,
	// which is how the store's own narrow interfaces reach them.
	entries, err := sqlscan.LibraryEntries(srctest.Root(t))
	if err != nil {
		t.Fatal(err)
	}
	type runner struct {
		Name  string
		Index int
	}
	var runners []runner
	seen := map[runner]bool{}
	for _, e := range entries {
		parts := strings.Split(e.Name, ".")
		if e.Class != sqlscan.LibRunsSQL || len(parts) != 3 {
			continue
		}
		r := runner{parts[2], e.Index}
		if !seen[r] {
			seen[r] = true
			runners = append(runners, r)
		}
	}
	if len(runners) == 0 {
		t.Fatal("the reader knows no library method that runs SQL")
	}
	for n, r := range runners {
		params := make([]string, r.Index+1)
		args := make([]string, r.Index+1)
		for i := range args {
			params[i], args[i] = "any", "nil"
		}
		params[r.Index], args[r.Index] = "string", "j.sql"
		typ := fmt.Sprintf("canaryRunner%d", n)
		files[name] += "\ntype " + typ + " interface{ " + r.Name + "(" + strings.Join(params, ", ") + ", ...any) }\n" +
			"func " + typ + r.Name + "(x " + typ + ", j struct{ sql string }) {\n\tx." + r.Name + "(" + strings.Join(args, ", ") + ")\n}\n"
	}
	registry := append([]sqlscan.Registered{}, columnWritePolicies...)
	for _, col := range []string{"literal_col", "assembled_col", "argument_col", "hole_col", "fragment_col"} {
		registry = append(registry, sqlscan.Registered{Table: table, Column: col, Policy: sqlscan.FillEmptyOnly, Reason: "canary"})
	}
	stmts, unread := columnWriteStatementsOf(t, files, registry)
	rep := sqlscan.Check(stmts, registry, columnWriteExceptions)
	caught := map[string]bool{}
	for _, v := range rep.Violations {
		if v.Table == table {
			caught[v.Column] = true
		}
	}
	for _, u := range unread.Violations {
		if u.File == name {
			caught["unread:"+u.Decl] = true
		}
	}
	for n, r := range runners {
		if !caught[fmt.Sprintf("unread:canaryRunner%d%s", n, r.Name)] {
			t.Errorf("the canary for an unread argument of %s was not caught: the reader knows that call by name and did not report what it was given", r.Name)
		}
	}
	for want, what := range map[string]string{
		"literal_col":           "a statement written as one backtick literal (sqlscan.Statements)",
		"assembled_col":         "a statement assembled from fixed text (sqlscan.Assemble)",
		"argument_col":          "a statement that is the SQL argument of a call, in a quoted string",
		"unread:canaryHole":     "an SQL argument with a part that cannot be read (a violation wherever the part stands)",
		"unread:canaryUnread":   "an SQL argument with no fixed text",
		"unread:canaryFragment": "an expression that is no argument and shows a write to a registered table with a part unread",
	} {
		if !caught[want] {
			t.Errorf("the canary for %s was not caught (%s): statements of that kind are not reaching the policy check", what, want)
		}
	}
}

// The store's package is not the only one that can run SQL: any package
// holding a pool can (PostgresStore.Pool(), the data-test tooling, the test
// harness). The write-policy check reads internal/db; this reads EVERY
// OTHER package of the repository with the same typed finder, and fails
// when one of them writes a registered column against its policy, or runs
// an argument that cannot be read whole and can be seen to write a
// registered table. (0.29.211, review round 4: the package comment said
// what could hide inside the store and nothing about outside it.)
//
// What it does not do: an argument outside the store with no part that
// names a registered table is not a violation here (the data-test tools
// build many from catalog names). The count below says how many there are.
func TestNoPackageOutsideTheStoreWritesAProtectedColumnUnchecked(t *testing.T) {
	root := srctest.Root(t)
	dirs := map[string]map[string]string{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			name := d.Name()
			if rel != "." && (strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") || name == "testdata" || name == "node_modules" || name == "vendor" || rel == "docs" || rel == "internal/db") {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		dir := filepath.ToSlash(filepath.Dir(rel))
		if dirs[dir] == nil {
			dirs[dir] = map[string]string{}
		}
		dirs[dir][rel] = string(b)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registered := map[string]bool{}
	for _, r := range columnWritePolicies {
		registered[r.Table] = true
	}
	writes := regexp.MustCompile(`(?i)\b(INSERT\s+INTO|UPDATE|DELETE\s+FROM|MERGE\s+INTO|COPY)\b`)
	packages, arguments, unreadArgs := 0, 0, 0
	for dir, files := range dirs {
		runs := false
		for _, src := range files {
			if strings.Contains(src, `"github.com/jackc/pgx/v5`) || strings.Contains(src, `"database/sql"`) || strings.Contains(src, ".Pool()") {
				runs = true
			}
		}
		if !runs {
			continue
		}
		whole, unread, err := sqlscan.AssembleTyped(root, files)
		if err != nil {
			t.Errorf("%s: %v", dir, err)
			continue
		}
		n, err := sqlscan.SQLArguments(root, files)
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			continue
		}
		packages++
		arguments += n
		stmts := append(sqlscan.Statements(files), whole...)
		counted := map[string]bool{}
		for _, u := range unread {
			if !u.SQLArg {
				continue
			}
			if at := fmt.Sprintf("%s:%d:%d", u.File, u.Line, u.Call); !counted[at] {
				counted[at] = true
				unreadArgs++
			}
			stmts = append(stmts, sqlscan.Stmt{File: u.File, SQL: u.Text})
			if !writes.MatchString(u.Text) {
				continue
			}
			for table := range registered {
				if strings.Contains(u.Text, table) || strings.Contains(u.Text, strings.TrimPrefix(table, "aveloxis_data.")+" ") {
					t.Errorf("%s:%d (%s): an SQL argument outside internal/db that cannot be read whole (%s) and can be seen to write %s, whose columns have write policies. Move the statement into the store, where the policy check reads it:\n  %s",
						u.File, u.Line, u.Decl, u.Why, table, strings.Join(strings.Fields(u.Text), " "))
				}
			}
		}
		for _, v := range sqlscan.Check(stmts, columnWritePolicies, nil).Violations {
			t.Errorf("%s: a statement outside internal/db writes %s.%s against its policy (%s):\n  expr: %s\n  %s\n  Move the write into the store.", v.File, v.Table, v.Column, v.Want, v.Expr, v.Detail)
		}
	}
	// The reading of internal/db took the values of some EXPORTED maps and
	// lists as every value a lookup can give (an ORDER BY from
	// PackageExposureSorts). Nothing in the store writes them, which the
	// reader checks by object; another package could. None does: every
	// mention of one outside the store is a read (an index, a range, len).
	relied, scalars, err := sqlscan.ExportedRelied(root, srctest.PackageFiles(t, "internal/db", 30))
	if err != nil {
		t.Fatal(err)
	}
	// The same for an exported string VARIABLE read as its declared value
	// (ToolVersion): no other package assigns it or takes its address.
	for _, name := range scalars {
		assigns := regexp.MustCompile(`\b\w+\.` + regexp.QuoteMeta(name) + `\s*(=[^=]|\+=)|&\s*\w+\.` + regexp.QuoteMeta(name) + `\b`)
		for _, files := range dirs {
			for file, src := range files {
				if m := assigns.FindString(srctest.StripGoComments(src)); m != "" {
					t.Errorf("%s: %q gives the store's exported %s another value (or takes its address): the write-policy reader reads statements built with it as built with its declared value", file, m, name)
				}
			}
		}
	}
	if len(relied) == 0 {
		t.Error("the reader relied on no exported map or list of the store: PackageExposureSorts and SupplyChainViewNames are two, and this check has nothing to look for")
	}
	for _, name := range relied {
		mention := regexp.MustCompile(`\b\w+\.` + regexp.QuoteMeta(name) + `\b`)
		for _, files := range dirs {
			for file, src := range files {
				src = srctest.StripGoComments(src)
				for _, loc := range mention.FindAllStringIndex(src, -1) {
					if why := writesExported(src[:loc[0]], src[loc[1]:]); why != "" {
						t.Errorf("%s: %s of the store's exported %s, whose values the write-policy reader takes as fixed (they reach a statement's text). Give the other package its own copy", file, why, name)
					}
				}
			}
		}
	}
	// The denominator: the finder saw the calls the repository has. The
	// floor is one package and one argument; the real numbers are logged
	// for the ledger.
	if packages == 0 || arguments == 0 {
		t.Fatalf("no SQL argument was found outside internal/db (%d packages, %d arguments): the scheduler alone runs several through Pool()", packages, arguments)
	}
	t.Logf("outside internal/db: %d packages run SQL, at %d SQL arguments; %d places there are not read whole or are reported references (none writes a registered table)", packages, arguments, unreadArgs)
}

// The true size of what the registrations cover, for the ledger and for
// anyone asking how much is taken on a person's word (review round 4: the
// changelog's counts were wrong). Nothing here is asserted but that the
// numbers exist; they are printed with -v.
func TestWritePolicyRegistrationCounts(t *testing.T) {
	root, files := srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30)
	args, err := sqlscan.SQLArguments(root, files)
	if err != nil {
		t.Fatal(err)
	}
	_, unread, err := sqlscan.AssembleTyped(root, files)
	if err != nil {
		t.Fatal(err)
	}
	type site struct {
		decl string
		call int
	}
	sites, decls, alternatives := map[site]bool{}, map[string]bool{}, 0
	for _, u := range unread {
		if u.SQLArg {
			sites[site{u.Decl, u.Call}] = true
			decls[u.Decl] = true
			alternatives++
		}
	}
	shapeArgs := 0
	for _, sh := range columnWriteShapes {
		shapeArgs += len(sh.Args)
	}
	if args == 0 || len(sites) == 0 {
		t.Fatalf("the finder sees %d SQL arguments and %d unread ones: it is not reading the package", args, len(sites))
	}
	t.Logf("internal/db: %d SQL arguments; %d of them (in %d declarations, %d alternative texts) are not read whole and are registered: %d fills, %d shape entries in %d shapes, %d exceptions",
		args, len(sites), len(decls), alternatives, len(columnWriteFills(t)), shapeArgs, len(columnWriteShapes), len(columnWriteUnreadableExceptions))
}

// Enforces SR-11 (scripts/standing_rules.go).
func TestColumnWritePolicies(t *testing.T) {
	stmts, _ := columnWriteStatements(t)
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

// writesExported says how a mention of an exported map or list, given the
// source before and after it, is anything but a read: "" for an index that
// is read, a range over it, or len of it.
func writesExported(before, after string) string {
	before = strings.TrimRight(before, " \t")
	switch {
	case strings.HasSuffix(before, "&"):
		return "the address is taken"
	case strings.HasSuffix(before, "range") || strings.HasSuffix(before, "len("):
		return ""
	}
	rest := strings.TrimLeft(after, " \t")
	if !strings.HasPrefix(rest, "[") {
		return "a use that is not an index, a range or len (the value could be kept and changed)"
	}
	depth := 0
	for i, r := range rest {
		switch r {
		case '[':
			depth++
		case ']':
			depth--
		}
		if depth == 0 {
			rest = strings.TrimLeft(rest[i+1:], " \t")
			break
		}
	}
	for _, op := range []string{"==", "!="} {
		if strings.HasPrefix(rest, op) {
			return ""
		}
	}
	for _, op := range []string{"=", "+=", ":=", "++", "--", "["} {
		if strings.HasPrefix(rest, op) {
			return "an element is assigned (or the value is a nested index this check does not read)"
		}
	}
	return ""
}

func TestWritesExportedTellsAReadFromAWrite(t *testing.T) {
	for src, wantWrite := range map[string]bool{
		"x := db.M[k]": false, "if db.M[k] == \"a\" {": false, "for _, v := range db.M {": false, "n := len(db.M)": false,
		"db.M[k] = v": true, "db.M[k] += v": true, "p := &db.M": true, "db.M = nil": true, "m := db.M": true, "delete(db.M, k)": true, "f(db.M)": true,
	} {
		i := strings.Index(src, "db.M")
		if got := writesExported(src[:i], src[i+len("db.M"):]) != ""; got != wantWrite {
			t.Errorf("%q: read as a write %v, want %v", src, got, wantWrite)
		}
	}
}
