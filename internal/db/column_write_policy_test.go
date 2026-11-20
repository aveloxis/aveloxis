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
	"go/ast"
	"go/build"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strconv"
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

// packageKeyFillTablesChecked are the tables the package-key fill walks.
var packageKeyFillTablesChecked = []PackageKeyFillTable{PackageKeyFillInventory, PackageKeyFillLockfile}

// packageKeyFillSQL's statements are judged as the function's own results
// for these tables (columnWriteSites runs it); this keeps the function to
// one return per table and the refusal.
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
	if strings.Count(body, "_, pageSQL, writeSQL, err := packageKeyFillSQL(table)") != 1 {
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
	// 0.29.212: the two statements are built by tableRowCountSQL and
	// populatedCountsSQL; the names are quoted there.
	for _, fn := range []string{"func tableRowCountSQL(", "func populatedCountsSQL("} {
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
	// Each call is wrong in ONE input, the one under test; the others are
	// values the function accepts, taken from the lists it reads (0.29.212,
	// S5-1: the metric here was "commits", which is no key of
	// metricSeriesSQL, so the call failed on the metric whatever the
	// bucket check did, and removing the bucket check failed nothing).
	metrics := make([]string, 0, len(metricSeriesSQL))
	for m := range metricSeriesSQL {
		metrics = append(metrics, m)
	}
	sort.Strings(metrics)
	if len(metrics) == 0 {
		t.Fatal("metricSeriesSQL has no metric: nothing valid to call MetricWeeklySeries with")
	}
	if _, ok := seriesBuckets["week"]; !ok {
		t.Fatal(`"week" is not one of seriesBuckets: the metric case below would fail on its bucket`)
	}
	if _, ok := seriesBuckets["day"]; ok {
		t.Fatal(`"day" is one of seriesBuckets: the bucket case below tests nothing`)
	}
	for _, metric := range metrics {
		if _, err := s.MetricWeeklySeries(ctx, []int64{1}, metric, "day", time.Time{}, time.Time{}); err == nil {
			t.Errorf("MetricWeeklySeries(%q) took a bucket that is neither week nor month", metric)
		}
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
	if strings.Count(body, "sql := goneClearSQL(c)") != 1 || len(gives) != 1 || strings.Contains(body, "&sql") {
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

// Enforces SR-11 (scripts/standing_rules.go).
func TestColumnWritePolicies(t *testing.T) {
	stmts := columnWriteReading(t).Statements
	if len(stmts) == 0 {
		t.Fatal("no statement reached the policy check")
	}
	t.Logf("%d whole statements of the store judged", len(stmts))
	for _, f := range columnWriteFailures(stmts) {
		t.Error(f)
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

// columnWriteFailures is everything the policy check holds against the
// statements: the one function the real test and the canary both report
// through, so that a kind of failure dropped here is dropped in neither
// without the canary saying so.
func columnWriteFailures(stmts []sqlscan.Stmt) []string {
	rep := sqlscan.Check(stmts, columnWritePolicies, columnWriteExceptions)
	var out []string
	for _, u := range rep.Unparsed {
		out = append(out, fmt.Sprintf("%s: a statement the policy check did not read to its end (%s), so nothing was checked on it:\n  %.300s", u.File, u.Detail, strings.Join(strings.Fields(u.SQL), " ")))
	}
	for _, v := range rep.Violations {
		out = append(out, fmt.Sprintf("%s.%s in %s violates %s:\n  expr: %s\n  %s\n  TRIAGE (never weaken a matcher): fix the SQL | correct the registered policy | add an Exception WITH a reason",
			v.Table, v.Column, v.File, v.Want, v.Expr, v.Detail))
	}
	for _, ex := range rep.StaleExceptions {
		out = append(out, fmt.Sprintf("stale exception %s.%s (%s: %q): it suppressed nothing this run; delete it (reason was: %s)",
			ex.Table, ex.Column, ex.File, ex.Match, ex.Reason))
	}
	for _, u := range rep.Unwritten {
		out = append(out, fmt.Sprintf("registered column %s.%s has NO writer statement: registry rot (renamed column?) or a dark column (the column-writer tripwire's territory)",
			u.Table, u.Column))
	}
	return out
}

// envFunc hands one of the store's real text builders to the reading: the
// reading calls IT, with the constants the source calls it with.
func envFunc(t testing.TB, name string, f any) func([]string) string {
	v := reflect.ValueOf(f)
	ft := v.Type()
	if ft.Kind() != reflect.Func || ft.IsVariadic() || ft.NumOut() != 1 || ft.Out(0).Kind() != reflect.String {
		t.Fatalf("%s is not a function of strings that returns a string", name)
	}
	for i := 0; i < ft.NumIn(); i++ {
		if ft.In(i).Kind() != reflect.String {
			t.Fatalf("%s: parameter %d is not a string", name, i+1)
		}
	}
	return func(args []string) string {
		if len(args) != ft.NumIn() {
			t.Fatalf("%s is called with %d arguments and takes %d", name, len(args), ft.NumIn())
		}
		in := make([]reflect.Value, len(args))
		for i, a := range args {
			in[i] = reflect.ValueOf(a).Convert(ft.In(i))
		}
		return v.Call(in)[0].String()
	}
}

// columnWriteEnv is the store's own text that statements are built from:
// the embedded files and the functions that make a piece of SQL from
// string constants. Each entry IS the code (sqlscan.Env).
func columnWriteEnv(t testing.TB) sqlscan.Env {
	funcs := map[string]any{
		"bucketSQL": bucketSQL, "clientAddrVisibleSQL": clientAddrVisibleSQL, "backgroundBackendSQL": backgroundBackendSQL,
		"appNamePrefixSQL": appNamePrefixSQL, "effectiveProviderSQL": effectiveProviderSQL, "resolvedRepoIDSQL": resolvedRepoIDSQL,
		"itemRepoIDSQL": itemRepoIDSQL, "itemKeySQL": itemKeySQL, "ownerLinksURL": ownerLinksURL, "repoScopeSQL": repoScopeSQL,
		"packageExposureSQL": packageExposureSQL, "packageAdvisorySQL": packageAdvisorySQL, "exposureSelectColumns": exposureSelectColumns,
		"stampLinkedOwnerSQL": stampLinkedOwnerSQL, "deleteSessionsSQL": deleteSessionsSQL, "deleteSessionTreeSQL": deleteSessionTreeSQL,
		"backendOnThisHostSQL": backendOnThisHostSQL, "probingSessionSQL": probingSessionSQL,
		"apiTokenDailyLimitColumnType": apiTokenDailyLimitColumnType,
		"forgeIDChangesListSQL":        forgeIDChangesListSQL, "forgeIDChangesAdoptedSQL": forgeIDChangesAdoptedSQL,
	}
	env := sqlscan.Env{
		Embedded: map[string]string{"schemaSQL": schemaSQL, "viewsSQL": viewsSQL, "matviewsSQL": matviewsSQL},
		Funcs:    map[string]func([]string) string{},
	}
	for name, f := range funcs {
		env.Funcs[name] = envFunc(t, name, f)
	}
	return env
}

// storeTestNames are the names of the store package's tests, read from its
// test files: what a registration's refusal test must be one of.
func storeTestNames(t testing.TB) map[string]bool {
	names := map[string]bool{}
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	decl := regexp.MustCompile(`(?m)^func (Test\w+)\(t \*testing\.T\) \{`)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range decl.FindAllStringSubmatch(string(b), -1) {
			names[m[1]] = true
		}
	}
	return names
}

// columnWriteReading is the reading of the store: every SQL argument of
// internal/db, classified, joined with the registrations of those that
// were not read. Its statements are what the policy check judges.
func columnWriteReading(t testing.TB) sqlscan.SiteReport {
	t.Helper()
	return columnWriteReadingOf(t, srctest.PackageFiles(t, "internal/db", 30), columnWriteSites(t))
}

func columnWriteReadingOf(t testing.TB, files map[string]string, sites []sqlscan.Site) sqlscan.SiteReport {
	t.Helper()
	root, env := srctest.Root(t), columnWriteEnv(t)
	args, err := sqlscan.StrictArguments(root, files, env)
	if err != nil {
		t.Fatal(err)
	}
	pins, err := sqlscan.StrictPins(root, files, env)
	if err != nil {
		t.Fatal(err)
	}
	// A registered builder covers every SQL argument that is exactly a
	// call of it (or a local taken once from one): the registration is the
	// builder's, not the call site's.
	registered := map[string]bool{}
	for _, s := range sites {
		registered[fmt.Sprintf("%s#%d", s.Decl, s.Ordinal)] = true
	}
	builders := columnWriteBuilders(t)
	for _, a := range args {
		b, ok := builders[a.Builder]
		if a.Kind != sqlscan.StrictHole || !ok || registered[fmt.Sprintf("%s#%d", a.Decl, a.Ordinal)] {
			continue
		}
		result := a.BuilderResult
		sites = append(sites, sqlscan.Site{Decl: a.Decl, Ordinal: a.Ordinal, Builder: a.Builder, Result: result, Tuples: b.Tuples, FromCallers: b.FromCallers,
			Run:    func(in []string) (string, error) { return b.Run(result, in) },
			Domain: b.Domain, Open: b.Open, RefusalTest: b.RefusalTest, Quoters: columnWriteQuoters, Raw: b.Raw, Reason: b.Reason})
	}
	return sqlscan.CheckSites(args, sites, pins, storeTestNames(t))
}

// No SQL argument of the store goes unread: each is a compiler constant,
// a pure call on constants, built from the store's own text by running it,
// a parameter read at its function's calls, or registered. (The name is
// kept: the contributor guide and the lens file cite it.)
func TestNoProtectedWriteGoesUnread(t *testing.T) {
	rep := columnWriteReading(t)
	for _, p := range rep.Problems {
		t.Errorf("%s\n  TRIAGE, in this order:\n"+
			"  1. Make the statement a constant: a value belongs in a placeholder ($1), never in the text.\n"+
			"  2. Build it with one named function of the store from string constants, and add that function to columnWriteEnv.\n"+
			"  3. Register it in columnWriteSites as EXECUTED: name the builder, give the statements running it gives over its whole domain, and name the test that shows it refuses any other input.\n"+
			"  4. FROZEN (last, with the operator's decision): say what the statement is, and record the digest printed above.", p)
	}
	if rep.Frozen != 0 {
		t.Errorf("%d SQL arguments are registered FROZEN (read by a person, not run): the store has none, and one needs the operator's decision", rep.Frozen)
	}
	if rep.Raw != 1 {
		t.Errorf("%d builder parameters reach a statement's text unquoted by design; the store has ONE (contentWherePassingSQL's where, an operator's predicate): another needs the operator's decision", rep.Raw)
	}
	t.Logf("SQL arguments of internal/db: %d constants, %d pure calls on constants, %d built by running the store's own text, %d fixed text with a number in it, %d fixed text filled with its callers' constants, %d queued statements read back, %d parameters read at their callers; registered: %d executed, %d open, %d frozen",
		rep.Constant, rep.Pure, rep.Built, rep.Numbered, rep.FromCallers, rep.Queued, rep.Parameter, rep.Executed, rep.Open, rep.Frozen)
}

// columnWriteFrozen holds the digest of the source the two libyear
// statement variables are computed from (the Pin of their registrations):
// a change to it fails TestNoProtectedWriteGoesUnread with the new digest;
// read how the variable is computed before recording it.
var columnWriteFrozen = map[string]string{
	"PostgresStore.ReplaceRepoLibyear": "542965e8c1f0c231/16",
}

// columnWriteSites registers the SQL arguments of the store that are a
// package VARIABLE computed at start (a function of the store's own lists):
// EXECUTED, the statement judged being the real variable, with a Pin on the
// source it is computed from. Every other unread argument is a call of a
// registered builder (columnWriteBuilders), which the reading runs. Nothing
// is FROZEN (TestNoProtectedWriteGoesUnread fails on one).
func columnWriteSites(t testing.TB) []sqlscan.Site {
	var sites []sqlscan.Site
	sites = append(sites,
		sqlscan.Site{Decl: "PostgresStore.ReplaceRepoLibyear", Ordinal: 1, Builder: "rotateLibyearInsertSQL", Statements: []string{rotateLibyearInsertSQL},
			Domain: "no input: the variable's one value", Pin: columnWriteFrozen["PostgresStore.ReplaceRepoLibyear"], Reason: "the INSERT that rotates a repository's libyear rows to history"},
		sqlscan.Site{Decl: "PostgresStore.ReplaceRepoLibyear", Ordinal: 2, Builder: "replaceLibyearDeleteAndCarrySQL", Statements: []string{replaceLibyearDeleteAndCarrySQL},
			Domain: "no input: the variable's one value", Pin: columnWriteFrozen["PostgresStore.ReplaceRepoLibyear"], Reason: "the DELETE ... RETURNING and carrying INSERT of a libyear replacement, run through Query for its rows"})
	return sites
}

// columnWriteReferences are the store's references to something that runs
// SQL which no reading of arguments covers, each read by a person.
var columnWriteReferences = []struct{ Decl, Why, Reason string }{
	{Decl: "freshBatch", Why: "a batch made with its QueuedQueries given",
		Reason: "freshBatch copies a batch for a re-send: the list starts empty (make) and takes only copies of the queries queued on the batch it is given, each of which a Queue call in this package put there"},
	{Decl: "freshBatch", Why: "QueuedQueries used other than to read it",
		Reason: "the same copy: fresh.QueuedQueries = append(fresh.QueuedQueries, a copy of q), for q ranging over the given batch's own list"},
}

// The store refers to nothing that runs SQL in a way the reading does not
// cover (a runner taken as a value, a call that writes rows with no text,
// a library type that carries SQL arriving from or leaving to other code),
// but for the references registered above.
func TestNoSQLReachesTheLibraryUnseen(t *testing.T) {
	refs, err := sqlscan.StrictReferences(srctest.Root(t), srctest.PackageFiles(t, "internal/db", 30))
	if err != nil {
		t.Fatal(err)
	}
	used := make([]bool, len(columnWriteReferences))
	for _, r := range refs {
		ok := false
		for i, a := range columnWriteReferences {
			if a.Decl == r.Decl && strings.Contains(r.Why, a.Why) {
				used[i], ok = true, true
			}
		}
		if !ok {
			t.Errorf("%s:%d (%s): %s", r.File, r.Line, r.Decl, r.Why)
		}
	}
	for i, a := range columnWriteReferences {
		if !used[i] {
			t.Errorf("stale reference registration %s (%q): it covers nothing; delete it", a.Decl, a.Why)
		}
		if a.Reason == "" {
			t.Errorf("reference registration %s (%q) gives no reason", a.Decl, a.Why)
		}
	}
}

// The store's package is not the only one that can run SQL. This reads
// EVERY OTHER package of the module with the same finder (a package is
// one that runs SQL when the finder finds a call of a classified library
// function in it, by type; no text filter chooses the packages).
//
// What it proves, exactly: (1) every SQL argument outside internal/db
// that is a compiler constant, or a pure call on constants, was parsed
// and judged against the registered policies like the store's own, and
// none violates one or failed to be read to its end; (2) no argument
// outside the store that is NOT read holds, among the compiler constants
// it is made of, both a word that writes and the name of a registered
// table (matched as SQL names, case folded). What it does not prove: that
// an unread argument outside the store, whose table name arrives at run
// time, writes no registered column. The count of unread arguments is
// logged; the store's package is the place for writes.
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
		// The files this platform builds, as the compiler chooses them.
		if ok, err := build.Default.MatchFile(filepath.Dir(path), d.Name()); err != nil || !ok {
			return err
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
	bare := map[string]string{}
	for _, r := range columnWritePolicies {
		bare[r.Table[strings.LastIndex(r.Table, ".")+1:]] = r.Table
	}
	writeWords := map[string]bool{"update": true, "insert": true, "delete": true, "merge": true, "copy": true, "truncate": true, "alter": true}
	read, packages, arguments, unread := 0, 0, 0, 0
	for dir, files := range dirs {
		args, err := sqlscan.StrictArguments(root, files, sqlscan.Env{})
		if err != nil {
			t.Errorf("%s: %v", dir, err)
			continue
		}
		read++
		if len(args) == 0 {
			continue
		}
		packages++
		arguments += len(args)
		var stmts []sqlscan.Stmt
		for _, a := range args {
			switch a.Kind {
			case sqlscan.StrictConstant, sqlscan.StrictPure:
				stmts = append(stmts, sqlscan.Stmt{File: a.File, SQL: a.Text})
			case sqlscan.StrictHole:
				unread++
				writes, table := false, ""
				for _, f := range a.Fragments {
					for _, w := range sqlscan.FragmentWords(f) {
						writes = writes || writeWords[w]
						if full, ok := bare[w]; ok {
							table = full
						}
					}
				}
				if writes && table != "" {
					t.Errorf("%s:%d (%s): an SQL argument outside internal/db that is not read, and whose fixed text both writes and names %s, whose columns have write policies. Move the statement into the store:\n  %s",
						a.File, a.Line, a.Decl, table, a.Expr)
				}
			}
		}
		rep := sqlscan.Check(stmts, columnWritePolicies, nil)
		for _, v := range rep.Violations {
			t.Errorf("%s: a statement outside internal/db writes %s.%s against its policy (%s):\n  expr: %s\n  %s\n  Move the write into the store.", v.File, v.Table, v.Column, v.Want, v.Expr, v.Detail)
		}
		for _, u := range rep.Unparsed {
			t.Errorf("%s: a statement outside internal/db that the policy check did not read to its end (%s):\n  %.200s", u.File, u.Detail, strings.Join(strings.Fields(u.SQL), " "))
		}
	}
	if packages == 0 || arguments == 0 {
		t.Fatalf("no SQL argument was found outside internal/db (%d packages read): the scheduler alone runs several", read)
	}
	t.Logf("outside internal/db: %d packages read by type, %d of them run SQL, at %d SQL arguments; %d of those are not read (none holds a write word and a registered table's name)", read, packages, arguments, unread)
}

// The denominator, without a number to keep. "No violation" proves nothing
// if the reading stopped: so a file of the store's package is added, in
// memory, with a forbidden write as a constant, one queued on a batch, one
// through a helper's parameter, and an argument that cannot be read. Each
// must come back THROUGH columnWriteReadingOf and sqlscan.Check, the
// functions the real tests use, beside the store's real files.
func TestTheWritePolicyCheckSeesWhatIsAddedToTheStore(t *testing.T) {
	files := map[string]string{}
	for name, src := range srctest.PackageFiles(t, "internal/db", 30) {
		files[name] = src
	}
	const canary = "internal/db/zz_canary.go"
	files[canary] = `package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func zzCanaryConstant(ctx context.Context, s *PostgresStore) {
	s.pool.Exec(ctx, "UPDATE aveloxis_data.repos SET platform_repo_id = 'canary-constant' WHERE repo_id = $1")
}

func zzCanaryBatch(ctx context.Context, s *PostgresStore) {
	b := &pgx.Batch{}
	b.Queue("UPDATE aveloxis_data.repos SET platform_repo_id = 'canary-batch' WHERE repo_id = $1")
	s.pool.SendBatch(ctx, b)
}

func zzCanaryHelper(ctx context.Context, s *PostgresStore, q string) { s.pool.Exec(ctx, q) }

func zzCanaryCaller(ctx context.Context, s *PostgresStore) {
	zzCanaryHelper(ctx, s, "UPDATE aveloxis_data.repos SET platform_repo_id = 'canary-helper' WHERE repo_id = $1")
}

func zzCanaryUnparsed(ctx context.Context, s *PostgresStore) {
	s.pool.Exec(ctx, "UPDATE aveloxis_data.repos SET platform_repo_id /* canary-unparsed */")
}

func zzCanaryHole(ctx context.Context, s *PostgresStore, table string) {
	s.pool.Exec(ctx, "UPDATE "+table+" SET platform_repo_id = 'canary-hole'")
}
`
	rep := columnWriteReadingOf(t, files, columnWriteSites(t))
	hole := false
	for _, p := range rep.Problems {
		if strings.Contains(p, "zzCanaryHole#1") && strings.Contains(p, "not read and not registered") {
			hole = true
		} else {
			t.Errorf("a problem that is not the canary's: %s", p)
		}
	}
	if !hole {
		t.Error("an SQL argument that cannot be read, added to the store, was not reported")
	}
	failures := columnWriteFailures(rep.Statements)
	for _, f := range failures {
		if !strings.Contains(f, canary) {
			t.Errorf("a failure that is not the canary's: %s", f)
		}
	}
	all := strings.Join(failures, "\n")
	seen := map[string]bool{}
	for _, want := range []string{"'canary-constant'", "'canary-batch'", "'canary-helper'", "canary-unparsed"} {
		seen[want] = strings.Contains(all, want)
	}
	for _, want := range []string{"'canary-constant'", "'canary-batch'", "'canary-helper'", "canary-unparsed"} {
		if !seen[want] {
			t.Errorf("the forbidden or unreadable statement %s, added to the store, was not reported", want)
		}
	}
}

// The gone-clear builders are run for every goneClear in the store's
// source: each literal a plain reading of repo_gone.go finds is one of the
// inputs, as written (0.29.212, S5s-9: a fill that left values out went
// unnoticed).
func TestTheGoneClearSitesAreJudgedForEveryGoneClear(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/repo_gone.go"))
	literals := regexp.MustCompile(`goneClear\{[^}]*\}`).FindAllString(src, -1)
	if len(literals) < 5 {
		t.Fatalf("%d goneClear literals found in repo_gone.go; the store has had five", len(literals))
	}
	b := columnWriteBuilders(t)
	for _, name := range []string{"goneClearSQL", "goneClearLockSQL"} {
		inputs := map[string]bool{}
		for _, row := range b[name].Tuples {
			inputs[row[0]] = true
		}
		for _, lit := range literals {
			if !inputs[lit] {
				t.Errorf("%s is not run for %s", name, lit)
			}
		}
	}
}

// sqlBuilder registers one of the store's functions that build a statement
// from inputs: the inputs it is run with here, and how it is run.
type sqlBuilder struct {
	// Tuples are the inputs: the whole domain, or (Open) representative
	// inputs that reach every branch.
	Tuples [][]string
	// Run calls the real function and gives the result'th of its results.
	Run func(result int, args []string) (string, error)
	// Domain says what the inputs are. Open, when they are not a finite
	// set, says what varies and why it cannot name a protected column.
	// RefusalTest names the test showing an input outside a finite domain
	// produces no statement.
	Domain, Open, RefusalTest, Reason string
	// FromCallers: the inputs are the constants every caller passes,
	// which the reading finds (sqlscan.Site.FromCallers).
	FromCallers bool
	// Raw: parameters that reach the text unquoted by design, each with
	// what confines it (sqlscan.Site.Raw).
	Raw map[string]string
}

// runSQLBuilder runs f, one of the store's builders, with inputs written as
// text: each is converted to the parameter's type (a string, a number, a
// bool, or a declared type of one of those).
func runSQLBuilder(f any) func(result int, args []string) (string, error) {
	v := reflect.ValueOf(f)
	ft := v.Type()
	return func(result int, args []string) (out string, err error) {
		if len(args) != ft.NumIn() {
			return "", fmt.Errorf("%d inputs for a builder of %d parameters", len(args), ft.NumIn())
		}
		in := make([]reflect.Value, len(args))
		for i, a := range args {
			p := reflect.New(ft.In(i)).Elem()
			switch p.Kind() {
			case reflect.String:
				p.SetString(a)
			case reflect.Bool:
				b, err := strconv.ParseBool(a)
				if err != nil {
					return "", err
				}
				p.SetBool(b)
			case reflect.Int, reflect.Int64, reflect.Int32:
				n, err := strconv.ParseInt(a, 10, 64)
				if err != nil {
					return "", err
				}
				p.SetInt(n)
			case reflect.Slice:
				if p.Type().Elem().Kind() != reflect.String {
					return "", fmt.Errorf("parameter %d: a list of %s is not run from text", i+1, p.Type().Elem())
				}
				parts := strings.Split(a, "\x00")
				p.Set(reflect.MakeSlice(p.Type(), len(parts), len(parts)))
				for k, part := range parts {
					p.Index(k).SetString(part)
				}
			default:
				return "", fmt.Errorf("parameter %d is a %s, which is not run from text", i+1, p.Kind())
			}
			in[i] = p
		}
		// A builder that refuses by panicking (an index outside its list)
		// refuses here too.
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("the builder panicked: %v", r)
			}
		}()
		res := v.Call(in)
		if last := res[len(res)-1]; last.Type().Implements(reflect.TypeOf((*error)(nil)).Elem()) && !last.IsNil() {
			return "", last.Interface().(error)
		}
		if result >= len(res) || res[result].Kind() != reflect.String {
			return "", fmt.Errorf("result %d of the builder is not a string", result)
		}
		return res[result].String(), nil
	}
}

// indexedBuilders are the builders that take the index of a member of a
// fixed list of the store (the tables a rename rewrites, the foreign keys
// a migration alters): each with the length of its list. The domain is
// every index; an index outside it panics before any statement exists.
func indexedBuilders() map[string]struct {
	n int
	f func(int) string
} {
	return map[string]struct {
		n int
		f func(int) string
	}{
		"repoURLRewriteSQL":                 {len(repoURLColumns), repoURLRewriteSQL},
		"dataVerifyInvariantSQL":            {len(dataVerifyInvariants), dataVerifyInvariantSQL},
		"renameHealStepSQL":                 {len(renameHealSteps), renameHealStepSQL},
		"dedupPairStepSQL":                  {len(dedupPairSteps), dedupPairStepSQL},
		"loserJoinDeleteSQL":                {len(loserJoinDeletes), loserJoinDeleteSQL},
		"loserRepoIDDeleteSQL":              {len(loserRepoIDDeletes), loserRepoIDDeleteSQL},
		"loserHygieneDeleteSQL":             {len(loserHygieneDeletes), loserHygieneDeleteSQL},
		"repoGroupRepointSQL":               {len(repoGroupRepointTables), repoGroupRepointSQL},
		"repoGroupLoserRowsDeleteSQL":       {len(repoGroupLoserRowTables), repoGroupLoserRowsDeleteSQL},
		"dedupListServeStepSQL":             {len(dedupListServeSteps), dedupListServeStepSQL},
		"healAutomationPhantomStepSQL":      {len(healAutomationPhantomSteps), healAutomationPhantomStepSQL},
		"matviewRefreshConcurrentlySQL":     {len(matviewNames), matviewRefreshConcurrentlySQL},
		"matviewRefreshSQL":                 {len(matviewNames), matviewRefreshSQL},
		"supplyChainRefreshConcurrentlySQL": {len(SupplyChainViewNames), supplyChainRefreshConcurrentlySQL},
		"supplyChainRefreshSQL":             {len(SupplyChainViewNames), supplyChainRefreshSQL},
		"supplyChainViewDropSQL":            {len(supplyChainViewDefs()), supplyChainViewDropSQL},
		"supplyChainViewCreateSQL":          {len(supplyChainViewDefs()), supplyChainViewCreateSQL},
		"supplyChainViewIndexSQL":           {len(supplyChainViewDefs()), supplyChainViewIndexSQL},
		"assignmentBackfillCountSQL":        {len(assignmentBackfillTables), assignmentBackfillCountSQL},
		"assignmentBackfillMaxSQL":          {len(assignmentBackfillTables), assignmentBackfillMaxSQL},
		"assignmentBackfillUpdateSQL":       {len(assignmentBackfillTables), assignmentBackfillUpdateSQL},
		"extraFKAddSQL":                     {len(fkExtraConstraints), extraFKAddSQL},
		"extraFKValidateSQL":                {len(fkExtraConstraints), extraFKValidateSQL},
		"cntrbFullFKAddSQL":                 {len(cntrbIDChildFKs), cntrbFullFKAddSQL},
		"cntrbFullFKValidateSQL":            {len(cntrbIDChildFKs), cntrbFullFKValidateSQL},
		"msgRefDataSourceMaxSQL":            {len(msgRefDataSourceTargets), msgRefDataSourceMaxSQL},
		"msgRefDataSourceWindowSQLAt":       {len(msgRefDataSourceTargets), msgRefDataSourceWindowSQLAt},
		"autovacuumInsertFactorSQL":         {len(autovacuumInsertTables), autovacuumInsertFactorSQL},
		"sessionTokenTagStepSQL":            {len(sessionTokenTagSteps), sessionTokenTagStepSQL},
		"badTimestampFixSQL":                {len(badTimestampFixes), badTimestampFixSQL},
		"cntrbFKCascadeSQL":                 {len(cntrbIDChildFKs), cntrbFKCascadeSQL},
		"cntrbFKValidateSQL":                {len(cntrbIDChildFKs), cntrbFKValidateSQL},
		"cntrbIDFKIndexSQL":                 {len(cntrbIDFKIndexes), cntrbIDFKIndexSQL},
		"fkExtraIndexSQL":                   {len(fkExtraIndexes), fkExtraIndexSQL},
		"repoGroupFKIndexSQL":               {len(repoGroupFKIndexes), repoGroupFKIndexSQL},
		"emailMessageFKIndexSQL":            {len(emailMessageFKIndexes), emailMessageFKIndexSQL},
		"perfWaveIndexSQL":                  {len(perfWaveIndexDDL), perfWaveIndexSQL},
		"repoAggDeleteSQL":                  {len(repoAggQueries), repoAggDeleteSQL},
		"repoAggInsertSQL":                  {len(repoAggQueries), repoAggInsertSQL},
		"groupAggDeleteSQL":                 {len(groupAggQueries), groupAggDeleteSQL},
		"groupAggInsertSQL":                 {len(groupAggQueries), groupAggInsertSQL},
	}
}

func TestAnIndexedStatementBuilderRefusesAnIndexOutsideItsList(t *testing.T) {
	for name, b := range indexedBuilders() {
		if b.n == 0 {
			t.Errorf("%s: its list is empty, so nothing is judged for it", name)
		}
		for _, i := range []int{-1, b.n} {
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("%s(%d) gave a statement for an index outside its list of %d", name, i, b.n)
					}
				}()
				_ = b.f(i)
			}()
		}
	}
}

// keyedBuilder is a builder with a finite domain it enforces itself: every
// input of the domain, and inputs outside it, each of which it must refuse.
// callerCols is how many of its leading inputs are constants its callers
// pass (the reading finds those; the domain's rows then hold a sample).
type keyedBuilder struct {
	f          any
	domain     [][]string
	bads       [][]string
	callerCols int
}

// keyedBuilders are the builders that look their input up in a fixed map
// or switch of the store and refuse anything else.
func keyedBuilders() map[string]keyedBuilder {
	keys := func(m map[string]string) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		sort.Strings(out)
		return out
	}
	one := func(ks []string) [][]string {
		var out [][]string
		for _, k := range ks {
			out = append(out, []string{k})
		}
		return out
	}
	bools := []string{"false", "true"}
	var metricByBucket, exposure, queue, listing [][]string
	for _, m := range keys(metricSeriesSQL) {
		for _, b := range keys(seriesBuckets) {
			metricByBucket = append(metricByBucket, []string{m, b})
		}
	}
	for _, sortKey := range keys(PackageExposureSorts) {
		for source := exposureFromView; source <= exposureFromLiveFleet; source++ {
			for _, eco := range bools {
				for _, search := range bools {
					exposure = append(exposure, []string{strconv.Itoa(source), eco, search, sortKey})
				}
			}
		}
	}
	for _, sortKey := range append([]string{""}, keys(queueSorts)...) {
		for _, searching := range bools {
			for _, desc := range bools {
				queue = append(queue, []string{searching, sortKey, desc})
			}
		}
	}
	const member = `SELECT repo_id FROM aveloxis_ops.user_repos WHERE group_id = $1`
	for _, sortKey := range keys(collectionRepoSorts) {
		for _, dir := range []string{"asc", "desc"} {
			listing = append(listing, []string{member, sortKey, dir})
		}
	}
	const inject = "repo_name FROM x; --"
	return map[string]keyedBuilder{
		"snapshotSeriesSQL":        {f: snapshotSeriesSQL, domain: one(keys(snapshotColumns)), bads: [][]string{{inject}}},
		"latestCountSQL":           {f: latestCountSQL, domain: one(keys(snapshotColumns)), bads: [][]string{{inject}}},
		"retentionSeriesStatement": {f: retentionSeriesStatement, domain: one(keys(seriesBuckets)), bads: [][]string{{"week; DROP TABLE x"}}},
		"metricSeriesStatement":    {f: metricSeriesStatement, domain: metricByBucket, bads: [][]string{{keys(metricSeriesSQL)[0], "day"}, {"no-such-metric", "week"}}},
		"packageExposureListSQL":   {f: packageExposureListSQL, domain: exposure, bads: [][]string{{"3", "false", "false", "repos"}, {"-1", "false", "false", "repos"}, {"0", "false", "false", inject}}},
		"queuePageSQL":             {f: queuePageSQL, domain: queue, bads: [][]string{{"false", inject, "false"}}},
		"repoListingStatement":     {f: repoListingStatement, domain: listing, bads: [][]string{{member, inject, "asc"}, {member, keys(collectionRepoSorts)[0], "sideways"}, {member, keys(collectionRepoSorts)[0], ""}}, callerCols: 1},
	}
}

func TestAKeyedStatementBuilderRefusesAKeyItDoesNotHave(t *testing.T) {
	for name, k := range keyedBuilders() {
		if len(k.domain) == 0 || len(k.bads) == 0 {
			t.Errorf("%s: no input in its domain, or none outside it", name)
			continue
		}
		run := runSQLBuilder(k.f)
		ft := reflect.TypeOf(k.f)
		for result := 0; result < ft.NumOut()-1; result++ {
			for _, in := range k.domain {
				if _, err := run(result, in); err != nil {
					t.Errorf("%s(%q) is in the domain and was refused: %v", name, in, err)
				}
			}
			for _, bad := range k.bads {
				if text, err := run(result, bad); err == nil {
					t.Errorf("%s(%q) gave a statement: %s", name, bad, text)
				}
			}
			// Each TEXT input of a domain row, changed, is refused too: the
			// refusal is per input. (A number or a bool that is not one is
			// no input at all; those have their own rows above.)
			for i := k.callerCols; i < ft.NumIn(); i++ {
				if ft.In(i).Kind() != reflect.String {
					continue
				}
				in := append([]string(nil), k.domain[len(k.domain)-1]...)
				in[i] = "no-such-key"
				if text, err := run(result, in); err == nil {
					t.Errorf("%s(%q) gave a statement: %s", name, in, text)
				}
			}
		}
	}
}

// openBuilders are the builders whose inputs are not a finite set: each
// with representative inputs that reach every branch, and the sentence
// saying what varies.
func openBuilders() map[string]struct {
	f                      any
	inputs                 [][]string
	domain, varies, reason string
} {
	const names = "names the database's catalog gives at run time; each reaches the text only as a quoted identifier"
	const number = "a number: it is written with %d, and a number is digits"
	const keyed = names + " (a key column also as a quoted literal, by sqlLiteral)"
	key := [][]string{{"aveloxis_data", "repos", "repo_id"}, {"s", `odd "t`, "a'b\x00c\"d"}}
	return map[string]struct {
		f                      any
		inputs                 [][]string
		domain, varies, reason string
	}{
		"closedBySweepSQL":     {closedBySweepSQL, [][]string{{"0"}, {"25"}}, "any limit", number, "a read"},
		"tableRowCountSQL":     {tableRowCountSQL, [][]string{{"aveloxis_data", "repos"}, {"s", `odd "t`}}, "any table of the aveloxis schemas", names, "a count"},
		"populatedCountsSQL":   {populatedCountsSQL, [][]string{{"aveloxis_data", "repos", "repo_git\x00repo_id\x00odd \"c", "text\x00bigint\x00jsonb"}}, "any table with its columns and their data types", names + "; a data type only chooses among three fixed predicates", "counts"},
		"contentTotalsSQL":     {contentTotalsSQL, [][]string{{"aveloxis_data", "repos"}, {"s", `odd "t`}}, "any table", names, "a read"},
		"contentBucketsSQL":    {contentBucketsSQL, key, "any table and its key", keyed, "a read"},
		"contentBucketRowsSQL": {contentBucketRowsSQL, key, "any table and its key", keyed, "a read"},
		"contentRowsByKeySQL":  {contentRowsByKeySQL, key, "any table and its key", keyed, "a read"},
	}
}

// boolBuilders are the builders whose every parameter is a bool: the type
// is the whole domain, and each is run for every combination.
func boolBuilders() map[string]any {
	return map[string]any{
		// (bool builders are added here)
		"packageVersionsInUseSQL":  packageVersionsInUseSQL,
		"packageExposedReposSQL":   packageExposedReposSQL,
		"topContributorsStatement": topContributorsStatement,
		"gapHealCandidatesSQL":     gapHealCandidatesSQL,
		"distributionClaimSQL":     distributionClaimSQL,
		"weeklyCommitsSQL":         weeklyCommitsSQL,
		"oauthUserByIDSQL":         oauthUserByIDSQL,
		"cachedCountsProbeSQL":     cachedCountsProbeSQL,
		"metadataDriftProbeSQL":    metadataDriftProbeSQL,
		"mailingListStripBatchSQL": mailingListStripBatchSQL,
	}
}

// columnWriteQuoters are the store's quoting helpers an open builder may
// hand a varying value to, each with the test that pins what it quotes.
var columnWriteQuoters = map[string]string{
	"quotedIdentifier": "TestCatalogNamesAreQuotedAsSQLQuotesThem",
	"quoteIdent":       "TestCatalogNamesAreQuotedAsSQLQuotesThem",
	"sqlLiteral":       "TestSQLLiteralIsOneStringToSQL",
}

// columnWriteBuilders are the store's statement builders, by name.
func columnWriteBuilders(t testing.TB) map[string]sqlBuilder {
	out := map[string]sqlBuilder{}
	for name, b := range indexedBuilders() {
		var tuples [][]string
		for i := 0; i < b.n; i++ {
			tuples = append(tuples, []string{strconv.Itoa(i)})
		}
		out[name] = sqlBuilder{Tuples: tuples, Run: runSQLBuilder(b.f), Domain: "the index of each member of the builder's fixed list",
			RefusalTest: "TestAnIndexedStatementBuilderRefusesAnIndexOutsideItsList", Reason: "one statement per member of a fixed list of the store"}
	}
	for name, f := range boolBuilders() {
		n := reflect.TypeOf(f).NumIn()
		var tuples [][]string
		for mask := 0; mask < 1<<n; mask++ {
			row := make([]string, n)
			for k := range row {
				row[k] = strconv.FormatBool(mask&(1<<k) != 0)
			}
			tuples = append(tuples, row)
		}
		out[name] = sqlBuilder{Tuples: tuples, Run: runSQLBuilder(f), Domain: "every combination of the builder's bools",
			Reason: "a statement chosen among fixed texts by conditions"}
	}
	// Builders keyed by a value of a fixed map of the store: run for every
	// key, and refused for any other (keyedBuilders).
	for name, k := range keyedBuilders() {
		tuples := k.domain
		if k.callerCols > 0 {
			// The leading inputs are the callers' constants, which the
			// reading finds; the registration gives the rest.
			seen := map[string]bool{}
			tuples = nil
			for _, row := range k.domain {
				if key := strings.Join(row[k.callerCols:], "\x00"); !seen[key] {
					seen[key] = true
					tuples = append(tuples, row[k.callerCols:])
				}
			}
		}
		out[name] = sqlBuilder{Tuples: tuples, FromCallers: k.callerCols > 0, Run: runSQLBuilder(k.f), Domain: "every input the builder accepts: the keys of the map, or the cases of the switch, it looks each up in",
			RefusalTest: "TestAKeyedStatementBuilderRefusesAKeyItDoesNotHave", Reason: "a read whose column, unit, source or order is chosen by a request value"}
	}
	// Open builders whose every text parameter reaches the statement only
	// as a quoted name (the reading checks that, by object) and whose other
	// parameters are numbers.
	for name, o := range openBuilders() {
		out[name] = sqlBuilder{Tuples: o.inputs, Run: runSQLBuilder(o.f), Domain: o.domain, Open: o.varies, Reason: o.reason}
	}
	// THE ONE DELIBERATE EXCEPTION: an operator's predicate, from the
	// data-test rules file, is put into this statement as written.
	out["contentWherePassingSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "repos", "repo_id", "t.repo_archived IS NOT TRUE"}, {"s", `odd "t`, "a\x00b", "TRUE"}},
		Run: runSQLBuilder(contentWherePassingSQL), Domain: "any table with its key, and any predicate a rule gives",
		Open:   "the table and its key, quoted; and the rule's predicate, which is NOT quoted (Raw)",
		Raw:    map[string]string{"where": "the statement runs only inside contentQuery's transaction, begun READ ONLY (pgx.TxOptions{AccessMode: pgx.ReadOnly}), and as one parameterised statement, so a second statement cannot ride on it; PostgreSQL refuses a write to a table in such a transaction. Nothing checks the predicate's text"},
		Reason: "the rows of a table a data-test rule's predicate holds for"}
	const identifiers = "names of the tables and columns being compared, from the catalog or an operator's rules file; each goes through pgx.Identifier"
	out["contentCountSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "repos"}, {"s", `odd "t`}}, Run: runSQLBuilder(contentCountSQL), Domain: "any table", Open: identifiers, Reason: "a count"}
	out["contentNotNullCountSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "repos", "repo_git"}, {"s", `odd "t`, `odd "c`}}, Run: runSQLBuilder(contentNotNullCountSQL),
		Domain: "any table and column", Open: identifiers, Reason: "a count"}
	// The gone-clear statements: run for every goneClear written in the
	// store's source (goneClearsInSource reads the literals and gives each
	// its real value). A goneClear is an unexported struct whose fields are
	// never assigned, so the literals are every one there can be.
	clears := goneClearsInSource(t)
	var clearKeys [][]string
	for lit := range clears {
		clearKeys = append(clearKeys, []string{lit})
	}
	sort.Slice(clearKeys, func(i, j int) bool { return clearKeys[i][0] < clearKeys[j][0] })
	for name, f := range map[string]func(goneClear) string{"goneClearSQL": goneClearSQL, "goneClearLockSQL": goneClearLockSQL} {
		out[name] = sqlBuilder{Tuples: clearKeys, Domain: "every goneClear literal in the store's source", RefusalTest: "TestNoFieldOfAGoneClearIsEverAssigned",
			Reason: "the gone-clear statement and its row lock",
			Run: func(_ int, in []string) (string, error) {
				c, ok := clears[in[0]]
				if !ok {
					return "", fmt.Errorf("no goneClear %q in the source", in[0])
				}
				return f(c), nil
			}}
	}
	keyTables := [][]string{}
	for _, tbl := range packageKeyFillTablesChecked {
		keyTables = append(keyTables, []string{string(tbl)})
	}
	for name, f := range map[string]any{"packageKeyFillSQL": packageKeyFillSQL, "packageKeyFillMarkerSQL": packageKeyFillMarkerSQL} {
		out[name] = sqlBuilder{Tuples: keyTables, Run: runSQLBuilder(f), Domain: "the two tables the fill walks", RefusalTest: "TestPackageKeyFillTablesAreTheFilledOnes",
			Reason: "the package-key fill's statements for one table"}
	}
	// Builders whose inputs are the constants their callers pass (the
	// reading finds them), builders that take nothing, and open builders.
	for name, f := range map[string]any{"addColumnSQL": addColumnSQL, "addQualifiedColumnSQL": addQualifiedColumnSQL, "dropSupersededIndexSQL": dropSupersededIndexSQL,
		"catalogPickSQL": catalogPickSQL} {
		out[name] = sqlBuilder{FromCallers: true, Run: runSQLBuilder(f), Domain: "the constants every caller passes", Reason: "text whose varying parts are written at its callers"}
	}
	for name, f := range map[string]any{"loginHistoryFromIdentitiesSQL": loginHistoryFromIdentitiesSQL, "loginHistoryFromContributorsSQL": loginHistoryFromContributorsSQL,
		"seedAPITokenSettingsSQL": seedAPITokenSettingsSQL} {
		out[name] = sqlBuilder{Tuples: [][]string{{}}, Run: runSQLBuilder(f), Domain: "no input", Reason: "fixed text with the running version or the shipped limits in it"}
	}
	const catalog = "names the database's catalog gives at run time; each reaches the text only through a quoting helper or pgx.Identifier, so it can be nothing but a name"
	out["reposTrgmIndexSQL"] = sqlBuilder{Tuples: [][]string{{"public"}, {`odd "schema`}}, Run: runSQLBuilder(reposTrgmIndexSQL), Domain: "the schema pg_trgm is installed in", Open: catalog, Reason: "CREATE INDEX"}
	out["toolVersionDefaultSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "repos"}, {"aveloxis_ops", `odd "name`}}, Run: runSQLBuilder(toolVersionDefaultSQL),
		Domain: "every table with a tool_version column", Open: catalog, Reason: "SET DEFAULT: not a write of stored rows"}
	out["fkDeferrableSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "issues", "fk_x"}, {"s", `odd "t`, `odd "c`}}, Run: runSQLBuilder(fkDeferrableSQL),
		Domain: "every foreign key of the aveloxis schemas that is not yet deferrable", Open: catalog, Reason: "ALTER CONSTRAINT"}
	const rows = "the number of rows in the chunk: a number, which makes placeholders ($1, $2, ...) and nothing else"
	for name, f := range map[string]any{"commitBatchInsertSQL": commitBatchInsertSQL, "commitMessageBatchUpsertSQL": commitMessageBatchUpsertSQL} {
		out[name] = sqlBuilder{Tuples: [][]string{{"1"}, {"2"}, {"500"}}, Run: runSQLBuilder(f), Domain: "any chunk size", Open: rows, Reason: "a batch upsert"}
	}
	out["analyzeTableSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "repos"}, {"s", `odd "t`}}, Run: runSQLBuilder(analyzeTableSQL),
		Domain: "the table of an expression index just built", Open: catalog, Reason: "ANALYZE: writes no row of any table"}
	out["dropInvalidIndexSQL"] = sqlBuilder{Tuples: [][]string{{"aveloxis_data", "idx_x"}, {"aveloxis_ops", `odd "name`}}, Run: runSQLBuilder(dropInvalidIndexSQL),
		Domain: "the index a caller is about to build", Open: "the index's schema and name, from its caller's list; both go through pgx.Identifier", Reason: "DROP INDEX"}
	return out
}

// sqlLiteral writes a value as ONE SQL string constant: whatever the value
// holds (a quote, a backslash, a comment's opening, a dollar quote, a new
// line), PostgreSQL's scanner reads the result as a single string and nothing
// else. It assumes standard_conforming_strings is on, PostgreSQL's default
// since 9.1: a backslash is then an ordinary character in '...'.
func TestSQLLiteralIsOneStringToSQL(t *testing.T) {
	if got := sqlLiteral("a'b"); got != "'a''b'" {
		t.Errorf("sqlLiteral = %s", got)
	}
	for _, v := range []string{"", "plain", "a'b", "''", "a\\'; DROP TABLE x; --", "$$x$$", "-- c", "/* c", "line\nbreak", "E'x'", `"q"`} {
		lit := sqlLiteral(v)
		ws, err := sqlscan.ParseWrites("SELECT " + lit + " AS v")
		if err != nil || len(ws) != 0 {
			t.Errorf("%q: SELECT %s is not read as a plain read: %v %v", v, lit, ws, err)
		}
		if words := sqlscan.FragmentWords("SELECT " + lit + " AS v"); strings.Join(words, " ") != "select as v" {
			t.Errorf("%q: the literal %s is not one string to the scanner: words %q", v, lit, words)
		}
	}
}

// builtByQueuePageSQL: ListQueuePage runs the two statements queuePageSQL
// builds, and nothing else. The pins on the queue page's statements read
// the builder's body; this is what ties them to the reader.
func builtByQueuePageSQL(t *testing.T) {
	t.Helper()
	body := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func (s *PostgresStore) ListQueuePage("))
	for _, needle := range []string{
		`countQuery, dataQuery, err := queuePageSQL(search != "", sortKey, strings.EqualFold(sortDir, "desc"))`,
		"s.pool.QueryRow(ctx, countQuery, args...)", "s.pool.Query(ctx, dataQuery, args...)",
	} {
		if strings.Count(body, needle) != 1 {
			t.Fatalf("ListQueuePage must contain, once: %s", needle)
		}
	}
	if strings.Count(body, "s.pool.") != 2 {
		t.Fatal("ListQueuePage must run queuePageSQL's two statements and no other")
	}
}

// Every call of execCreateIndexConcurrently names its index by a schema and
// a name, apart, each a plain lower-case name: never an already-qualified
// or already-quoted string (0.29.212: dropInvalidIndexSQL quotes the two
// as two identifiers, where they stood in the text unquoted). The calls
// are read from the source; a name that comes from a list is checked in
// the list.
func TestEveryIndexHelperCallNamesAPlainSchemaAndIndex(t *testing.T) {
	plain := regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)
	fset := token.NewFileSet()
	calls, fromLists := 0, 0
	for name, src := range srctest.PackageFiles(t, "internal/db", 30) {
		af, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(af, func(n ast.Node) bool {
			c, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			if id, ok := c.Fun.(*ast.Ident); !ok || id.Name != "execCreateIndexConcurrently" {
				return true
			}
			calls++
			if len(c.Args) != 7 {
				t.Errorf("%s: execCreateIndexConcurrently called with %d arguments", fset.Position(c.Pos()), len(c.Args))
				return true
			}
			for i, what := range map[int]string{4: "schema", 5: "index name"} {
				switch a := c.Args[i].(type) {
				case *ast.BasicLit:
					if v, err := strconv.Unquote(a.Value); err != nil || !plain.MatchString(v) {
						t.Errorf("%s: the %s %s is not a plain lower-case name", fset.Position(a.Pos()), what, a.Value)
					}
				case *ast.SelectorExpr, *ast.Ident:
					fromLists++ // a member of one of the lists below, or a local chosen between two literals
				default:
					t.Errorf("%s: the %s is neither a literal nor a list member's field: it could be a qualified or quoted string", fset.Position(c.Args[i].Pos()), what)
				}
			}
			return true
		})
	}
	if calls < 20 || fromLists == 0 {
		t.Fatalf("%d calls of execCreateIndexConcurrently read (%d naming their index from a list): the store has more", calls, fromLists)
	}
	var names []string
	for _, idx := range cntrbIDFKIndexes {
		names = append(names, idx.indexName)
	}
	for _, idx := range emailMessageFKIndexes {
		names = append(names, idx.indexName)
	}
	for _, idx := range fkExtraIndexes {
		names = append(names, idx.indexName)
	}
	for _, idx := range repoGroupFKIndexes {
		names = append(names, idx.indexName)
	}
	for _, idx := range perfWaveIndexDDL {
		names = append(names, idx.name)
	}
	for _, n := range names {
		if !plain.MatchString(n) {
			t.Errorf("the index name %q, from a list, is not a plain lower-case name", n)
		}
	}
}
