// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// migration_ledger_test.go — source-contract pins for the v0.28.4
// completed-backfill ledger (the F13 full fix). The ledger exists so a
// version-bump migrate stops re-walking every historical one-shot
// backfill (~1.5-2.5h of no-op scans measured on the 2026-08-23
// production migrate); these pins freeze the load-bearing pieces:
// the table shape, runOnce's record-only-on-success + SR-5 contracts,
// the create-before-use ordering, the exact ledgered-label registry,
// and the steps that must NEVER be ledgered.

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSchemaDeclaresMigrationLedger pins the table in schema.sql
// (fresh installs) — step_label is the PK so ON CONFLICT (step_label)
// has a real arbiter (SR-14).
func TestSchemaDeclaresMigrationLedger(t *testing.T) {
	norm := strings.Join(strings.Fields(srctest.Read(t, "internal/db/schema.sql")), " ")
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.migration_ledger",
		"step_label TEXT PRIMARY KEY",
		"completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()",
		"tool_version TEXT NOT NULL DEFAULT ''",
	} {
		if !strings.Contains(norm, needle) {
			t.Errorf("schema.sql must declare the migration ledger; missing %q", needle)
		}
	}
}

// TestRunOnceContract pins the four load-bearing behaviors of the
// runOnce wrapper:
//  1. probe by step_label;
//  2. ErrNoRows (never completed) runs the step;
//  3. a probe ERROR runs the step anyway (SR-5: a lookup error is not
//     "no" — running an idempotent step twice is safe, skipping it on
//     bad information is not);
//  4. the label is recorded ONLY when the step contributed zero errors
//     (fail-closed preserved: failed steps re-run next migrate).
func TestRunOnceContract(t *testing.T) {
	src := srctest.Read(t, "internal/db/migration_ledger.go")
	for _, needle := range []string{
		`SELECT 1 FROM aveloxis_ops.migration_ledger WHERE step_label = $1`,
		"errors.Is(err, pgx.ErrNoRows)",
		"running the step anyway", // the SR-5 arm's log message
		"ON CONFLICT (step_label) DO NOTHING",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("migration_ledger.go missing runOnce contract needle %q", needle)
		}
	}
	// Positional: the failure early-return must come BEFORE the
	// recording INSERT — a step that appended errors must never be
	// recorded as complete.
	failPos := strings.Index(src, "len(local) > 0")
	insertPos := strings.Index(src, "INSERT INTO aveloxis_ops.migration_ledger")
	if failPos < 0 || insertPos < 0 || failPos > insertPos {
		t.Errorf("runOnce must check the step's local error collector BEFORE recording the label (failPos=%d insertPos=%d) — recording a failed step would permanently skip its retry", failPos, insertPos)
	}
}

// TestRunMigrationsCreatesLedgerBeforeLedgeredSteps pins the ordering:
// the ledger table create runs right after the base DDL, before the
// first runOnce-gated step (cleanupBadTimestamps).
func TestRunMigrationsCreatesLedgerBeforeLedgeredSteps(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	basePos := strings.Index(src, `"base schema DDL"`)
	createPos := strings.Index(src, "ensureMigrationLedgerTable(ctx, pg, logger, &errs)")
	firstStepPos := strings.Index(src, `"cleanup garbage timestamps from prior versions"`)
	if basePos < 0 || createPos < 0 || firstStepPos < 0 {
		t.Fatalf("missing anchors: base=%d create=%d firstStep=%d", basePos, createPos, firstStepPos)
	}
	if !(basePos < createPos && createPos < firstStepPos) {
		t.Errorf("ledger table must be ensured after the base DDL and before the first ledgered step (base=%d create=%d firstStep=%d)", basePos, createPos, firstStepPos)
	}
}

// ledgeredStepLabels is this test's OWN fixture of every label gated
// through runOnce/runOnceStep (deliberately NOT imported from
// production code — the local copy is what makes a silent label rename
// or a silently-dropped gate fail the build; the house
// anti-rename-drift pattern). A renamed label re-runs its step
// (harmless — idempotent by contract — but wasteful and log-confusing),
// so renames must be deliberate: update BOTH the call site and this
// fixture in the same change.
var ledgeredStepLabels = []string{
	"v0.29.0 backfill collection_queue.last_activity_90d from the 90-day window",
	"v0.29.0 heal cross-system mis-drained mailing-list rows",
	"v0.29.0 backfill synthetic Jira issue state from notification subjects",
	"v0.29.0 heal automation-phantom contributors (relay identity fabrication)",
	// Function-wrapped walkers.
	"cleanup garbage timestamps from prior versions",
	"v0.27.7 rotate non-latest repo_labor snapshots to repo_labor_history",
	"v0.27.104 backfill pull_requests.meta_head_id/meta_base_id",
	"v0.27.15 msg_ref bridge repairs (dedup + data_source + inline-comment backfills)",
	// Plain-SQL one-shot backfills (runOnceStep).
	"backfill collection_queue.last_commits with distinct counts",
	"v0.20.5 backfill force_full_collect for repos with PR gap",
	"v0.20.12 backfill placeholder contributors for unresolvable logins",
	"v0.20.7 clear false-positive 'no data collected' errors for repos with real commits",
	"v0.21.0 backfill scancode_last_run from aveloxis_scan.scancode_scans",
	"v0.21.2 backfill collection_queue.last_issues with cumulative counts",
	"v0.21.2 backfill collection_queue.last_prs with cumulative counts",
	"v0.23.0 backfill contributor_login_history from identities",
	"v0.23.0 backfill contributor_login_history from contributors.cntrb_login",
	"v0.25.6 backfill cntrb_canonical from contributors_aliases",
	"v0.25.6 backfill cmt_author_platform_username from cmt_ght_author_id",
	"v0.26.4 backfill pull_requests.pr_diff_url from pr_html_url",
	"v0.26.4 backfill pull_request_meta.meta_label from pull_request_repo",
	"v0.27.79 re-null activity-check stamps from the resource-limits incident",
	"v0.27.103 backfill releases.data_source from repo platform",
	"v0.27.108 delete poisoned platform_user_id=0 identity rows",
	"v0.28.7 backfill vuln_scan_last_run from finding evidence (a scan provably ran)",
	// v0.28.18: the v0.27.37 GitLab force-full flag re-matched every
	// collected GitLab repo as soon as CompleteJob cleared the flag, so
	// an unledgered step forced a full recollect of every GitLab repo on
	// EVERY migrate (each version bump).
	"v0.27.37 force full recollect for GitLab repos (main-path comment drop heal)",
}

// TestLedgeredStepRegistry verifies every registered label is present
// in migrate.go AND actually gated (a runOnce/runOnceStep identifier
// appears in the ~250 chars preceding the label — the call the label
// is an argument of).
func TestLedgeredStepRegistry(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	for _, label := range ledgeredStepLabels {
		idx := strings.Index(src, `"`+label+`"`)
		if idx < 0 {
			t.Errorf("ledgered label %q not found in migrate.go — a rename must update the registry fixture too", label)
			continue
		}
		lo := idx - 250
		if lo < 0 {
			lo = 0
		}
		if !strings.Contains(src[lo:idx], "runOnce") {
			t.Errorf("label %q is registered as ledgered but its call site is not gated through runOnce/runOnceStep", label)
		}
	}
}

// TestEveryLedgeredCallSiteIsRegistered is the REVERSE direction
// (Copilot round 13 on PR #193): the fixture-to-source loop above
// cannot see a NEW runOnce/runOnceStep call whose label was never
// added to the fixture — which is exactly how the two v0.29.0 Jira/
// phantom labels slipped past it. Derive every label from the
// dispatch sites themselves (the round-7 rule: derive "every X" pins
// from the dispatch site, never a hand list) and require each in the
// fixture.
func TestEveryLedgeredCallSiteIsRegistered(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/db/migrate.go"))
	registered := map[string]bool{}
	for _, label := range ledgeredStepLabels {
		registered[label] = true
	}
	re := regexp.MustCompile(`runOnce(?:Step)?\(ctx, pg, logger, [^,]+,\s*"([^"]*)"`)
	matches := re.FindAllStringSubmatch(src, -1)
	if len(matches) < 15 {
		t.Fatalf("dispatch-site scan found only %d runOnce/runOnceStep labels — the regex rotted (guard the denominator)", len(matches))
	}
	for _, m := range matches {
		if !registered[m[1]] {
			t.Errorf("ledgered step %q is gated in migrate.go but ABSENT from ledgeredStepLabels — add it to the fixture so a rename or a dropped gate trips the registry", m[1])
		}
	}
}

// TestNeverLedgeredSteps pins the categories that must stay LIVE on
// every migrate: DDL heals hand-dropped objects, dedup-gated uniques
// re-evaluate their guards, CIC builds re-check INVALID leftovers.
func TestNeverLedgeredSteps(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	for _, label := range []string{
		"base schema DDL",
		"v0.25.1 drop inherited natural-key UNIQUEs on distribution history tables",
		"v0.27.60 default repos.added_at to NOW() for future inserts",
	} {
		idx := strings.Index(src, `"`+label+`"`)
		if idx < 0 {
			t.Fatalf("anchor label %q missing from migrate.go", label)
		}
		lo := idx - 250
		if lo < 0 {
			lo = 0
		}
		if strings.Contains(src[lo:idx], "runOnce") {
			t.Errorf("step %q must NEVER be ledgered — the explicit `aveloxis migrate` must keep healing this class on every run", label)
		}
	}
	// The uq_pr_review_msg_ref arbiter must have an un-ledgered
	// execCreateIndexConcurrently build in migrate.go (outside the
	// ledgered ensureMsgRefMetadata wrap) so a hand-dropped unique
	// still heals on the next explicit migrate.
	pos := strings.Index(src, `"uq_pr_review_msg_ref"`)
	if pos < 0 {
		t.Fatal("migrate.go must carry the live uq_pr_review_msg_ref re-ensure (execCreateIndexConcurrently) outside the ledgered msg_ref wrap")
	}
	lo := pos - 250
	if lo < 0 {
		lo = 0
	}
	if !strings.Contains(src[lo:pos], "execCreateIndexConcurrently") {
		t.Error("the uq_pr_review_msg_ref re-ensure must go through execCreateIndexConcurrently (INVALID-recovery re-check)")
	}
}

// TestDeduplicateCommitsStaysSelfGated pins that the commits dedup
// keeps its own cheap catalog-probe gate (index-exists short-circuit)
// instead of the ledger — its guard must re-evaluate on every run
// (the dedup-gated-unique rule).
func TestDeduplicateCommitsStaysSelfGated(t *testing.T) {
	src := srctest.Read(t, "internal/db/migrate.go")
	idx := strings.Index(src, "deduplicateCommits(ctx, pg, logger)")
	if idx < 0 {
		t.Fatal("deduplicateCommits call site missing")
	}
	lo := idx - 250
	if lo < 0 {
		lo = 0
	}
	if strings.Contains(src[lo:idx], "runOnce") {
		t.Error("deduplicateCommits must not be ledgered — its index-exists probe is the correct (re-evaluated) gate")
	}
}
