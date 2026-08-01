// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"strings"
	"testing"
)

// v0.25.6 — source-contract tests for the structural rewrite of
// explorer_new_contributors (UUID-based contributor join, no
// canonical_full_names CTE) + the augur_new_contributors VIEW
// alias restoration + the commit-resolver canonical-backfill fix +
// the three one-shot migration steps.

// TestExplorerNewContributorsUsesCmtGhtAuthorID pins the v0.25.6
// commit-branch contract: JOIN contributors ON cntrb_id =
// cmt_ght_author_id. This is the structural fix that takes commit-
// branch coverage from ~27% (pre-v0.25.6 email-canonical chain) to
// ~92% (UUID directly stamped by the commit resolver) and eliminates
// the empty-string Cartesian product that produced the 4.9TB temp-
// space ENOSPC observed on aveloxis_large 2026-05-25.
func TestExplorerNewContributorsUsesCmtGhtAuthorID(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	if !strings.Contains(region, "JOIN aveloxis_data.contributors c ON c.cntrb_id = co.cmt_ght_author_id") {
		t.Error("explorer_new_contributors commit branch must JOIN contributors ON c.cntrb_id = co.cmt_ght_author_id (v0.25.6 contract). The pre-v0.25.6 join via cntrb_canonical = cmt_author_email had empty-string Cartesian risk AND ~27% coverage vs ~92% with the UUID column.")
	}
	if !strings.Contains(region, "WHERE co.cmt_ght_author_id IS NOT NULL") {
		t.Error("explorer_new_contributors commit branch must filter `WHERE co.cmt_ght_author_id IS NOT NULL` so rows without a resolved author UUID are excluded from the matview (they have no contributor to attribute to).")
	}
}

// TestExplorerNewContributorsDropsCanonicalCTE pins the negative
// contract: the canonical_full_names CTE must be gone. Pre-v0.25.6
// it was a 180K-row DISTINCT ON subquery materialized once and JOINed
// to 6 of the 6 UNION branches. v0.25.6's structural rewrite uses
// direct cntrb_id joins on every branch, so the CTE serves no reader.
//
// A future "let me factor this out" refactor that brings the CTE
// back without re-introducing the canonical-email join paths would
// be harmless but unnecessary. A refactor that brings BOTH back
// would re-introduce the original 16-hour rebuild + ENOSPC failure
// mode.
func TestExplorerNewContributorsDropsCanonicalCTE(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	if strings.Contains(region, "canonical_full_names") {
		t.Error("explorer_new_contributors must NOT reference canonical_full_names — v0.25.6 dropped the CTE entirely in favor of direct cntrb_id joins. Bringing it back risks reintroducing the 16-hour-rebuild / 4.9TB-temp-space failure mode.")
	}
	if strings.Contains(region, "(cntrb_canonical)::text = (cntrb_email)::text") {
		t.Error("explorer_new_contributors must NOT join on cntrb_canonical = cntrb_email — that was the pre-v0.25.6 anchor of the canonical_full_names CTE. v0.25.6 dropped this entirely.")
	}
}

// TestExplorerNewContributorsFiltersSoftDeleted pins that every
// branch of the matview filters `COALESCE(c.cntrb_deleted, 0) = 0`.
// Soft-deleted contributors (the v0.20.2 logical-merge "loser" rows
// where cntrb_deleted = 1) must not appear in the matview's output
// — analytics consumers of explorer_new_contributors expect the
// active contributor identity, not the pre-merge duplicate.
//
// Six branches × one filter each = 6 occurrences expected.
func TestExplorerNewContributorsFiltersSoftDeleted(t *testing.T) {
	src := readMatviewsSQLForV0255(t)
	region := extractMatviewBlock(t, src, "explorer_new_contributors")

	got := strings.Count(region, "COALESCE(c.cntrb_deleted, 0) = 0")
	if got < 6 {
		t.Errorf("explorer_new_contributors must filter `COALESCE(c.cntrb_deleted, 0) = 0` in every branch (6 branches × 1 filter = 6) — found %d occurrences. Without the filter, v0.20.2 merge-loser rows appear in the matview as duplicates.", got)
	}
}

// TestAugurNewContributorsIsViewAlias pins the v0.25.6 restoration of
// augur_new_contributors as a plain VIEW. v0.25.5 dropped it outright;
// the operator pointed out it's used (outside 8Knot) to identify new
// contributors. v0.25.6 restores it as `CREATE OR REPLACE VIEW ...
// SELECT * FROM aveloxis_data.explorer_contributor_actions` so it
// stays queryable without adding any refresh cost.
//
// Same shape as the v0.25.5 explorer_libyear_all VIEW alias.
func TestAugurNewContributorsIsViewAlias(t *testing.T) {
	src := readMatviewsSQLForV0255(t)

	if !strings.Contains(src, "CREATE OR REPLACE VIEW aveloxis_data.augur_new_contributors AS") {
		t.Error("augur_new_contributors must be declared as a regular VIEW (not MATERIALIZED VIEW). v0.25.6 restored it as a VIEW alias for explorer_contributor_actions; pre-v0.25.6 history of this view is documented in the matviews.sql comment block above the CREATE.")
	}
	if !strings.Contains(src, "SELECT * FROM aveloxis_data.explorer_contributor_actions") {
		t.Error("augur_new_contributors VIEW must SELECT * FROM aveloxis_data.explorer_contributor_actions — that's how the alias preserves the queryable name without duplicating storage.")
	}

	// Negative pin: the old MATERIALIZED VIEW declaration must NOT come back.
	if strings.Contains(src, "CREATE MATERIALIZED VIEW IF NOT EXISTS aveloxis_data.augur_new_contributors") {
		t.Error("augur_new_contributors must NOT be declared as MATERIALIZED VIEW post-v0.25.6 — restored as a regular VIEW alias.")
	}

	// Pin the DROP statements that fire before the CREATE VIEW so existing
	// deployments transition cleanly regardless of prior state (matview
	// from pre-v0.25.5 OR nothing from v0.25.5).
	if !strings.Contains(src, "DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.augur_new_contributors") {
		t.Error("matviews.sql must DROP MATERIALIZED VIEW IF EXISTS aveloxis_data.augur_new_contributors before the CREATE VIEW so a pre-v0.25.5 install with the old matview transitions cleanly.")
	}
	if !strings.Contains(src, "DROP VIEW IF EXISTS aveloxis_data.augur_new_contributors") {
		t.Error("matviews.sql must DROP VIEW IF EXISTS aveloxis_data.augur_new_contributors before the CREATE OR REPLACE so the CASCADE clause clears any downstream views that depend on the alias.")
	}
}

// TestCommitResolverEnsureAliasBackfillsCanonical pins the v0.25.6
// commit-resolver forward-looking fix: ensureAlias must call
// SetContributorCanonical after EnsureContributorAlias succeeds. The
// pre-v0.25.6 implementation populated the alias table but left
// cntrb_canonical empty on the parent contributor row, suppressing
// the email-canonical resolution path that downstream queries relied
// on (e.g. the BackfillCommitAuthorIDs join on contributors.gh_login,
// the v0.25.6 migration backfill of cntrb_canonical from aliases).
//
// SetContributorCanonical uses COALESCE(NULLIF(cntrb_canonical, ”),
// $2) internally so a non-empty existing canonical is preserved.
func TestCommitResolverEnsureAliasBackfillsCanonical(t *testing.T) {
	data, err := os.ReadFile("../collector/commit_resolver.go")
	if err != nil {
		t.Fatalf("could not read commit_resolver.go: %v", err)
	}
	src := string(data)

	region := extractEnsureAliasBody(t, src)

	if !strings.Contains(region, "SetContributorCanonical") {
		t.Error("ensureAlias must call r.store.SetContributorCanonical after EnsureContributorAlias succeeds. The v0.25.6 changelog explains why: strategies 2 (DB lookup) and 4 (Search API) populated the alias table but left cntrb_canonical empty on the contributor row, suppressing downstream resolution paths.")
	}
}

// TestV025_6_MigrationStepsRegistered pins the three one-shot
// migrations + the two DROP INDEX statements that ship with v0.25.6.
func TestV025_6_MigrationStepsRegistered(t *testing.T) {
	src := readSourceFile(t, "migrate.go")

	// Three migration step labels — operators see these in the log on
	// the first v0.25.6 migrate. Renaming them is a breaking change
	// for operator log-grep workflows.
	wantLabels := []string{
		"v0.25.6 backfill cntrb_canonical from contributors_aliases",
		"v0.25.6 backfill cmt_author_platform_username from cmt_ght_author_id",
		"v0.25.6 drop idx_contributors_cntrb_canonical (obsoleted by matview rewrite)",
		"v0.25.6 drop idx_contributors_canonical_eq_email (obsoleted by matview rewrite)",
	}
	for _, label := range wantLabels {
		if !strings.Contains(src, label) {
			t.Errorf("migrate.go must register v0.25.6 step %q via execMigrationStep.", label)
		}
	}

	// Pin the load-bearing SQL fragments. A refactor that drops the
	// soft-delete filter or the COALESCE empty-string guard would
	// silently corrupt data on every migrate run.
	wantFragments := []string{
		"FROM aveloxis_data.contributors_aliases",
		"COALESCE(c.cntrb_canonical, '') = ''",
		"COALESCE(c.cntrb_deleted, 0) = 0",
		"FROM aveloxis_data.contributors c",
		"WHERE c.cntrb_id = co.cmt_ght_author_id",
		"COALESCE(co.cmt_author_platform_username, '') = ''",
		"DROP INDEX IF EXISTS aveloxis_data.idx_contributors_cntrb_canonical",
		"DROP INDEX IF EXISTS aveloxis_data.idx_contributors_canonical_eq_email",
	}
	for _, frag := range wantFragments {
		if !strings.Contains(src, frag) {
			t.Errorf("migrate.go must contain v0.25.6 SQL fragment %q. A refactor that drops it would silently change migration semantics.", frag)
		}
	}
}

// TestV025_6_ObsoleteIndexCreatesRemoved pins that the v0.25.5 CREATE
// statements for the two indexes that v0.25.6 drops are gone from
// migrate.go. Keeping the CREATEs alongside the DROPs is harmless on
// a fresh install (create + immediate drop = no-op) but wasteful, and
// could confuse a future operator reading the migration source.
func TestV025_6_ObsoleteIndexCreatesRemoved(t *testing.T) {
	src := readSourceFile(t, "migrate.go")

	for _, banned := range []string{
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contributors_cntrb_canonical",
		"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contributors_canonical_eq_email",
	} {
		if strings.Contains(src, banned) {
			t.Errorf("migrate.go must NOT contain the CREATE statement %q — v0.25.6 obsoleted these indexes by removing the canonical_email join paths from explorer_new_contributors. Only the DROP statements should remain.", banned)
		}
	}
}

// TestVersionStampedV0256 pins the version bump.
func TestVersionStampedV0256(t *testing.T) {
	// Version line advances: 0.25.6 → 0.25.7 (mailing lists) → 0.25.8
	// (matview commit-index fix) → 0.25.9 (Phase 4 verify-mailing-list
	// harness + PonyMail FirstMonth cheap-window fix) → 0.25.10
	// (explorer_new_contributors malformed-author-date guard) → 0.25.11
	// (refresh tool_version on every data_collection_date upsert) → 0.25.12
	// (mailing_list_backfill_months 0 = full history at the config layer) →
	// 0.25.13 (remove the duplicate 0→6 clamp in the worker constructor that
	// still defeated full history) → 0.25.14 (mailing-list staging refactor:
	// MailingListWorker stages classified messages, a per-list single-threaded
	// MailingListProcessor drains them — keeps the pipeline off the hot-table
	// direct-upsert path that reproduced Augur's contention) → 0.25.15
	// (Phase 2: runMailingListSenderResolve ticker resolves mailing-list
	// senders the DB can't, via the shared email→identity chain — Search +
	// global commit-search — and links/creates the contributor) → 0.25.16
	// (Phase 3/Phase A: issue_event → issues link-or-create projection for
	// clean_fit systems + projection_policy gate + email_message
	// linked_pr_review_id/projected_kind columns. Apache issue data, formerly
	// absent, lands under the PMC repo for standard per-repo analytics) →
	// 0.25.17 (thread-inheritance so full email threads attach to an issue;
	// LINK-by-title + conflict-safe external-key backfill to prevent missed-LINK
	// shadows; Phase 4 email-only contributors for direct-human senders; Phase 5
	// backfill-mailing-list-projection over existing email_message rows) →
	// 0.25.18 (Phase C: mailing_list_pr_equivalents read-only VIEW surfacing
	// forge-less kernel [PATCH] threads as PR-equivalents without polluting
	// pull_requests) → 0.25.19 (conflict-safe external-key backfill: DISTINCT ON
	// winner-per-key + NOT EXISTS guard, fixes the 23505 that survived the first
	// partial fix) → 0.25.20 (literal `node_id <> ''` predicates + candidate-batch
	// partial indexes so the projection backfill uses index scans instead of a
	// 66s-per-row parallel seqscan over 19.8M messages) → 0.25.21–0.25.22
	// (projection-backfill hardening) → 0.25.23 (deterministic lock order in
	// UpsertContributorBatch via sort.Strings(logins) — fixes the 40P01 deadlocks
	// from concurrent workers locking shared bot contributors in map-random order)
	// → 0.25.24 (scancode startup preflight + aveloxis_ops.aveloxis_status table:
	// detect a corrupt host libmagic that wedges every scan, log it prominently,
	// record subsystem health) → 0.25.25 (DB-down graceful-pause guard:
	// runDBHealthMonitor probes the pool, fillWorkerSlots gates on dbHealthy, so a
	// nightly PostgreSQL restart pauses collection instead of erroring) → 0.25.26
	// (cross-OS scancode libmagic check: generic 'magic + Warning + offset +
	// invalid' fingerprint catches macOS Homebrew libmagic too, OS-aware
	// remediation text via runtime.GOOS) → 0.25.27 (scancode preflight libmagic
	// check is now VOLUME-based, not presence-based: only `broken` when the
	// offset-invalid fingerprint repeats past the systemic-spam threshold —
	// fixes the 2026-06-10 false positive where a repaired libmagic emitting a
	// handful of benign warnings, with scans completing and producing valid
	// data, was wrongly flagged broken) → 0.25.28 (bound the scancode per-repo
	// failure capture with a head+tail buffer — the corrupt host libmagic made
	// large repos like aws/aws-sdk-cpp emit 15+ GB of stderr that the unbounded
	// bytes.Buffer held entirely in RAM; also surface a libmagic likely_cause
	// hint on per-repo failures and name the libmagic-mgc package in the
	// remediation text) → 0.25.29 (debounce the DB-health monitor: require
	// dbHealthFailureThreshold=3 consecutive failed probes before pausing
	// collection, so transient connect/SASL-auth timeouts under host CPU
	// pressure — TLS+SCRAM handshakes exceeding the 5s connect deadline — stop
	// flapping the fleet between "unavailable"/"back" on non-outages) →
	// 0.25.30 (the pg_trgm GIN index idx_repos_owner_name_trgm is now
	// warn-only like the extension it depends on: gate its creation on
	// ginTrgmOpsVisible (pg_opclass_is_visible probe) so a registered-but-
	// not-visible operator class — extension installed in a schema off the
	// role's search_path, observed on kate 2026-06-13, SQLSTATE 42704 —
	// skips-with-warning instead of failing migration fatally and blocking
	// serve startup over a monitor-search perf optimization) → 0.25.31 (a
	// single 401 no longer permanently invalidates an API key: KeyPool now
	// quarantines a key only after maxAuthStrikes consecutive 401s — any
	// success resets the count — with an exponential auto-recovering cooldown,
	// and GetKey waits for a quarantined key to recover instead of returning
	// ErrAllKeysInvalidated. Fixes the 2026-06-17 aveloxis_large crash-loop
	// where transient GitHub-auth-backend 401s bled 18 good keys out one at a
	// time over 15 hours and the scheduler then crashed on ClassAuth) →
	// 0.25.32 (case-variant duplicate repos: GitHub/GitLab URLs are
	// case-insensitive at the forge, but repo_git matching was byte-exact —
	// 1,220 duplicate pairs on aveloxis_large. Prevention via
	// resolveCaseVariantURL in UpsertRepo/FindRepoByURL, cleanup via
	// `aveloxis dedup-repos`, uq_repos_repo_git_ci backstop after drain,
	// Phase 0 case self-heal via HealRepoCaseDrift) → 0.25.33 (first
	// production dedup-repos run failed on 18f/identity-idp: the
	// globally-scoped FindReviewDBID had cross-linked winner-owned
	// review_comments to loser-owned reviews; dedupOnePair now remaps
	// cross-repo review links to winner equivalents and FindReviewDBID is
	// repo-scoped) → 0.25.34 (dedup pairs spent 18+ min inside COMMIT:
	// email_message's linked_issue/PR/review FK columns shipped unindexed
	// in v0.25.7, so deferred NO-ACTION checks seqscanned per deleted
	// parent row — three partial indexes added; and the contributor_repo
	// repoint was removed from dedupOnePair: it's the breadth worker's
	// GitHub-wide observational stream with no repos FK, and the repoint
	// both rewrote history and seqscanned 51M rows per pair) → 0.25.35 →
	// 0.25.36 (safego recover-wrappers on ~30 goroutines, swallowed-Exec
	// fixes, -race on the integration tier, staticcheck blocking, weekly
	// network canaries, KeyPool concurrency stress test) → 0.25.37 (the
	// three-layer config wiring collapsed: scheduler.Config's 45-field
	// mirror is gone, the scheduler consumes *config.CollectionConfig
	// directly through the accessors, main.go's 42-line wiring became
	// one Collection pointer. The collapse FIXED a live double-clamp:
	// NewWithKeys was still re-defaulting ScancodeShutdownGrace 0→30m,
	// silently defeating the v0.23.7 immediate-kill default) → 0.25.38
	// (behavioral tests on the three incident-prone hot paths: the
	// Processor write path end-to-end, the breadth circuit breaker via
	// the new noteContributorOutcome seam + breadthStore role interface,
	// and the runJob lock lifecycle — which immediately exposed and
	// fixed TWO real bugs: facade cloned https://unknown/... for every
	// generic-git repo, and git-only facade failures reported SUCCESS
	// with no last_error) → 0.26.0 (GraphQL becomes the DEFAULT for
	// GitHub PR-child fetch and issue+PR listing — the flip the v0.19.0
	// sunset plan scheduled but never executed; REST stays as the
	// aveloxis.json escape hatch, path deletion is a separate operator
	// decision) → 0.26.1 (data-test gains the column-fill diff: per-
	// column populated counts, type-aware, FAIL when a column goes
	// completely dark under the new binary — the platform_label_id=0
	// class that row counts cannot see; row diff now also covers
	// aveloxis_scan) → 0.26.2 (one-shot
	// `aveloxis collect` delegates its API phases to the staged
	// pipeline — the legacy direct-write path dropped EVERY event via
	// FK 23503; data-test subprocess output now side-tagged).
	// (docs truth-reconciliation: 12 undocumented commands added to
	// commands.md, stale counts/cadences/defaults fixed everywhere, and
	// three VALUE-checking tripwires added — example-config effective
	// defaults, commands-doc coverage, schema-count pins).
	src := readSourceFile(t, "version.go")
	if !strings.Contains(src, `var ToolVersion = "0.27.69"`) {
		t.Error("internal/db/version.go must declare ToolVersion = \"0.27.69\". The tool_version columns and SBOM output read this constant.")
	}
}

// extractEnsureAliasBody returns the body of the ensureAlias function
// in commit_resolver.go. Used to scope the SetContributorCanonical
// assertion to the right function (vs false-matching elsewhere in
// the file, e.g. a doc comment or the ResolveEmailsToCanonical
// loop body).
func extractEnsureAliasBody(t *testing.T, src string) string {
	t.Helper()
	anchor := "func (r *CommitResolver) ensureAlias("
	start := strings.Index(src, anchor)
	if start < 0 {
		t.Fatalf("could not find ensureAlias function in commit_resolver.go")
	}
	// End at the next top-level `func ` declaration.
	rest := src[start+len(anchor):]
	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		return src[start:]
	}
	return src[start : start+len(anchor)+end]
}
