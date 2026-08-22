// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.106 — Copilot review round on PR #184 (all 8 findings verified;
// 7 taken, 1 declined as a deliberate Augur-parity behavior). This file
// pins the db-side fixes.
package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Finding 1 (the severe one): login-only UserRefs (userID == 0 —
// GraphQL Bot actors; GitLab group owners until v0.27.109 stopped
// producing them) all shared the resolver cache key
// {platform, 0}, so after the first resolved, EVERY subsequent login-only
// ref returned the FIRST one's cntrb_id — collapsing distinct identities
// onto one contributor. The identities lookup had the same collapse via
// a (platform, 0) probe. Post-fix, userID==0 refs key by login and skip
// the identities probe.
func TestLoginOnlyRefsResolveDistinctly(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	t.Cleanup(func() {
		cctx, ccancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer ccancel()
		_, _ = store.pool.Exec(cctx, `DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE 'avpr184-%'`)
	})

	r := NewContributorResolver(store)
	id1, err := r.Resolve(ctx, 2, 0, "avpr184-group-alpha", "", "", "", "", "", "Organization")
	if err != nil {
		t.Fatal(err)
	}
	id2, err := r.Resolve(ctx, 2, 0, "avpr184-group-beta", "", "", "", "", "", "Organization")
	if err != nil {
		t.Fatal(err)
	}
	if id1 == id2 {
		t.Fatalf("two DIFFERENT login-only refs collapsed onto one contributor (%s) — the {platform, 0} cache-key bug", id1)
	}
	// Same login twice → same contributor (the cache still works).
	id1b, err := r.Resolve(ctx, 2, 0, "avpr184-group-alpha", "", "", "", "", "", "Organization")
	if err != nil {
		t.Fatal(err)
	}
	if id1b != id1 {
		t.Fatalf("re-resolving the same login-only ref must be stable: %s vs %s", id1b, id1)
	}

	// v0.27.108 (round 3): NO identity row may exist for these refs —
	// the pre-fix step-3 INSERT funneled every login-only ref into one
	// shared (platform, 0) identity row (login churning per observation,
	// cntrb_id pinned to the first contributor).
	var zeroRows int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.contributor_identities
		WHERE platform_user_id = 0 AND cntrb_id::text IN ($1, $2)`, id1, id2).Scan(&zeroRows); err != nil {
		t.Fatal(err)
	}
	if zeroRows != 0 {
		t.Fatalf("login-only refs created %d (platform, 0) identity rows — identity rows are keyed by platform_user_id, which these refs don't have", zeroRows)
	}
}

// Finding 5: the rename-heal must route through UpdateRepoURLs (plural —
// which rewrites the child tables' stored issue/PR/release URLs carrying
// the old owner/repo path), and must only fall through to the insert on
// an actual 23505 uniqueness race; any other update failure propagates
// (falling through would recreate the very duplicate the heal prevents).
func TestRenameHealUsesUpdateRepoURLsAndGatesFallthrough(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpsertRepo(")
	if !strings.Contains(body, "UpdateRepoURLs(") {
		t.Error("the rename-heal must call UpdateRepoURLs (plural) so child-table URLs heal too")
	}
	// The fallthrough gate: a pgErr 23505 check must guard the heal's
	// error path (transient failures must NOT proceed to insert).
	i := strings.Index(body, "FindRepoByPlatformRepoID(")
	if i < 0 {
		t.Fatal("heal branch missing")
	}
	region := body[i:]
	if j := strings.Index(region, "var id int64"); j > 0 {
		region = region[:j]
	}
	if !strings.Contains(region, `"23505"`) {
		t.Error("the heal's URL-update failure path must gate the insert fallthrough on SQLSTATE 23505 specifically")
	}
}

// Finding 6: the column-writer tripwire must scope its writer search to
// INSERT/UPDATE statements that name the audited table — a column
// mentioned only in a comment or a SELECT-only reader must NOT count as
// having a writer.
//
// v0.27.124 (Phase 2): re-anchored. The scoping + comment-stripping now
// live in internal/srctest/sqlscan (FindWrites + Statements — whose own
// fixture suite pins both behaviors, incl. the moved WritesColumn
// negative fixtures); the flagship must ROUTE through that engine
// rather than reimplement it.
func TestColumnTripwireScopesToWriteStatements(t *testing.T) {
	src, err := os.ReadFile("column_writer_tripwire_test.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"sqlscan.Statements(",
		"sqlscan.FindWrites(",
		".WritesColumn(",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("the column-writer tripwire must use the sqlscan engine (%q) — its statement scoping and literal-aware comment stripping are the round-5 contract", needle)
		}
	}
}

// --- round 6 (review 4985645746: 1 active + 3 suppressed, ALL real) -------

// Active: UpdateRepoURLs must be transactional — the old shape updated
// repo_git first and swallowed child-update errors, so a mid-child
// failure reported success with repo_git already changed; later scans
// saw the new URL and never retried (child URLs permanently stale).
func TestUpdateRepoURLsIsTransactional(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpdateRepoURLs(")
	if !strings.Contains(body, "s.pool.Begin(ctx)") || !strings.Contains(body, "tx.Commit(ctx)") {
		t.Error("UpdateRepoURLs must run the repos update + child rewrites in ONE transaction")
	}
	if strings.Contains(body, "continue") {
		t.Error("child-update errors must roll the rename back, never continue — an error is not 'no matching rows'")
	}
}

// Suppressed 1+3 (wrongly suppressed): both heal-branch lookups must
// propagate errors. A forge-ID lookup error treated as not-found mints
// the rename duplicate; an ignored old-URL lookup passes oldURL="" to
// UpdateRepoURLs, whose repo-only fallback skips every child rewrite
// while reporting success.
func TestRenameHealPropagatesLookupErrors(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpsertRepo(")
	if !strings.Contains(body, "rename-heal forge-id lookup") {
		t.Error("FindRepoByPlatformRepoID errors must propagate (the v0.27.36 rule: a lookup ERROR is not 'not found')")
	}
	if !strings.Contains(body, "rename-heal old-URL lookup") {
		t.Error("the old-URL scan error must propagate — oldURL='' silently skips the child-URL rewrites")
	}
}

// --- round 7 (review 4986041250: 1 active + 4 suppressed, ALL real) -------

func TestRewalkClaimExcludesNeverCollectedAndRechecks(t *testing.T) {
	src, err := os.ReadFile("whitespace_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// Wrongly-suppressed: walking a never-collected repo stamps the
	// marker over zero commit rows; the later first collection's
	// incremental walk then skips those commits forever.
	if !strings.Contains(s, "q.last_collected IS NOT NULL") {
		t.Error("GetReposForWhitespaceRewalk must exclude never-collected repos (marker-over-nothing = permanent zero whitespace)")
	}
	// Active: the page-time status filter goes stale — the command
	// re-checks the claim per repo just before walking.
	if !strings.Contains(s, "func (s *PostgresStore) IsRepoCollecting(") {
		t.Error("IsRepoCollecting must exist for the rewalk worker's pre-walk re-check")
	}
	cmdSrc, err := os.ReadFile("../../cmd/aveloxis/rewalk_whitespace.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(cmdSrc), "IsRepoCollecting(") {
		t.Error("the rewalk worker must re-check the collection claim before each walk")
	}
}

func TestMailingListWritersRetryDeadlocks(t *testing.T) {
	src, err := os.ReadFile("email_message_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// The 2026-08-20 CI drop: a 40P01 on the email_message upsert was
	// DROPPED by the processor's drop-for-progress policy. The v0.25.36
	// note pre-decided the fix: bounded retry — withRetry retries
	// exactly 40P01.
	if strings.Count(s, "s.withRetry(ctx, func(ctx context.Context) error {") < 3 {
		t.Error("UpsertEmailMessage, UpsertMailingListMessageBody, and InsertEmailMessageRef must route through withRetry (40P01)")
	}
}

func TestIdentityHealIsReachable(t *testing.T) {
	src, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (r *ContributorResolver) Resolve(")
	// Wrongly-suppressed: the v0.27.103 heal lived only in ON CONFLICT
	// clauses that existing identities never reach (the step-2 hit
	// returns first). The hit branch must read node_id/user_type and
	// heal in place.
	if !strings.Contains(body, "COALESCE(node_id, ''), COALESCE(user_type, '')") {
		t.Error("Resolve's identity hit must read node_id/user_type alongside cntrb_id")
	}
	if !strings.Contains(body, "SET node_id = COALESCE(NULLIF(node_id, ''), $3)") {
		t.Error("Resolve's identity hit must heal empty node_id/user_type in place — the ON CONFLICT heal is unreachable for existing rows")
	}
}

func TestRenameHealURLProbePropagatesRealErrors(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpsertRepo(")
	// Wrongly-suppressed: only ErrNoRows means "untracked".
	if !strings.Contains(body, "rename-heal URL probe") {
		t.Error("the urlTracked probe must propagate non-ErrNoRows errors instead of steering into the heal path on bad information")
	}
}

// ---------------------------------------------------------------------------
// Round 8 (2026-08-20): 1 active + 2 suppressed — all three real again.
// ---------------------------------------------------------------------------

// Active: single-repo mode (`rewalk-whitespace --repo-id N`) bypassed
// BOTH gates the fleet query applies in SQL. Walking a never-collected
// repo stamps whitespace_head_hash over ZERO commit rows; the later
// first collection's incremental phase then skips all historical
// whitespace forever — the exact marker-over-missing-rows class the
// v0.27.112 fleet-query fix closed. The gate must run BEFORE the walk.
func TestSingleRepoRewalkAppliesFleetGates(t *testing.T) {
	src, err := os.ReadFile("whitespace_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "func (s *PostgresStore) RepoWhitespaceRewalkState(") {
		t.Error("RepoWhitespaceRewalkState must exist — single-repo rewalk mode needs the never-collected + mid-collection gates the fleet query applies in SQL")
	}
	cmdSrc, err := os.ReadFile("../../cmd/aveloxis/rewalk_whitespace.go")
	if err != nil {
		t.Fatal(err)
	}
	c := string(cmdSrc)
	gateIdx := strings.Index(c, "RepoWhitespaceRewalkState(")
	walkIdx := strings.Index(c, "fc.RewalkWhitespace(")
	if gateIdx < 0 {
		t.Fatal("single-repo rewalk mode must consult RepoWhitespaceRewalkState before walking")
	}
	// The single-repo branch precedes the worker loop, so the FIRST
	// RewalkWhitespace call site is the single-repo one — the gate must
	// appear before it.
	if walkIdx >= 0 && gateIdx > walkIdx {
		t.Error("the rewalk gate must run BEFORE the single-repo RewalkWhitespace call — gating after the walk is decorative")
	}
}

// Suppressed-but-real: the doc grouped msg_header/rgls_id with the
// writer-backed msg_sender_email under "writer correct, data
// legitimately rare" while the canonical documentedEmpty map says both
// have NO writer. The doc must not drift from the map it claims to
// mirror. (The other suppressed finding — the tripwire missing
// ALTER-added columns — is pinned by the tripwire's own
// repos.whitespace_head_hash self-check.)
func TestColumnMappingDocMatchesDocumentedEmpty(t *testing.T) {
	doc, err := os.ReadFile("../../docs/architecture/column-mapping.md")
	if err != nil {
		t.Fatal(err)
	}
	d := string(doc)
	if strings.Contains(d, "msg_sender_email`/`msg_header") {
		t.Error("column-mapping.md groups msg_header with the writer-backed msg_sender_email — msg_header is documentedEmpty (no writer)")
	}
	for _, needle := range []string{"msg_header", "rgls_id"} {
		if !strings.Contains(d, needle) {
			t.Errorf("column-mapping.md must still document messages.%s in the known-empty section", needle)
		}
	}
}

// ---------------------------------------------------------------------------
// Round 9 (2026-08-20): 0 active + 3 suppressed — all three real (fifth
// consecutive round the suppressed tier earned its read).
// ---------------------------------------------------------------------------

// Suppressed #1: the v0.27.111 transactional UpdateRepoURLs wrote the
// raw newURL — UpdateRepoURL (singular) trims trailing "/" and ".git"
// first, and prelim passes the raw redirect target, so a redirect to
// ".../name.git" could persist a noncanonical repo_git and undermine
// the URL-dedup invariant (uq_repos_repo_git_ci matches on the stored
// string).
func TestUpdateRepoURLsNormalizesNewURL(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpdateRepoURLs(")
	if !strings.Contains(body, `strings.TrimSuffix(strings.TrimSuffix(newURL, "/"), ".git")`) {
		t.Error("UpdateRepoURLs must normalize newURL (trim trailing / and .git) before writing repo_git — the singular UpdateRepoURL does, and prelim passes the raw redirect target")
	}
}

// Suppressed #2: the reformat lookup was a linear scan of every pending
// removal per added line — O(N×M) on a replacement block (a 500K-line
// generated-file rewrite ≈ 10^11 string comparisons stalling a fleet
// worker). The container must be an occurrence-count multiset
// (map[string]int) — O(1) lookup/consume with identical semantics,
// since entries are only ever matched by exact content equality.
// (The multiset SEMANTICS — duplicates consumed once each — are pinned
// behaviorally in internal/collector/whitespace_test.go.)
func TestWhitespaceReformatLookupIsConstantTime(t *testing.T) {
	src, err := os.ReadFile("../collector/whitespace.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "wsCheck") || !strings.Contains(s, "map[string]int") {
		t.Error("wsCheck must be a map[string]int occurrence-count multiset")
	}
	if strings.Contains(s, "for i, chk := range wsCheck") {
		t.Error("the linear reformat scan is back — a replacement block is O(N×M) again")
	}
}

// Suppressed #3: a status-query ERROR bypassed the pre-walk gate and
// started the walk anyway — the "a lookup ERROR is not 'no'" class,
// fixed twice already in this PR (v0.27.111 heal branch, v0.27.112
// urlTracked probe). Overlap is data-safe by construction, so the cost
// is a wasted multi-GB walk that then fails on clone contention or the
// same DB trouble — treat the error as a failed repo (empty marker; a
// rerun retries; the exit status reflects it).
func TestRewalkStatusErrorCountsAsFailed(t *testing.T) {
	cmdSrc, err := os.ReadFile("../../cmd/aveloxis/rewalk_whitespace.go")
	if err != nil {
		t.Fatal(err)
	}
	c := string(cmdSrc)
	if strings.Contains(c, "cerr == nil && collecting") {
		t.Error("the worker loop still proceeds to walk on a status-query error — count it as failed and continue instead")
	}
	if !strings.Contains(c, "whitespace rewalk claim check failed") {
		t.Error("a status-query error must WARN and count in the failed total (nonzero exit) so the skipped safety check is visible")
	}
}

// Round-9 follow-on, surfaced while gating: the combined db+collector
// integration run failed on EVERY attempt with `base schema DDL:
// ERROR: deadlock detected (SQLSTATE 40P01)` — the v0.27.18 deadlock
// retry covered execMigrationStep but the base schema DDL ran as a
// bare pool.Exec outside it, so concurrent packages' migrations (and,
// in production, `aveloxis migrate` beside a live serve) could lose
// the whole base DDL to a deadlock victim kill. Every schema.sql
// statement is IF NOT EXISTS-idempotent, so the retry is safe.
func TestBaseSchemaDDLRetriesDeadlocks(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, `execMigrationStep(ctx, pg, logger, &errs, "base schema DDL", schemaSQL)`) {
		t.Error("the base schema DDL must route through execMigrationStep for the bounded 40P01 retry")
	}
	if strings.Contains(s, "pg.pool.Exec(ctx, schemaSQL)") {
		t.Error("the bare base-DDL Exec is back — it loses the deadlock retry")
	}
}

// Round-9 follow-on #2: the combined-run gating also caught the OTHER
// half of the deadlock class — Postgres sometimes picks the concurrent
// package's ORDINARY statement as the victim instead of the migration.
// The two production-relevant victims get the v0.25.36 pre-decided
// bounded retry: the mailing-list PROJECTION writers (drop-for-progress
// turns an unretried 40P01 into a dropped message — the v0.27.112 wave
// covered email_message_store.go but missed this file) and the
// vulnerability batch upsert (a scan beside `aveloxis migrate`).
func TestProjectionAndVulnWritersRetryDeadlocks(t *testing.T) {
	proj, err := os.ReadFile("mailinglist_projection_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(proj), "s.withRetry(ctx, func(ctx context.Context) error {") < 3 {
		t.Error("LinkOrCreateIssueFromEmail's CREATE and both BridgeEmailToIssue statements must route through withRetry (40P01)")
	}
	vuln, err := os.ReadFile("vulnerability_store.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(vuln), "s.withRetry(ctx, func(ctx context.Context) error {") {
		t.Error("InsertVulnerabilityBatch must route through withRetry — every queued statement is an idempotent upsert")
	}
}

// ---------------------------------------------------------------------------
// Round 10 (2026-08-20): 2 active + 3 suppressed — all five real. Two
// of them are EARLIER PR FIXES violating standing rules (the operator's
// "fixes breaking other things" observation): the v0.27.102 schema.sql
// index declaration broke the v0.27.98 migration-only rule, and the
// v0.27.105 rewalk walk reintroduced the marker-over-missing-rows class
// on a third path. New fixes now get checked against the standing-rules
// list before shipping.
// ---------------------------------------------------------------------------

// Active #1: SetPlatformRepoIDIfEmpty's zero-row update was silent for
// BOTH "already set to the same value" and "set to a DIFFERENT value" —
// the second is a delete-and-recreate-under-the-same-URL identity
// conflict whose histories silently merge. Detection is OBSERVATION-ONLY
// (the house never-auto-mutate rule): an ERROR log naming stored vs
// observed; the scan pass proceeds unchanged.
func TestForgeIDMismatchIsDetected(t *testing.T) {
	src, err := os.ReadFile("repo_forge_id.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) SetPlatformRepoIDIfEmpty(")
	if !strings.Contains(body, "RowsAffected() == 0") {
		t.Error("SetPlatformRepoIDIfEmpty must inspect the zero-row case — silence papers over the identity conflict its own doc comment warns about")
	}
	if !strings.Contains(body, `stored != "" && stored != forgeID`) {
		t.Error("the zero-row probe must distinguish same-value (benign) from DIFFERENT-value (delete-recreate identity conflict)")
	}
	if !strings.Contains(body, "s.logger.Error(") {
		t.Error("a forge-ID mismatch is a data-integrity signal — it must log at ERROR with stored + observed IDs")
	}
}

// Active #2: the whitespace walk stamped the marker without proving the
// emitted stats matched stored commit rows. Historical per-row write
// failures (swallowed by the pre-v0.27.107 facade while last_collected
// advanced) leave gaps; UpdateCommitWhitespaceBatch silently no-ops on
// them, and a stamped marker excludes their whitespace from every
// future incremental walk — the marker-over-missing-rows class on its
// THIRD path (fleet query v0.27.112, single-repo gate v0.27.113, the
// walk itself here).
func TestWhitespaceWalkRefusesToStampOverMissingRows(t *testing.T) {
	store, err := os.ReadFile("whitespace_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(store)
	if !strings.Contains(s, "func (s *PostgresStore) UpdateCommitWhitespaceBatch(ctx context.Context, repoID int64, stats []CommitWhitespaceStat) (updated, matched int64, err error)") {
		t.Error("UpdateCommitWhitespaceBatch must return a matched count independent of the IS DISTINCT guard (the existence probe)")
	}
	walk, err := os.ReadFile("../collector/whitespace.go")
	if err != nil {
		t.Fatal(err)
	}
	w := string(walk)
	refIdx := strings.Index(w, "matched < total")
	stampIdx := strings.Index(w, "SetWhitespaceHead(ctx, repoID, head)")
	if refIdx < 0 {
		t.Fatal("runWhitespaceWalk must compare matched stats against emitted total")
	}
	if stampIdx >= 0 && refIdx > stampIdx {
		t.Error("the shortfall refusal must run BEFORE the marker stamp — refusing after is decorative (the v0.27.107 lesson)")
	}
}

// ---------------------------------------------------------------------------
// Round 11 (2026-08-20): 3 active + 4 suppressed — all seven real. The
// three actives are round-10 interactions: the existing prefer-nonempty
// forge-ID writers silently DESTROYED the new mismatch signal, and the
// newly always-run view carried a row-multiplying join. (The two
// writer-shape flips are pinned in rename_dedup_test.go; the rune-count
// parity fix is pinned behaviorally in the collector walker suite.)
// ---------------------------------------------------------------------------

// Active #2: the view's contributor join multiplied threads — neither
// email column is unique, and an empty sender_email equals every
// empty-email contributor row. The LATERAL aggregate returns at most
// one row (HAVING count = 1: ambiguous → NULL, never an arbitrary
// pick), preserving one-row-per-thread STRUCTURALLY.
func TestPREquivalentsViewResolvesAtMostOneContributor(t *testing.T) {
	src, err := os.ReadFile("views.sql")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "HAVING count(*) = 1") {
		t.Error("the view's contributor match must be unambiguous-or-NULL (HAVING count(*) = 1)")
	}
	if !strings.Contains(s, `root.sender_email <> ''`) {
		t.Error("an empty sender_email must match NO contributor — it would equal every empty-email row")
	}
	if strings.Contains(s, "LEFT JOIN aveloxis_data.contributors c") {
		t.Error("the plain contributors join is back — it multiplies threads on non-unique emails")
	}
}

// Suppressed #1: views.sql runs on EVERY migrate — a DROP ... CASCADE
// there removes dependent views and grants on every deploy.
func TestViewsSQLNeverDrops(t *testing.T) {
	src, err := os.ReadFile("views.sql")
	if err != nil {
		t.Fatal(err)
	}
	stripped := regexp.MustCompile(`(?m)--[^\n]*`).ReplaceAllString(string(src), "")
	if regexp.MustCompile(`(?i)\bDROP\b`).MatchString(stripped) {
		t.Error("views.sql must be CREATE OR REPLACE only — a DROP CASCADE on every migrate removes dependents and grants; incompatible shape changes ship as one-shot migration drops instead")
	}
}

// Suppressed #2: the incremental whitespace walk fell back to a FULL
// multi-hour walk on ANY error — a transient DB failure became an
// outage amplifier. Marker validity is now checked up front; errors
// just warn and retry next cycle.
func TestWhitespacePhaseValidatesMarkerUpFront(t *testing.T) {
	src, err := os.ReadFile("../collector/whitespace.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	if !strings.Contains(s, "func markerResolves(") {
		t.Error("markerResolves must pre-validate the marker (rev-parse --verify) so only a vanished marker triggers the full walk")
	}
	if strings.Contains(s, "retrying full history") {
		t.Error("the blanket error-triggered full-walk fallback is back — a transient DB failure must not launch a multi-hour git log -p")
	}
}

// Suppressed #4: a failed node_id/user_type heal must NOT cache the
// identity — a cached entry exits at the step-1 cache lookup forever,
// so "the next observation retries" would be false until restart.
func TestFailedIdentityHealIsNotCached(t *testing.T) {
	src, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (r *ContributorResolver) Resolve(")
	healIdx := strings.Index(body, "identity node_id/user_type heal failed")
	if healIdx < 0 {
		t.Fatal("the heal-failure warn is gone")
	}
	// The failure branch must return BEFORE the cache write that
	// follows it.
	after := body[healIdx:]
	retIdx := strings.Index(after, "return cntrbID, nil")
	cacheIdx := strings.Index(after, "r.cache[key] = cntrbID")
	if retIdx < 0 || (cacheIdx >= 0 && cacheIdx < retIdx) {
		t.Error("a failed heal must return WITHOUT caching so the next observation re-runs the heal (a cached identity never reaches this branch again)")
	}
}

// ---------------------------------------------------------------------------
// Round 12 (2026-08-20): 2 active + 2 suppressed — three real, one
// DECLINED (its premise doesn't match the tree: no platform-scoped
// login fallback or cross-platform separation test exists, and the
// "separation required" assumption contradicts the schema's deliberate
// one-person-row model — see the DELIBERATELY GLOBAL comment at
// Resolve's step 2.5, which this round added to make the design
// decision visible at the site).
// ---------------------------------------------------------------------------

// Active #2 (real): the whitespace existence probe ran as a SEPARATE
// statement from the UPDATE — a commit-file row inserted by a
// concurrent collection between the two snapshots counted as matched
// without ever receiving values, letting the walker stamp over it.
// One statement = one snapshot.
func TestWhitespaceProbeSharesUpdateSnapshot(t *testing.T) {
	src, err := os.ReadFile("whitespace_store.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) UpdateCommitWhitespaceBatch(")
	if !strings.Contains(body, "WITH v AS") || !strings.Contains(body, "upd AS") {
		t.Error("the update and the existence probe must run as ONE statement (CTE) — two statements = two snapshots = false coverage from concurrent inserts")
	}
	if strings.Count(body, "s.pool.QueryRow") != 1 || strings.Contains(body, "s.pool.Exec") {
		t.Error("UpdateCommitWhitespaceBatch must issue exactly one SQL statement per chunk")
	}
}

// Suppressed #1 (real): the PR-equivalents root lookup must key on ALL
// of (thread_key, repo_id, list_address) — the threads CTE groups by
// all three, and two lists reusing a thread id would otherwise serve
// the OTHER list's subject/sender as this thread's root.
func TestPREquivalentsRootLookupKeysOnList(t *testing.T) {
	src, err := os.ReadFile("views.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "em2.list_address = t.list_address") {
		t.Error("the root LATERAL must match list_address — the thread key includes it")
	}
}

// ---------------------------------------------------------------------------
// Round 14 (2026-08-20/21): 2 active + 6 suppressed — all eight real,
// five of them aimed at the days-old Phase 1 infrastructure (FuncBody
// went AST-based after two lexical generations each carried a real
// false-window class; the ratchet detector went AST-based; the three
// PR-added helpers grandfathered into the baseline were migrated; an
// empty baseline became the valid GOAL state). The two data-quality
// items are pinned below; srctest's own fixture suite pins the rest.
// ---------------------------------------------------------------------------

// Suppressed #5: updated_at must never REGRESS — overlapping metadata
// refreshes finish out of order, and an older forge response landing
// last must not overwrite a newer forge timestamp (it only increases).
// Both writers (UpsertRepo ON CONFLICT + UpdateRepoMetadata) use
// GREATEST over the nil-safe COALESCE pair.
func TestUpdatedAtNeverRegresses(t *testing.T) {
	for _, f := range []string{"postgres.go", "repo_metadata.go"} {
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(src), "updated_at = GREATEST(") {
			t.Errorf("%s: updated_at must be written via GREATEST — prefer-incoming regresses under out-of-order refreshes", f)
		}
	}
}

// ---------------------------------------------------------------------------
// Round 15 (v0.27.123) — review 4989260861: 1 active + 5 suppressed, all
// six real. The active one is a data-integrity hole in the identity
// backfills; three suppressed ones are the repo's own standing rules
// applied to the v0.27.115 drift remediation (the migration-only-index
// rule violated AGAIN, five rounds after round 10 re-affirmed it); one
// is the "a lookup ERROR is not 'no'" class inside the round-10
// detector itself; one is docs drift.
// ---------------------------------------------------------------------------

// Active: both owner-login sweeps used bare DISTINCT ON with no ORDER BY
// beyond the target id and no uniqueness guard — gh_login is backed only
// by a NON-unique index, so two active contributor rows matching the same
// lowered owner login handed the target an ARBITRARY cntrb_id,
// contradicting the backfill's documented "ambiguous stays NULL" /
// never-fabricate contract. Both sweeps now GROUP BY the target id and
// keep only groups with exactly ONE distinct contributor; the dry-run
// counts apply the same rule. The closers derivation legitimately keeps
// DISTINCT ON — its ORDER BY (issue_id, e.created_at DESC) makes the
// pick deterministic ("latest closed event"), not arbitrary.
func TestLoginSweepsRejectAmbiguousOwnerLogins(t *testing.T) {
	src, err := os.ReadFile("identity_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, fn := range []string{
		"func (s *PostgresStore) BackfillPRMetaOwners(",
		"func (s *PostgresStore) BackfillPRRepoOwners(",
	} {
		body := extractFuncBody(t, string(src), fn)
		if !strings.Contains(body, "HAVING COUNT(DISTINCT c.cntrb_id) = 1") {
			t.Errorf("%s: the login sweep must keep only owner logins matching exactly ONE distinct contributor (ambiguous stays NULL)", fn)
		}
		if strings.Contains(body, "DISTINCT ON (m.pr_meta_id)") ||
			strings.Contains(body, "DISTINCT ON (pr.pr_repo_id)") {
			t.Errorf("%s: bare DISTINCT ON without a tiebreak ORDER BY assigns an ARBITRARY contributor on ambiguous logins — banned", fn)
		}
	}
	if !strings.Contains(string(src), "ORDER BY issue_id, e.created_at DESC") {
		t.Error("closedByCandidates must keep its deterministic ORDER BY (latest closed event) — that DISTINCT ON is ordered, not arbitrary")
	}
}

// Drive-by alignment forced by rewriting the same statement:
// BackfillPRMetaOwners' login sweep never received the v0.27.110
// platform restriction (only BackfillPRRepoOwners did), so a phase-1
// re-run could still attribute a GitLab-platform meta row to an
// unrelated same-name GitHub user — the exact cross-platform
// fabrication v0.27.109 banned. Same gates as the pr_repo sweep now.
func TestMetaOwnerSweepIsGitHubOnly(t *testing.T) {
	src, err := os.ReadFile("identity_backfill.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) BackfillPRMetaOwners(")
	for _, needle := range []string{
		"r.platform_id = 1",
		"COALESCE(c.gl_username, '') = ''",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("BackfillPRMetaOwners login sweep missing %q — the v0.27.110 GitHub-only rule applies to BOTH backfills", needle)
		}
	}
}

// Suppressed #1 + #2: repo_labor_history index lifecycle. The v0.27.115
// schema.sql declaration violated the v0.27.98 migration-only rule for
// INTRODUCING releases: base DDL runs before any migration step, so an
// upgraded fleet that happens to LACK the accidental LIKE copy would
// block-build the index with a plain CREATE INDEX on a fleet-scale
// history table. The index is migration-owned (CONCURRENTLY; fresh
// installs get it through the same step — instant on empty tables), and
// the composite-copy drop is CONCURRENTLY too (a plain DROP INDEX takes
// ACCESS EXCLUSIVE and blocks rotation writers beside a live serve).
func TestHistoryIndexIsMigrationOnlyAndDropIsConcurrent(t *testing.T) {
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(schema), "repo_labor_history_repo_id_idx") {
		t.Error("schema.sql must NOT declare repo_labor_history_repo_id_idx — base DDL block-builds it on fleets lacking the LIKE copy (v0.27.98 migration-only rule for introducing releases)")
	}
	helper, err := os.ReadFile("repo_labor_history.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(helper), "CREATE INDEX CONCURRENTLY IF NOT EXISTS repo_labor_history_repo_id_idx") {
		t.Error("ensureRepoLaborHistoryIndex must own repo_labor_history_repo_id_idx via execCreateIndexConcurrently — fresh installs AND upgrades")
	}
	mig, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(mig), "ensureRepoLaborHistoryIndex(") {
		t.Error("RunMigrations must invoke ensureRepoLaborHistoryIndex")
	}
	if !strings.Contains(string(mig), "DROP INDEX CONCURRENTLY IF EXISTS aveloxis_data.repo_labor_history_repo_id_rl_analysis_date_idx") {
		t.Error("the composite-copy drop must be DROP INDEX CONCURRENTLY — a plain drop blocks writers on the 1.2 GB history table beside a live serve")
	}
}

// Suppressed #5: SetPlatformRepoIDIfEmpty's zero-row verification probe
// treated a QUERY FAILURE as success — the caller got neither the DB
// error nor the promised forge-ID conflict signal. Only a genuinely
// missing row (deleted between UPDATE and probe) may be ignored; every
// other probe error propagates. The "a lookup ERROR is not 'no'" rule,
// this time inside the round-10 detector itself.
func TestForgeIDProbeErrorIsNotSuccess(t *testing.T) {
	src, err := os.ReadFile("repo_forge_id.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) SetPlatformRepoIDIfEmpty(")
	if !strings.Contains(body, "errors.Is(perr, pgx.ErrNoRows)") {
		t.Error("only a genuinely-missing row may be ignored on the verification probe (ErrNoRows)")
	}
	if !strings.Contains(body, "verify stored forge ID") {
		t.Error("a probe failure must propagate wrapped (\"verify stored forge ID\") — silently returning nil loses the error AND the conflict signal")
	}
}

// Suppressed #3: docs/guide/commands.md described phase 1 as assignments
// + pr_meta owners only, while v0.27.104 added the 41.2M-row
// pull_request_repo pass — operators plan these repairs by phase, so the
// scope must be accurate.
func TestBackfillIdentitiesDocsCoverPRRepoPass(t *testing.T) {
	doc := srctest.Read(t, "docs/guide/commands.md")
	idx := strings.Index(doc, "backfill-identities")
	if idx < 0 {
		t.Fatal("commands.md must document backfill-identities")
	}
	section := doc[idx:min(idx+3000, len(doc))]
	if !strings.Contains(section, "pr_repo") && !strings.Contains(section, "pull_request_repo") {
		t.Error("commands.md's backfill-identities section must document the v0.27.104 pull_request_repo owner pass in phase 1")
	}
}

// Suppressed #4: docs/schema.md still documented contributors_old after
// v0.27.115 dropped it — operators must not be told to query a table
// the migration deletes.
func TestSchemaDocsDropContributorsOld(t *testing.T) {
	if strings.Contains(srctest.Read(t, "docs/schema.md"), "contributors_old") {
		t.Error("docs/schema.md still documents contributors_old — the table was dropped in v0.27.115; remove the section")
	}
}

// ---------------------------------------------------------------------------
// Round 16 (v0.27.125) — review 4994358938: 0 active + 9 suppressed, all
// real. Four target the days-old test/scan infrastructure again (the
// strippers' token concatenation is pinned behaviorally in srctest's own
// suite; the curly-quote class got a scripts/ tripwire); the rest are
// pinned here.
// ---------------------------------------------------------------------------

// Suppressed: FindRepoByPlatformRepoID must carry the partial-index
// predicate LITERALLY — a generic prepared plan cannot prove `$2 <> ""`
// at plan time, so without it idx_repos_platform_repo_id (partial WHERE
// platform_repo_id <> "") is unusable and every org-listing lookup can
// seq-scan repos. Semantically free: the Go guard already rejects "".
func TestForgeIDLookupCarriesPartialIndexPredicate(t *testing.T) {
	src, err := os.ReadFile("repo_forge_id.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (s *PostgresStore) FindRepoByPlatformRepoID(")
	if !strings.Contains(body, "platform_repo_id <> ''") {
		t.Error("FindRepoByPlatformRepoID must include the literal partial-index predicate so generic plans can use idx_repos_platform_repo_id")
	}
}

// Suppressed ×3: the version-gated test migrate raced RunMigrations'
// advisory lock on FRESH parallel runs — every binary saw the old stamp
// and each still ran the full DDL after queueing. Both twins now
// RECHECK the stamp under a shared test-scoped advisory lock, and the
// repo-case connect helper routes through testMigrate instead of an
// inline check.
func TestTestMigrateRechecksUnderAdvisoryLock(t *testing.T) {
	for _, f := range []string{"internal/db/testexec_test.go", "internal/collector/testmigrate_test.go"} {
		s := srctest.Read(t, f)
		if !strings.Contains(s, "pg_try_advisory_lock($1)") || !strings.Contains(s, "0x41564C5854455354") {
			t.Errorf("%s: testMigrate must serialize under the shared test-scoped advisory lock via TRY-lock polling", f)
		}
		// v0.27.128 (round 17): the BLOCKING form is banned — a waiter
		// blocked inside pg_advisory_lock holds a snapshot the holder's
		// CREATE INDEX CONCURRENTLY waits on: the v0.27.20 undetectable
		// deadlock, recreated here for one release. Poll
		// pg_try_advisory_lock with ctx-aware sleeps, like RunMigrations.
		if strings.Contains(s, "SELECT pg_advisory_lock(") {
			t.Errorf("%s: blocking pg_advisory_lock is banned in testMigrate — it deadlocks against the holder's CREATE INDEX CONCURRENTLY (the v0.27.20 class)", f)
		}
		if strings.Count(s, "GetSchemaVersion(ctx)") < 2 {
			t.Errorf("%s: testMigrate must RECHECK the stamp after acquiring the lock (fast path + under-lock check)", f)
		}
	}
	rc, err := os.ReadFile("repo_case_integration_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rc), "testMigrate(ctx, t, store)") {
		t.Error("repo_case_integration_test.go must route through testMigrate, not an inline stamp check")
	}
}

// Suppressed: the login-hit branch's identity backfill discarded its
// Exec error and cached anyway — a deadlocked heal meant node_id /
// user_type / the identities row never retried for the process lifetime
// (the v0.27.117 failed-heal rule on its second branch). A failure now
// logs, returns the resolved contributor, and leaves the key UNCACHED.
func TestLoginHitIdentityBackfillFailureLeavesCacheCold(t *testing.T) {
	src, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFuncBody(t, string(src), "func (r *ContributorResolver) Resolve(")
	needle := "leaving resolver cache cold"
	if !strings.Contains(body, needle) {
		t.Errorf("Resolve's login-hit identity backfill must log %q and return WITHOUT caching on failure", needle)
	}
	i := strings.Index(body, needle)
	tail := body[i:]
	retPos := strings.Index(tail, "return existingID, nil")
	cachePos := strings.Index(tail, "r.cache[key] = existingID")
	if retPos < 0 || (cachePos >= 0 && cachePos < retPos) {
		t.Error("the failure path must RETURN before any cache assignment — caching a failed heal pins the miss for the process lifetime")
	}
}

// ---------------------------------------------------------------------------
// Round 17 (v0.27.128) — review 4994845070: 4 active, all four on the
// day-old Phase 2-4 infrastructure. The lock findings are the v0.27.20
// advisory-lock/CIC deadlock class RECREATED by the round-16 fix (the
// blocking-form ban is folded into the round-16 pin above); the
// meta-test findings are pinned here.
// ---------------------------------------------------------------------------

// Active #3+#4: the standing-rules meta-test's CLAUDE.md check must
// soft-skip ONLY on absence (any other read error fails — the SR-5
// class inside the meta-test itself), and must test EXACT ID
// membership (a substring check counts a low-numbered ID as present
// whenever any two-digit ID sharing its prefix appears, so a deleted
// low-numbered rule slips the drift check).
func TestStandingRulesMetaTestPrecision(t *testing.T) {
	s := srctest.Read(t, "scripts/standing_rules_test.go")
	if !strings.Contains(s, "errors.Is(err, os.ErrNotExist)") {
		t.Error("the CLAUDE.md soft-skip must be gated on os.ErrNotExist — a permission/IO error is not expected absence")
	}
	if !strings.Contains(s, "proseIDs[") {
		t.Error("the drift check must test EXACT SR-ID membership (regex-extracted set), not substring containment")
	}
	if strings.Contains(s, "strings.Contains(prose, r.ID)") {
		t.Error("substring ID matching is banned — a low-numbered ID matches inside two-digit IDs sharing its prefix")
	}
}

// ---------------------------------------------------------------------------
// Round 18 (v0.27.130) — review 4995652021: 3 active + 1 suppressed, all
// real, all on the days-old harness/infrastructure code again.
// ---------------------------------------------------------------------------

// Active #2+#3: the testMigrate unlock ran on the possibly-canceled
// migrate context — a failed pg_advisory_unlock followed by Release()
// returns a session that still OWNS the test lock to the pool, wedging
// every other binary's poll loop. Unlock uses a fresh bounded context;
// an unconfirmed unlock destroys the session (session death releases
// its advisory locks).
func TestTestMigrateUnlockSurvivesCanceledContext(t *testing.T) {
	for _, f := range []string{"internal/db/testexec_test.go", "internal/collector/testmigrate_test.go"} {
		s := srctest.Read(t, f)
		if !strings.Contains(s, "uctx, ucancel := context.WithTimeout(context.Background()") {
			t.Errorf("%s: the unlock must use a FRESH bounded context, never the migrate ctx", f)
		}
		if !strings.Contains(s, "conn.Conn().Close(uctx)") {
			t.Errorf("%s: an unconfirmed unlock must destroy the session — releasing a still-locked session to the pool wedges the twins", f)
		}
	}
}

// Suppressed (real; pinned behaviorally in sqlscan's own suite): backtick
// literals inside Go COMMENTS must not enter the SQL corpus — the tree
// really contains a backticked UPDATE in a doc comment, and counting
// documentation as a writer would let a removed real write pass.
func TestSQLCorpusStripsGoCommentsFirst(t *testing.T) {
	s := srctest.Read(t, "internal/srctest/sqlscan/sqlscan.go")
	if !strings.Contains(s, "srctest.BacktickLiterals(srctest.StripGoComments(") {
		t.Error("sqlscan.Statements must strip Go comments BEFORE extracting backtick literals")
	}
}

// ---------------------------------------------------------------------------
// Round 19 (v0.27.135) — review 4997293831: 4 comments, all real (two
// substantive on the day-old C2 attribution, two import-grouping style
// whose CLASS the scripts/import_grouping_test.go tripwire now kills).
// ---------------------------------------------------------------------------

// Finding 1: the chain index merged every lockfile's edges into ONE
// adjacency — in a monorepo, a parent from apps/a's lockfile could
// connect through a child name in apps/b's lockfile, fabricating an
// introduced_by path no single resolution ever produced. The adjacency
// is now PARTITIONED PER LOCKFILE and each walk stays inside one graph
// (behavioral proof: TestChainsForNeverCrossLockfileGraphs).
func TestChainIndexPartitionsPerLockfile(t *testing.T) {
	s := srctest.Read(t, "internal/api/vuln_chains.go")
	if !strings.Contains(s, "graphs map[string]map[string][]string") {
		t.Error("chainIndex must partition the adjacency by lockfile_path — one merged graph fabricates cross-lockfile chains")
	}
	if !strings.Contains(s, "idx.graphs[e.LockfilePath]") {
		t.Error("buildChainIndex must group edges by e.LockfilePath")
	}
}

// Finding 2: introduced_by was attached to RESOLVED historical
// findings from the CURRENT edge graph — a chain that may never have
// produced the finding, implying live exposure where none exists.
// Attribution is now gated to current rows (ResolvedAt == nil);
// historical edge snapshots are not retained, so absence is honest.
func TestChainAttributionGatedToCurrentFindings(t *testing.T) {
	s := srctest.Read(t, "internal/api/vulnerabilities.go")
	if !strings.Contains(s, `v.DependencyKind == "transitive" && v.ResolvedAt == nil {`) {
		t.Error("introduced_by must attach ONLY to current transitive findings — today's graph cannot explain a historical snapshot's finding")
	}
}

// Finding 1's SBOM sibling (applied while fixing the class): a
// lockfile edge names the parent's RESOLVED version, so the SBOM graph
// must attach children to that version's component/package first —
// name-level only as fallback — or p@2 inherits p@1's dependencies
// (behavioral proof: TestGenerateCycloneDX_ParentVersionExactAttach).
func TestSBOMGraphResolvesParentByVersionFirst(t *testing.T) {
	// v0.27.151 (round 30): the version-exact-first ordering moved
	// into the ONE shared resolver (sbomGraphIndex.parentRefs) both
	// generators call — pin the ordering THERE: within each
	// population, the version-exact lookup must precede the
	// name-level fallback.
	s := srctest.Read(t, "internal/collector/sbom.go")
	body := srctest.FuncBody(t, s, "func (x *sbomGraphIndex) parentRefs(")
	lockVer := strings.Index(body, `lockByNameVer[e.LockfilePath][pk+"@"+e.ParentVersion]`)
	lockName := strings.Index(body, "lockByName[e.LockfilePath][pk]")
	dirVer := strings.Index(body, `directByNameVer[pk+"@"+e.ParentVersion]`)
	dirName := strings.Index(body, "directByName[pk]")
	if lockVer < 0 || lockName < 0 || dirVer < 0 || dirName < 0 ||
		!(lockVer < lockName && lockName < dirVer && dirVer < dirName) {
		t.Error("parentRefs must try version-exact before name-level in BOTH populations (lockfile then direct) — attaching children to every same-name component hangs one version's dependencies off another's node")
	}
}

// ---------------------------------------------------------------------------
// Round 20 (v0.27.137) — review 4998079294: 1 comment, real — the
// direct-root set was repo-wide while the round-19 walk is
// per-lockfile.
// ---------------------------------------------------------------------------

// A package direct in apps/b but TRANSITIVE in apps/a terminated
// apps/a's walk early, naming a root that lockfile's manifest never
// declares. GetRepoDirectPackageSets now preserves lockfile_path
// provenance; the walk roots on the graph's OWN direct set plus the
// repo-wide DECLARED (manifest) fallback — repo_deps_libyear carries
// no path column, so path-aware handling is not derivable for it (the
// review's "explicitly separate fallback" arm). Behavioral proofs:
// TestChainsForRootsArePerLockfile +
// TestChainsForDeclaredFallbackRootsAnyGraph.
func TestChainRootsArePerLockfile(t *testing.T) {
	s := srctest.Read(t, "internal/api/vuln_chains.go")
	if !strings.Contains(s, "direct   map[string]map[string]bool") {
		t.Error("chainIndex.direct must be per-lockfile — a repo-wide root set truncates monorepo chains at non-actionable roots")
	}
	if !strings.Contains(s, "lockfileDirect[key] || declared[key]") {
		t.Error("the walk's root test must be lockfile-own-direct OR repo-wide-declared")
	}
	st := srctest.Read(t, "internal/db/lockfile_store.go")
	if !strings.Contains(st, "ByLockfile map[string]map[string]bool") {
		t.Error("DirectPackageSets must keep lockfile_path provenance on direct rows")
	}
}

// ---------------------------------------------------------------------------
// Round 21 (v0.27.138) — review 4998225609: 2 comments, both real.
// ---------------------------------------------------------------------------

// #2: with transitive scanning default-on (v0.27.136), the lockfile
// scan's completion log counted direct + transitive + the Go build
// list under "direct_resolutions" — a systematically misleading
// diagnostic. The log must SPLIT the kinds (log-the-effective-value).
func TestLockfileScanLogSplitsResolutionKinds(t *testing.T) {
	s := srctest.Read(t, "internal/collector/lockfile_scan.go")
	if !strings.Contains(s, `"transitive_resolutions", len(packages)-direct`) ||
		!strings.Contains(s, `"direct_resolutions", direct`) {
		t.Error("lockfile-scan completion log must report direct and transitive counts separately")
	}
}

// ---------------------------------------------------------------------------
// Round 22 (v0.27.141) — review 4998625140: 4 comments, all real (the
// C2 attribution's third hardening + the day-old tripwires tightened).
// ---------------------------------------------------------------------------

// Enforces SR-17 (scripts/standing_rules.go).
// #1: graph keys now fold through the ONE shared db.LockfileGraphKey —
// case, PyPI underscore/dot equivalence, and the rubygems↔gem-class
// ecosystem vocabulary split. Raw keys silently dropped chains for
// exactly the packages whose spellings differ between subsystems, and
// the SBOM side's private fold was a duplicate waiting to drift.
func TestLockfileGraphKeyIsTheOneFold(t *testing.T) {
	if got := LockfileGraphKey("rubygems", "Rails"); got != "gem|rails" {
		t.Errorf("alias+case folding broken: %q", got)
	}
	if got := LockfileGraphKey("pypi", "Foo_Bar.baz"); got != "pypi|foo-bar-baz" {
		t.Errorf("PyPI PEP 503 folding broken: %q", got)
	}
	chains := srctest.Read(t, "internal/api/vuln_chains.go")
	if !strings.Contains(chains, "db.LockfileGraphKey(") {
		t.Error("the chain walk must key through db.LockfileGraphKey")
	}
	sbom := srctest.Read(t, "internal/collector/sbom.go")
	if !strings.Contains(sbom, "return db.LockfileGraphKey(eco, name)") {
		t.Error("sbomGraphKey must delegate to db.LockfileGraphKey — a private fold copy WILL drift")
	}
	sets := srctest.Read(t, "internal/db/lockfile_store.go")
	if !strings.Contains(sets, "key := LockfileGraphKey(eco, name)") {
		t.Error("GetRepoDirectPackageSets must key through LockfileGraphKey")
	}
}

// #2 pinned behaviorally in gitlab/userref_singleflight_test.go
// (8 concurrent cold lookups → 1 HTTP request; failed flight retries).
// #3 re-red-verified: a stdlib-after-module violation in a SECOND
// import block now fails scripts/import_grouping_test.go.
// #4: the standing-rules meta-test collects test names from go/parser
// FuncDecls — a `func Test...(` in a comment or raw-string fixture can
// no longer fake a rule's enforcement (re-red-verified).
func TestStandingRulesMetaTestUsesAST(t *testing.T) {
	s := srctest.Read(t, "scripts/standing_rules_test.go")
	if !strings.Contains(s, "parser.ParseFile(") || !strings.Contains(s, "*ast.FuncDecl") {
		t.Error("the meta-test must collect FuncDecls via go/parser, not a source regex")
	}
	if strings.Contains(s, "regexp.MustCompile(`(?m)^func (Test") {
		t.Error("the regex test-name collector is back — comment/fixture mentions would count as enforcement again")
	}
}

// ---------------------------------------------------------------------------
// Round 23 (v0.27.142) — review 5000226546: 5 comments, all real, all
// on the day-old v0.27.139/140 code (the aggressive-validation loop
// again).
// ---------------------------------------------------------------------------

// #3+#5: a failed number LISTING is a failed assessment, not a
// zero-fill success — both listing errors now enter fillErrs, so the
// standalone healer's nonzero-exit contract and routine gap fill's
// v0.20.5 recovery both arm.
func TestGapFillListingFailuresAreErrors(t *testing.T) {
	s := srctest.Read(t, "internal/collector/gap_fill.go")
	for _, needle := range []string{
		`fillErrs = append(fillErrs, fmt.Errorf("issue number listing: %w", err))`,
		`fillErrs = append(fillErrs, fmt.Errorf("PR number listing: %w", err))`,
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("gap_fill.go missing %s — a failed listing must not report as a successful zero-fill", needle)
		}
	}
}

// #4: the GapForceList sentinel — threshold 0 still requires
// metadata > gathered, so completeness sweeps (count-netting: retained
// upstream-deleted rows offsetting missing ones) need an explicit
// force-list mode where the listing itself is the truth source.
func TestGapForceListSentinel(t *testing.T) {
	s := srctest.Read(t, "internal/collector/gap_fill.go")
	if !strings.Contains(s, "const GapForceList = -1.0") {
		t.Error("GapForceList sentinel missing")
	}
	body := srctest.FuncBody(t, s, "func gapExceedsThreshold(")
	if !strings.Contains(body, "if threshold < 0 {") {
		t.Error("gapExceedsThreshold must short-circuit TRUE in force-list mode before any count comparison")
	}
}

// #1 is pinned in since_boundary_test.go (store-enforced failure
// invariant + the mistaken-caller behavioral case); #2 in
// gap_heal_test.go (RefreshQueueGatheredCounts after successful heals
// — rerun-until-0 convergence).

// ---------------------------------------------------------------------------
// Round 24 (v0.27.143) — review 5000294838: 1 active + 1 suppressed,
// both real.
// ---------------------------------------------------------------------------

// Suppressed: hardcoding every podling to the incubator- form still
// seeded phantom rows for podlings whose canonical repo is plain
// (apache/amoro exists; apache/incubator-amoro does not) — the
// v0.27.132 variants only protected load-apache-lists, while
// import-foundations upserted the guess directly. The operator's
// domain rule: the prefix exists WHILE incubating, is shed at
// graduation, never returns, and podlings.json lags both directions —
// so the FORGE probe, not the naming convention, is the authority.
// Fetch now resolves each podling URL against the forge (test seam:
// podlingProbeBase) and moves no-variant-exists URLs to
// UnresolvedRepoURLs, which the importer logs and SKIPS.
func TestPodlingRepoURLsAreForgeResolved(t *testing.T) {
	a := srctest.Read(t, "internal/importers/apache/apache.go")
	for _, needle := range []string{
		"func resolvePodlingRepoURL(",
		"var podlingProbeBase",
		"p.UnresolvedRepoURLs = append(p.UnresolvedRepoURLs, rurl)",
	} {
		if !strings.Contains(a, needle) {
			t.Errorf("apache.go missing %s", needle)
		}
	}
	imp := srctest.Read(t, "cmd/aveloxis/import_foundations.go")
	if !strings.Contains(imp, "p.UnresolvedRepoURLs") {
		t.Error("import-foundations must log+skip unresolved podling URLs — upserting the guess is how the phantom rows were born")
	}
}

// Active: spdxPackageID hashed bare name@version — npm/foo@1.0.0 and
// pypi/foo@1.0.0 shared one SPDXID, and the second ecosystem's graph
// key pointed relationships at the first's package/purl. IDs are now
// ecosystem-scoped through the SAME alias-folded graph key everything
// else uses (behavioral proof:
// TestGenerateSPDX_CrossEcosystemNameCollision).
func TestSPDXIDsAreEcosystemScoped(t *testing.T) {
	s := srctest.Read(t, "internal/collector/sbom.go")
	if !strings.Contains(s, "func spdxPackageID(eco, name, version string) string") {
		t.Error("spdxPackageID must take the ecosystem")
	}
	if !strings.Contains(s, `sha256.Sum256([]byte(db.LockfileGraphKey(eco, name) + "@" + version))`) {
		t.Error("the ID hash must scope by the alias-folded graph key — bare name@version collides across ecosystems")
	}
}

// ---------------------------------------------------------------------------
// Round 25 (v0.27.144) — review 5000342003: 1 active + 2 suppressed,
// all real. The active one is SR-5 INSIDE the day-old round-24
// resolver — the class's fifth recurrence in this PR.
// ---------------------------------------------------------------------------

// Enforces SR-16 (scripts/standing_rules.go).
// Only DEFINITIVE probe responses (200 / 301 / 404-class) may decide a
// podling URL's fate; transport failures and 403/429/5xx must ERROR
// and abort the import, never demote a valid podling to unresolved.
// Behavioral proof: TestFetchAbortsOnNonDefinitiveProbeResponse.
func TestPodlingProbeDistinguishesAbsenceFromFailure(t *testing.T) {
	a := srctest.Read(t, "internal/importers/apache/apache.go")
	if !strings.Contains(a, "func resolvePodlingRepoURL(ctx context.Context, client *http.Client, repoURL, slug string) (string, bool, error)") {
		t.Error("the resolver must return an error arm — a bool alone conflates 'absent' with 'the forge is down'")
	}
	if !strings.Contains(a, "not a definitive answer") {
		t.Error("non-definitive statuses must ERROR, not fall through to the next variant")
	}
	body := srctest.FuncBody(t, a, "func Fetch(")
	if !strings.Contains(body, "return tlpProjects, fmt.Errorf(\"resolving podling") {
		t.Error("Fetch must ABORT on a probe error — the import re-runs cleanly; silent demotion loses podlings")
	}
}

// Suppressed ×2 pinned by content: the audit dump's stale views note
// corrected (plain views run every migrate since v0.27.115) and array/
// UDT columns carry their udt_name so TEXT[] vs INTEGER[] drift shows.
func TestSchemaAuditDumpCarriesUDTIdentity(t *testing.T) {
	s := srctest.Read(t, "scripts/schema_structure_dump.sql")
	if !strings.Contains(s, "THEN data_type || ':' || udt_name") {
		t.Error("array/user-defined columns must carry udt_name — bare 'ARRAY' lets real type drift pass as identical")
	}
	if !strings.Contains(s, "PLAIN views run from") {
		t.Error("the views note must reflect v0.27.115 (views.sql runs every migrate) — the stale text sent operators to hand-create ordinary views")
	}
}

// ---------------------------------------------------------------------------
// Round 26 (v0.27.147) — review 5000467615: 2 active + 2 suppressed,
// all real. Both actives are the repo's own standing rules on recent
// code (SR-2 on the added_at index; the drain-lock/RecoverStaleLocks
// race); both suppressed break real operator behavior (a stale
// repo_info snapshot defeating SR-19 convergence; the example config
// advertising env-var syntax the loader never implements).
// ---------------------------------------------------------------------------

// Active #2 — drain locks are heartbeated. LockReposForDrain parks a
// repo as status='collecting' for the WHOLE drain/heal (production has
// seen ~33h per repo on backlogged staging), while a running serve's
// periodic RecoverStaleLocks reclaims ANY 'collecting' row older than
// the 1-hour default — flipping the park back to 'queued' so routine
// collection's PurgeStagedForRepo wipes the very staging the lock
// protects. Both holders must wrap their work in StartDrainHeartbeat,
// and the ':drain' owner string has ONE spelling (SR-17).
// Behavioral proof: TestDrainHeartbeatDefeatsStaleLockRecovery.
func TestDrainLocksAreHeartbeated(t *testing.T) {
	qd := srctest.Read(t, "internal/db/queue_drain.go")
	// v0.27.150 (round 29): the heartbeat is SET-wide — the scheduler
	// lock-parks the whole drain set up front and drains sequentially,
	// so a per-repo beat left the waiting tail's locked_at frozen and
	// RecoverStaleLocks reclaimed it mid-drain.
	if !strings.Contains(qd, "func (s *PostgresStore) HeartbeatDrainLocks(ctx context.Context, workerID string)") {
		t.Fatal("HeartbeatDrainLocks (set-wide) must exist — a per-repo beat leaves the parked tail reclaimable")
	}
	hb := srctest.FuncBody(t, qd, "func (s *PostgresStore) HeartbeatDrainLocks(")
	if !strings.Contains(hb, "locked_by = $1 AND status = 'collecting'") {
		t.Error("the heartbeat must guard on locked_by + status so a straggler beat after release/reclaim is a 0-row no-op")
	}
	if strings.Contains(hb, "repo_id =") {
		t.Error("the beat must cover the worker's WHOLE parked set — a repo_id filter recreates the round-29 parked-tail gap")
	}
	// SR-17: exactly one spelling of the drain owner string — inside
	// drainLockedBy. A second inline Sprintf WILL drift.
	if n := strings.Count(srctest.StripGoComments(qd), `"%s:drain"`); n != 1 {
		t.Errorf("queue_drain.go must build the ':drain' owner in exactly one place (drainLockedBy); found %d spellings", n)
	}
	// Both holders start the ONE runner for their whole run.
	sched := srctest.Read(t, "internal/scheduler/scheduler.go")
	drain := srctest.FuncBody(t, sched, "func (s *Scheduler) processLeftoverStagingBackground(")
	hbPos := strings.Index(drain, "StartDrainHeartbeat(")
	loopPos := strings.Index(drain, "range drainSet")
	if hbPos < 0 || loopPos < 0 || hbPos > loopPos {
		t.Error("the leftover-staging drain must start the set-wide heartbeat BEFORE the drain loop (the parked tail is what round 29 protects)")
	}
	healer := srctest.Read(t, "cmd/aveloxis/heal_collection_gaps.go")
	if !strings.Contains(healer, "store.StartDrainHeartbeat(ctx, logger, workerID)") {
		t.Error("heal-collection-gaps must run the set-wide heartbeat for the whole run")
	}
}

// Suppressed #1 — the gap-heal candidate query resolves the LATEST
// repo_info snapshot. repo_info has no unique on repo_id and its
// history rotation is warn-and-continue, so multiple live snapshots
// can coexist; a bare join duplicated candidates and let a stale high
// count keep a HEALED repo in the candidate set forever — defeating
// the SR-19 "rerun until 0 candidates" convergence the flagship e2e
// pins. Behavioral proof: the stale-snapshot fixtures in
// TestGetGapHealCandidatesEndToEnd (red-proven against the bare join).
func TestGapHealCandidatesResolveLatestSnapshot(t *testing.T) {
	src := srctest.Read(t, "internal/db/gap_heal_store.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) GetGapHealCandidates(")
	if !strings.Contains(body, "JOIN LATERAL") ||
		!strings.Contains(body, "ORDER BY ri0.data_collection_date DESC NULLS LAST, ri0.repo_info_id DESC") {
		t.Error("GetGapHealCandidates must pick the latest repo_info snapshot per repo (LATERAL, date DESC with repo_info_id tiebreak)")
	}
	if strings.Contains(body, "JOIN aveloxis_data.repo_info ri USING (repo_id)") {
		t.Error("the bare repo_info join must not return — multiple live snapshots duplicate candidates and a stale count breaks convergence")
	}
}

// TestDrainHeartbeatDefeatsStaleLockRecovery — the behavioral half of
// round 26 active #2, both directions: a beaten drain lock SURVIVES
// RecoverStaleLocks past the timeout, and an un-beaten one is
// reclaimed (the pre-fix failure mode).
func TestDrainHeartbeatDefeatsStaleLockRecovery(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	// TWO parked repos (round 29): repoID is "currently draining",
	// tailID is the WAITING tail whose reclaim was the round-29 gap.
	repoID := int64(944_147_001)
	tailID := int64(944_147_002)
	const worker = "_avr26-hb"
	for _, id := range []int64{repoID, tailID} {
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repos (repo_id, repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, $2, '_avr26', 'hb', 1) ON CONFLICT (repo_id) DO NOTHING`, id, fmt.Sprintf("https://github.com/_avr26/hb%d", id))
		mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, priority, status, due_at)
			VALUES ($1, 100, 'queued', NOW()) ON CONFLICT (repo_id) DO UPDATE SET status = 'queued', locked_by = NULL, locked_at = NULL`, id)
	}
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, id := range []int64{repoID, tailID} {
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id)
			cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		}
	})

	locked, err := store.LockReposForDrain(ctx, []int64{repoID, tailID}, worker)
	if err != nil || len(locked) != 2 {
		t.Fatalf("drain lock: %v (locked %v)", err, locked)
	}
	backdate := func() {
		t.Helper()
		mustExecRetry(ctx, t, store,
			`UPDATE aveloxis_ops.collection_queue SET locked_at = NOW() - INTERVAL '2 hours' WHERE repo_id = ANY($1)`,
			[]int64{repoID, tailID})
	}
	status := func(id int64) (st string, lockedBy *string) {
		t.Helper()
		if err := store.pool.QueryRow(ctx,
			`SELECT status, locked_by FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&st, &lockedBy); err != nil {
			t.Fatal(err)
		}
		return st, lockedBy
	}

	// Direction 1: beat, then recover — the park must SURVIVE. The
	// set-wide beat takes only the workerID (round 29): it must cover
	// EVERY row this worker parked, not just a "current" one.
	backdate()
	if err := store.HeartbeatDrainLocks(ctx, worker); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecoverStaleLocks(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{repoID, tailID} {
		if st, lb := status(id); st != "collecting" || lb == nil || *lb != worker+":drain" {
			t.Fatalf("repo %d: a heartbeated drain lock must survive RecoverStaleLocks (the WAITING tail included — the round-29 gap); got status=%q locked_by=%v", id, st, lb)
		}
	}

	// Direction 2: backdate WITHOUT a beat — the pre-fix failure mode:
	// the park is reclaimed and the repos re-enter the claim path.
	backdate()
	if _, err := store.RecoverStaleLocks(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int64{repoID, tailID} {
		if st, _ := status(id); st != "queued" {
			t.Fatalf("repo %d: an un-beaten stale drain lock must be reclaimed (that risk is WHY the heartbeat exists); got status=%q", id, st)
		}
	}
}

// ---------------------------------------------------------------------------
// Round 27 (v0.27.148) — review 5000533981: 1 active + 3 suppressed,
// all real. The active one is a quantization edge INSIDE the
// day-old v0.27.139 blind-window fix (L10 again); two suppressed are
// the round-22 parse-don't-regex rule applied to its own tripwires.
// ---------------------------------------------------------------------------

// Active — the last_collected anchor is FLOORED to the whole second.
// Go's start timestamp is nanosecond-precision; forge timestamps
// serialize at second/millisecond precision truncated DOWN, so a
// sub-second anchor lets an item updated in the boundary second come
// back as updated_at == floor(start), compare Before(since) on the
// next round's breakout, and be permanently skipped — pagination
// terminating behind it. Enforces SR-18 (the floor lives in
// CompleteJob, not caller discipline). Behavioral proof: the
// fractional-start fixture in TestCompleteJobLastCollectedSemantics.
func TestCompleteJobFloorsAnchorToSecond(t *testing.T) {
	src := srctest.Read(t, "internal/db/queue.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) CompleteJob(")
	if !strings.Contains(body, "NullTime(startedAt.Truncate(time.Second))") {
		t.Error("CompleteJob must floor the anchor to the whole second — forge timestamps truncate DOWN, so a sub-second since re-opens the boundary second as a blind window")
	}
	if strings.Contains(body, "NullTime(startedAt)\n") {
		t.Error("the bare un-floored anchor must not return")
	}
}

// Suppressed #3 — the chain-root cap carries the TRUE total.
// Behavioral proof: TestChainsForReportsTotalRootsPastTheCap.
func TestVulnChainsExposeTotalRoots(t *testing.T) {
	v := srctest.Read(t, "internal/api/vulnerabilities.go")
	if !strings.Contains(v, "`json:\"introduced_by_total_roots,omitempty\"`") {
		t.Error("findings must expose introduced_by_total_roots — a capped introduced_by without the total presents a truncated remediation set as complete")
	}
	c := srctest.Read(t, "internal/api/vuln_chains.go")
	if !strings.Contains(c, "func (idx *chainIndex) chainsFor(ecosystem, pkg string) ([]vulnChainJSON, int)") {
		t.Error("chainsFor must return (chains, totalRoots) — the walk counts every root; only emitted chains are capped")
	}
}

// Suppressed #1+#2 — the import tripwires PARSE, never regex (the
// round-22 rule applied to its own kind): a backtick-quoted import
// path is legal Go that a textual scan misses, and an import-shaped
// sequence inside a comment/raw string is data a regex wrongly
// matches. Both re-red-verified with the exact bypass shapes.
func TestImportTripwiresParseNotRegex(t *testing.T) {
	sg := srctest.Read(t, "internal/srctest/selfguard_test.go")
	if !strings.Contains(sg, "parser.ImportsOnly") || !strings.Contains(sg, "strconv.Unquote(imp.Path.Value)") {
		t.Error("the srctest selfguard must verify PARSED ImportSpec paths — a raw-string import bypasses a textual search")
	}
	ig := srctest.Read(t, "scripts/import_grouping_test.go")
	if !strings.Contains(ig, "parser.ImportsOnly") || !strings.Contains(ig, "*ast.ImportSpec") {
		t.Error("the import-grouping tripwire must walk parsed ImportSpecs")
	}
	if strings.Contains(ig, "regexp.MustCompile") {
		t.Error("the regex import-block scan must not return — it false-negatives backtick paths and false-positives import-shaped text in comments/raw strings")
	}
}

// ---------------------------------------------------------------------------
// Round 28 (v0.27.148) — review 5000711915: 0 active + 2 suppressed,
// both real, both in the previous rounds' own fixes (L10). The
// gomod repo-wide budget is pinned in collector/c2_transitive_test.go
// (TestGoModGraphIsBestEffort + TestScanGoModGraphStopsOnExhaustedBudget);
// the import-grouping file-wide rule is pinned here.
// ---------------------------------------------------------------------------

// Suppressed #2 — the stdlib-first rule is FILE-wide: seenModule must
// persist across ALL import declarations, or declaration splitting
// (`import "example.com/mod"` then a separate `import "sync"`)
// bypasses the tripwire the round-27 AST rewrite had just rebuilt.
func TestImportGroupingRuleIsFileWide(t *testing.T) {
	ig := srctest.Read(t, "scripts/import_grouping_test.go")
	body := srctest.FuncBody(t, ig, "func TestImportGroupsKeepStdlibFirst(")
	declLoop := strings.Index(body, "range af.Decls")
	seen := strings.Index(body, `seenModule := ""`)
	if declLoop < 0 || seen < 0 || seen > declLoop {
		t.Error("seenModule must be declared BEFORE the decl loop (file-wide scope) — a per-declaration reset lets split import declarations bypass the stdlib-first rule")
	}
}

// ---------------------------------------------------------------------------
// Round 29 (v0.27.150) — review 5000780221: 3 active + 3 suppressed,
// all real. Actives: the round-26 heartbeat's parked-tail gap (fixed
// by widening TestDrainLocksAreHeartbeated + the two-repo behavioral
// test above), host-blind forge-ID identity, and the snapshot replace
// wiping the Go closure on incomplete expansion (pinned in
// collector/c2_transitive_test.go + the preserve behavioral test).
// ---------------------------------------------------------------------------

// Active #2 — the forge-ID lookup is HOST-scoped. detectPlatform
// classifies ANY gitlab-bearing hostname as PlatformGitLab, and
// GitLab project IDs are only unique PER INSTANCE — a bare
// (platform, id) match could read project 123 on two different hosts
// as a RENAME and rewrite one unrelated row's URL onto the other's
// project. Enforces SR-6 (never fabricate identity from an ambiguous
// match).
func TestForgeIDLookupIsHostScoped(t *testing.T) {
	src := srctest.Read(t, "internal/db/repo_forge_id.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) FindRepoByPlatformRepoID(")
	if !strings.Contains(body, "lower(split_part(repo_git, '/', 3)) = $3") {
		t.Error("FindRepoByPlatformRepoID must filter by the URL's host — GitLab project IDs are per-instance, not global")
	}
	pg := srctest.Read(t, "internal/db/postgres.go")
	if !strings.Contains(pg, "FindRepoByPlatformRepoID(ctx, r.Platform, r.PlatformID, ForgeHostOf(r.GitURL))") {
		t.Error("the rename-heal probe must pass the incoming URL's host")
	}
}

// TestForgeIDRenameHealNeverCrossesHosts — the behavioral half of
// round 29 active #2: two platform-2 rows with the SAME numeric
// project ID on DIFFERENT GitLab instances; upserting a renamed URL
// on one host must heal ONLY that host's row.
func TestForgeIDRenameHealNeverCrossesHosts(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	const forgeID = "944150777"
	comURL := "https://gitlab.com/_avr29/proj"
	gnomeURL := "https://gitlab.example-gnome.org/_avr29/proj"
	t.Cleanup(func() {
		cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE platform_repo_id = $1)`, forgeID)
		cleanupExecRetry(cctx, store, `DELETE FROM aveloxis_data.repos WHERE platform_repo_id = $1`, forgeID)
	})

	seed := func(gitURL string) int64 {
		t.Helper()
		id, serr := store.UpsertRepo(ctx, &model.Repo{
			Platform: model.PlatformGitLab, GitURL: gitURL,
			Owner: "_avr29", Name: "proj", PlatformID: forgeID,
		})
		if serr != nil {
			t.Fatal(serr)
		}
		return id
	}
	comID := seed(comURL)
	gnomeID := seed(gnomeURL)
	if comID == gnomeID {
		t.Fatalf("same project ID on two instances must be two rows — a shared row means the host-blind merge already happened (com=%d gnome=%d)", comID, gnomeID)
	}

	// A rename on gitlab.com: same forge ID, new gitlab.com URL. The
	// heal must land on the gitlab.com row and leave the other
	// instance's row untouched.
	renamedID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitLab, GitURL: "https://gitlab.com/_avr29/proj-renamed",
		Owner: "_avr29", Name: "proj-renamed", PlatformID: forgeID,
	})
	if err != nil {
		t.Fatal(err)
	}
	if renamedID != comID {
		t.Errorf("the rename must heal the SAME-host row (want %d, got %d)", comID, renamedID)
	}
	var gnomeGit string
	if err := store.pool.QueryRow(ctx,
		`SELECT repo_git FROM aveloxis_data.repos WHERE repo_id = $1`, gnomeID).Scan(&gnomeGit); err != nil {
		t.Fatal(err)
	}
	if gnomeGit != gnomeURL {
		t.Errorf("the OTHER instance's row must be untouched by a same-ID rename elsewhere — got %q (host-blind heal rewrote an unrelated project)", gnomeGit)
	}
}

// Suppressed #1 — the candidate query LEFT-joins repo_info so the
// --all completeness sweep includes collected repos with NO snapshot
// (force-list mode needs no counts); gap mode still excludes them via
// the COALESCE-to-zero predicate. Behavioral: the nosnap fixture in
// TestGetGapHealCandidatesEndToEnd.
func TestGapHealCandidatesIncludeSnapshotlessInAll(t *testing.T) {
	src := srctest.Read(t, "internal/db/gap_heal_store.go")
	body := srctest.FuncBody(t, src, "func (s *PostgresStore) GetGapHealCandidates(")
	if !strings.Contains(body, "LEFT JOIN LATERAL") {
		t.Error("repo_info must LEFT-join — an inner join drops snapshotless collected repos from the --all sweep")
	}
	if !strings.Contains(body, "COALESCE(ri.issues_count, 0)") {
		t.Error("missing snapshots must COALESCE to zero counts")
	}
}

// ---------------------------------------------------------------------------
// Round 30 (v0.27.151) — review 5000856729: 2 active + 2 suppressed,
// all real. The actives are the round-19 cross-lockfile class in the
// SBOM's costume — the chain index was partitioned in v0.27.135 while
// the SBOM graph kept repo-wide name maps.
// ---------------------------------------------------------------------------

// Actives — SBOM edge resolution is per-lockfile via the ONE shared
// sbomGraphIndex (SR-17: both formats use the same resolver, so they
// cannot diverge again). Behavioral proofs (red-proven against the
// repo-wide shape): TestGenerateCycloneDX_EdgesNeverCrossLockfiles +
// TestGenerateSPDX_EdgesNeverCrossLockfiles.
func TestSBOMGraphResolvesPerLockfile(t *testing.T) {
	s := srctest.Read(t, "internal/collector/sbom.go")
	for _, needle := range []string{
		"type sbomGraphIndex struct",
		"lockByName      map[string]map[string][]string",
		"func (x *sbomGraphIndex) parentRefs(e db.RepoLockfileEdge) []string",
		"func (x *sbomGraphIndex) childRefs(e db.RepoLockfileEdge) []string",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("sbom.go missing %q — edge endpoints must resolve inside the edge's own lockfile (repo-wide maps fabricated cross-workspace dependencies)", needle)
		}
	}
	if n := strings.Count(s, "gidx.parentRefs(e)"); n != 2 {
		t.Errorf("BOTH generators must resolve through the shared index (found %d parentRefs call sites, want 2)", n)
	}
}

// Suppressed #1 — the podling probe follows the forge's rename
// redirect: a validated same-forge /apache/<name> Location resolves
// directly (an Apache rename can target an ARBITRARY new slug, not
// just the twin); unsafe/malformed redirects ERROR per the round-25
// definitive-only rule. Behavioral:
// TestFetchFollowsRenameRedirectToArbitrarySlug +
// TestFetchAbortsOnUnsafeRedirectTarget.
func TestPodlingProbeFollowsValidatedRedirects(t *testing.T) {
	a := srctest.Read(t, "internal/importers/apache/apache.go")
	if !strings.Contains(a, "func podlingRedirectTarget(") {
		t.Fatal("the probe must validate and follow redirect targets — ignoring Location drops renamed podlings as unresolved")
	}
	body := srctest.FuncBody(t, a, "func podlingRedirectTarget(")
	if !strings.Contains(body, "strings.EqualFold(loc.Host, req.URL.Host)") {
		t.Error("redirect targets must stay on the SAME forge host")
	}
	if !strings.Contains(a, "`^/apache/([A-Za-z0-9_.-]+)/?$`") {
		t.Error("only /apache/<repo> paths are acceptable redirect targets")
	}
}

// Suppressed #2 — the data-test report gates AND renders TypeChanged:
// a release whose only schema drift is a column type change produced
// no drift section at all despite ColumnFillDiff detecting it.
func TestDataTestReportRendersTypeChanges(t *testing.T) {
	d := srctest.Read(t, "cmd/aveloxis/data_test_cmd.go")
	if !strings.Contains(d, "len(colReport.TypeChanged) > 0 {") {
		t.Error("TypeChanged must be rendered in the schema-drift section")
	}
	if !strings.Contains(d, "+len(colReport.TypeChanged) > 0 {") {
		t.Error("TypeChanged must be part of the drift-section GATE — type-only drift previously produced no section")
	}
}
