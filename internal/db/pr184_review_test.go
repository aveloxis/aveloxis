// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.106 — Copilot review round on PR #184 (all 8 findings verified;
// 7 taken, 1 declined as a deliberate Augur-parity behavior). This file
// pins the db-side fixes.
package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"
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
func TestColumnTripwireScopesToWriteStatements(t *testing.T) {
	src, err := os.ReadFile("column_writer_tripwire_test.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	// The table-scoped statement matcher (regex form INSERT\s+INTO|UPDATE
	// anchored to aveloxis_data.<table>).
	if !strings.Contains(s, `INSERT\s+INTO|UPDATE`) || !strings.Contains(s, "aveloxis_data") {
		t.Error("tripwire must scope matches to INSERT/UPDATE statements naming the audited table")
	}
	if !strings.Contains(s, "commentRe") {
		t.Error("tripwire must strip SQL comments so commented mentions can't satisfy the writer check")
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
