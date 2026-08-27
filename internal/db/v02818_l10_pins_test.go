// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.28.18 L10 re-run pins — the fresh-context pass over this release's
// own fixes found eight defects; these keep them closed.

func TestSchemaVersionAtLeast(t *testing.T) {
	cases := []struct {
		have, want string
		ok         bool
	}{
		{"0.27.37", "0.27.37", true},
		{"0.28.17", "0.27.37", true},
		{"1.0.0", "0.27.37", true},
		{"0.27.36", "0.27.37", false},
		{"0.9.9", "0.27.37", false},
		{"0.27", "0.27.37", false},
		{"0.27.37.1", "0.27.37", true},
		{"", "0.27.37", false},
		{"garbage", "0.27.37", false},
		{"0.27.37", "x", false},
	}
	for _, c := range cases {
		if got := schemaVersionAtLeast(c.have, c.want); got != c.ok {
			t.Errorf("schemaVersionAtLeast(%q, %q) = %v, want %v", c.have, c.want, got, c.ok)
		}
	}
}

// The v0.27.37 GitLab force-full step is ledgered AND seeded: a fleet
// whose stamp proves a >= 0.27.37 migrate completed has already run it
// on every migrate, so upgrading must not force one more fleet-wide
// GitLab full pass.
func TestGitLabForceFullStepIsSeededFromThePriorStamp(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	const label = `"v0.27.37 force full recollect for GitLab repos (main-path comment drop heal)"`
	seed := strings.Index(src, "runOnceSeedIfApplied(ctx, pg, logger,\n\t\t"+label+", \"0.27.37\")")
	step := strings.Index(src, "runOnceStep(ctx, pg, logger, errs,\n\t\t"+label)
	if seed < 0 || step < 0 {
		t.Fatalf("expected the seed call (%d) and the ledgered step (%d) for the v0.27.37 label", seed, step)
	}
	if seed > step {
		t.Error("the ledger seed must precede the ledgered step, or the step runs before the seed can skip it")
	}
	if strings.Contains(src, "execMigrationStep(ctx, pg, logger, errs,\n\t\t"+label) {
		t.Error("the v0.27.37 step must not be a plain execMigrationStep (it re-flags every GitLab repo on every migrate)")
	}
	body := srctest.FuncBody(t, readSourceFile(t, "migration_ledger.go"), "func runOnceSeedIfApplied(")
	for _, needle := range []string{"GetSchemaVersion(ctx)", "schemaVersionAtLeast(prior, appliedSince)", "ON CONFLICT (step_label) DO NOTHING"} {
		if !strings.Contains(body, needle) {
			t.Errorf("runOnceSeedIfApplied must contain %s", needle)
		}
	}
	if !strings.Contains(body, `prior == ""`) {
		t.Error("an absent stamp (fresh or pre-v0.14.5 database) must leave the step to run")
	}
}

// repo_info_history never inherited idx_repo_info_repo_id (created after
// the LIKE clone), and the unknown-count carry-forward reads it on the
// collection hot path: migration-owned CONCURRENTLY (SR-2), never a
// schema.sql declaration, never a drop target.
func TestRepoInfoHistoryRepoIDIndexIsMigrationOnly(t *testing.T) {
	migrate := readSourceFile(t, "migrate.go")
	schema := readSourceFile(t, "schema.sql")
	const name = "idx_repo_info_history_repo_id"
	if strings.Contains(schema, name) {
		t.Errorf("schema.sql declares %s — SR-2: keep it migration-only", name)
	}
	cic := regexp.MustCompile(`execCreateIndexConcurrently\(ctx, pg, logger, errs, "aveloxis_data", "` + name + `",\s*` + "`CREATE INDEX CONCURRENTLY IF NOT EXISTS " + name + `\s+ON aveloxis_data\.repo_info_history \(repo_id\)`)
	if !cic.MatchString(migrate) {
		t.Errorf("migrate.go must build %s via execCreateIndexConcurrently on repo_info_history (repo_id)", name)
	}
	drop := regexp.MustCompile(`(?i)DROP INDEX[^;` + "`" + `"]*` + name)
	if drop.MatchString(migrate) {
		t.Errorf("%s appears in a DROP INDEX statement — a dropped name rebuilds every migrate (SR-4)", name)
	}
}

// Both dm_ aggregate callers (the weekly scheduler rebuild and
// `refresh-views --aggregates`) go through RefreshAllRepoAggregates, and
// the layer serializes them with an advisory lock (SR-18) — the dm_*
// tables have no PK/UNIQUE, so two interleaved DELETE+INSERT passes
// duplicate aggregate rows.
func TestRefreshAllRepoAggregatesHoldsAnAdvisoryLock(t *testing.T) {
	body := srctest.FuncBody(t, readSourceFile(t, "aggregates.go"), "func (s *PostgresStore) RefreshAllRepoAggregates(")
	for _, needle := range []string{"pg_try_advisory_lock($1)", "DMAggregatesAdvisoryLockID", "return ErrAggregateRebuildRunning", "pg_advisory_unlock($1)"} {
		if !strings.Contains(body, needle) {
			t.Errorf("RefreshAllRepoAggregates must contain %s", needle)
		}
	}
	if strings.Contains(body, "pg_advisory_lock($1)") {
		t.Error("the aggregate pass must TRY the lock, never block on it (a blocked waiter holds a snapshot — the v0.27.20 class)")
	}
	if DMAggregatesAdvisoryLockID == MigrateAdvisoryLockID {
		t.Error("the aggregate lock must not share the migrate advisory lock id")
	}
	sched := srctest.Read(t, "internal/scheduler/scheduler.go")
	cmd := srctest.Read(t, "cmd/aveloxis/main.go")
	for _, needle := range []string{"RefreshRepoAggregates(", "RefreshRepoGroupAggregates("} {
		if strings.Contains(srctest.StripGoComments(sched), needle) || strings.Contains(srctest.StripGoComments(cmd), needle) {
			t.Errorf("a caller invokes the per-repo %s directly, bypassing the locked bulk pass", needle)
		}
	}
}

// The list dedup runs as ONE transaction on rows locked FOR UPDATE,
// keeps ONE staging row per (winner, header) across the WHOLE partition
// before repointing (mailing_list_staging is UNIQUE (rgls_id,
// message_id_header): two losers sharing a header collide with each
// other, not just with the winner), partitions by the CANONICAL group
// (the v0.27.17 consolidation repoints same-named groups' list rows) on
// rgls_email IS NOT NULL — empty-string addresses collide on the UNIQUE
// like real ones — and merges the losers' checkpoints into the winner.
func TestListDedupIsTransactionalAndCollisionAware(t *testing.T) {
	src := readSourceFile(t, "email_message_fk_indexes.go")
	outer := srctest.FuncBody(t, src, "func dedupRepoGroupsListServe(")
	for _, needle := range []string{"pg.pool.Begin(ctx)", "tx.Commit(ctx)", "tx.Rollback(ctx)", "dedupRepoGroupsListServeTx(ctx, tx, logger, MailingListStaleLock.String(), pg.ownBackendPIDs())"} {
		if !strings.Contains(outer, needle) {
			t.Errorf("dedupRepoGroupsListServe must contain %s", needle)
		}
	}
	body := srctest.FuncBody(t, src, "func dedupRepoGroupsListServeTx(")
	for _, needle := range []string{"FOR UPDATE", "dupListPartitionsSQL", "WHERE copies > 1", "c.ageLive && serveElsewhere", "unnest($1::bigint[], $2::bigint[]) AS w(rgls_id, winner)", "repo_group_id = (SELECT repo_group_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = w.winner)", "PARTITION BY w.winner, st.message_id_header", "ORDER BY (st.rgls_id = w.winner) DESC, st.mls_id", "GREATEST(COALESCE(r.mlls_last_month, ''), agg.last_month)"} {
		if !strings.Contains(body, needle) {
			t.Errorf("dedupRepoGroupsListServeTx must contain %s", needle)
		}
	}
	if strings.Contains(body, "execMigrationStep(") || strings.Contains(body, "pg.pool.") {
		t.Error("every statement must run on the caller's tx (a partial failure must roll the whole pass back)")
	}
	if strings.Contains(body, "COALESCE(rgls_email, '') <> ''") {
		t.Error("empty-string addresses must be deduplicated too — only NULLs are distinct under the UNIQUE")
	}
	lock := strings.Index(body, "FOR UPDATE")
	recheck := strings.Index(body, "re-checking worker locks")
	del := strings.Index(body, `"staging_duplicates_deleted"`)
	rep := strings.Index(body, `"staging_repointed"`)
	if lock < 0 || recheck < 0 || del < 0 || rep < 0 || lock > recheck || recheck > del || del > rep {
		t.Error("order must be: lock the frozen members FOR UPDATE, re-check worker locks, delete duplicate staging across the partition, then repoint")
	}
	// A young lock is live only while an aveloxis-serve OTHER than this
	// process is connected (`aveloxis serve` runs its startup migrate on
	// its own tagged pool before any worker exists; `stop serve` →
	// `migrate` must not read mid-scan ghosts as running workers). No PID
	// rule: PIDs are namespaced and boot ids host-global in containers.
	if strings.Contains(src, "pidfile.IsRunning") || strings.Contains(src, "hostid.BootID") {
		t.Error("no same-host PID/boot-id liveness rule — it is wrong in both directions under the container deployment (tenth pass)")
	}
	if !strings.Contains(src, "func serveBackendsBeyondOwnPool(ctx context.Context, tx pgx.Tx, ownPIDs []int32)") {
		t.Error("the probe must take pgx.Tx — its two statements (snapshot clear, then read) must run on ONE session")
	}
	probe := srctest.FuncBody(t, src, "func serveBackendsBeyondOwnPool(")
	if !strings.Contains(probe, "a.datname = current_database() AND a.application_name = $1") {
		t.Error("serveBackendsBeyondOwnPool must filter pg_stat_activity by THIS database and the serve application_name (pg_stat_activity is cluster-wide)")
	}
	clear := strings.Index(probe, "SELECT pg_stat_clear_snapshot()")
	read := strings.Index(probe, "FROM pg_stat_activity")
	if clear < 0 || read < 0 || clear > read {
		t.Error("the activity snapshot must be cleared in its OWN statement before the pg_stat_activity read — a same-statement clear runs after the read (proven on PG 18)")
	}
	if strings.Contains(probe, "pg_stat_activity, pg_stat_clear_snapshot()") {
		t.Error("the same-statement clear form is decorative")
	}
	// serve's own backends are excluded exactly, by server PID from the
	// pool's AfterConnect/BeforeClose hooks — never by pool counters
	// (TotalConns includes constructing connections; an own over-count
	// failed toward "ghost") and never by client_addr (a pooler or NAT
	// collapses it, excluding OTHER serves — the unsafe direction).
	if !strings.Contains(probe, "a.pid <> ALL($2::int4[])") {
		t.Error("serveBackendsBeyondOwnPool must exclude this process's own backend PIDs")
	}
	if strings.Contains(probe, "client_addr") || strings.Contains(probe, "TotalConns()") {
		t.Error("own-backend exclusion must be by server PID only")
	}
	pool := readSourceFile(t, "postgres.go")
	for _, needle := range []string{"cfg.AfterConnect = func(", "cfg.BeforeClose = func(", "store.trackBackend(conn.PgConn().PID(), true)", "store.trackBackend(conn.PgConn().PID(), false)"} {
		if !strings.Contains(pool, needle) {
			t.Errorf("NewPostgresStore must maintain the own-backend PID set via the pool hooks: missing %s", needle)
		}
	}
	// Membership must be frozen: the partition window is spelled once, in
	// dupListPartitionsSQL, and no statement re-derives the set.
	if strings.Count(src, "WINDOW w AS") != 1 {
		t.Errorf("the partition window must be spelled exactly once (dupListPartitionsSQL), found %d", strings.Count(src, "WINDOW w AS"))
	}
	if strings.Contains(srctest.FuncBody(t, src, "func listDedupPending("), "WINDOW") {
		t.Error("listDedupPending must reuse dupListPartitionsSQL, not spell the partition itself")
	}
	// pg_stat_activity readers filter by database: the cluster hosts two
	// aveloxis databases (the sixth-pass L11 sweep).
	for _, site := range []struct{ file, fn string }{{"postgres.go", "func (s *PostgresStore) PidsByAppName("}, {"migrate.go", "func checkBlockers("}} {
		fsrc := readSourceFile(t, site.file)
		if !strings.Contains(fsrc, site.fn) {
			t.Errorf("%s no longer defines %s — re-anchor the datname pin", site.file, site.fn)
			continue
		}
		if !strings.Contains(srctest.FuncBody(t, fsrc, site.fn), "datname = current_database()") {
			t.Errorf("%s %s reads pg_stat_activity without a datname filter — pg_stat_activity is cluster-wide", site.file, site.fn)
		}
	}
	// The consolidation repoints the staging table's repo_group_id too
	// (list identity for DrainList); the dedup's step 2 does the same.
	if !strings.Contains(readSourceFile(t, "migrate.go"), `"aveloxis_ops.mailing_list_staging",`) {
		t.Error("consolidateRepoGroups must repoint aveloxis_ops.mailing_list_staging.repo_group_id to the canonical group")
	}
	// The analyzer must still see the CONCURRENTLY build this dedup guards.
	migrate := readSourceFile(t, "migrate.go")
	dedup := strings.Index(migrate, "dedupRepoGroupsListServe(ctx, pg, logger, errs)")
	unique := strings.Index(migrate, `"idx_rgls_group_email"`)
	if dedup < 0 || unique < 0 || dedup > unique {
		t.Errorf("dedupRepoGroupsListServe (%d) must run before the idx_rgls_group_email build (%d) — SR-1", dedup, unique)
	}
}
