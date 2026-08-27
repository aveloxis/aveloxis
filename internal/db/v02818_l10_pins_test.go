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

// The list dedup runs as ONE transaction, deletes loser staging rows the
// winner already holds BEFORE repointing (mailing_list_staging is UNIQUE
// (rgls_id, message_id_header)), and partitions on rgls_email IS NOT NULL
// — empty-string addresses collide on the UNIQUE like real ones.
func TestListDedupIsTransactionalAndCollisionAware(t *testing.T) {
	body := srctest.FuncBody(t, readSourceFile(t, "email_message_fk_indexes.go"), "func dedupRepoGroupsListServe(")
	for _, needle := range []string{"pg.pool.Begin(ctx)", "tx.Commit(ctx)", "tx.Rollback(ctx)", "WHERE rgls_email IS NOT NULL", "PARTITION BY w.winner, st.message_id_header", "ORDER BY (st.rgls_id = w.winner) DESC, st.mls_id", "GREATEST(COALESCE(r.mlls_last_month, ''), agg.last_month)", "NOT live_locked"} {
		if !strings.Contains(body, needle) {
			t.Errorf("dedupRepoGroupsListServe must contain %s", needle)
		}
	}
	if strings.Contains(body, "execMigrationStep(") {
		t.Error("the four statements must run inside the ONE transaction, not as independent execMigrationStep calls (a partial failure orphans staging rows)")
	}
	if strings.Contains(body, "COALESCE(rgls_email, '') <> ''") {
		t.Error("empty-string addresses must be deduplicated too — only NULLs are distinct under the UNIQUE")
	}
	del := strings.Index(body, "delete duplicate staging rows across each partition")
	rep := strings.Index(body, "repoint remaining loser staging rows")
	if del < 0 || rep < 0 || del > rep {
		t.Error("the staging collision delete must precede the staging repoint")
	}
	// The analyzer must still see the CONCURRENTLY build this dedup guards.
	migrate := readSourceFile(t, "migrate.go")
	dedup := strings.Index(migrate, "dedupRepoGroupsListServe(ctx, pg, logger, errs)")
	unique := strings.Index(migrate, `"idx_rgls_group_email"`)
	if dedup < 0 || unique < 0 || dedup > unique {
		t.Errorf("dedupRepoGroupsListServe (%d) must run before the idx_rgls_group_email build (%d) — SR-1", dedup, unique)
	}
}
