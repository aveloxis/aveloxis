// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
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
	for _, needle := range []string{"pg.pool.Begin(ctx)", "tx.Commit(ctx)", "tx.Rollback(ctx)", "dedupRepoGroupsListServeTx(ctx, tx, logger, pg.ownBackendPIDs)"} {
		if !strings.Contains(outer, needle) {
			t.Errorf("dedupRepoGroupsListServe must contain %s", needle)
		}
	}
	body := srctest.FuncBody(t, src, "func dedupRepoGroupsListServeTx(")
	for _, needle := range []string{"FOR UPDATE", "dupListPartitionsSQL", "WHERE copies > 1", "unnest($1::bigint[], $2::bigint[]) AS w(rgls_id, winner)", "repo_group_id = (SELECT repo_group_id FROM aveloxis_data.repo_groups_list_serve WHERE rgls_id = w.winner)", "PARTITION BY w.winner, st.message_id_header", "ORDER BY (st.rgls_id = w.winner) DESC, st.mls_id", "GREATEST(COALESCE(r.mlls_last_month, ''), agg.last_month)"} {
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
	del := strings.Index(body, `"staging_duplicates_deleted"`)
	rep := strings.Index(body, `"staging_repointed"`)
	emailRep := strings.Index(body, `"email_messages_repointed"`)
	listDel := strings.Index(body, `"list_rows_deleted"`)
	if lock < 0 || del < 0 || rep < 0 || emailRep < 0 || listDel < 0 || lock > del || del > rep || rep > emailRep || emailRep > listDel {
		t.Error("order must be: lock the frozen members FOR UPDATE, delete duplicate staging across the partition, repoint staging, repoint email_message, then delete the loser list rows (a reading convention — the deferred FK re-reads current state at COMMIT, so the e2e proves the repoint, not the order)")
	}
	// Every consolidation that deletes a repos row passes THE shared
	// gate (SR-18) before any write — dedup-repos per batch,
	// reconcile-repos' pair consolidation per pair, prelim's rename heal
	// — and the gate derives its columns and index names from
	// emailMessageFKIndexes (SR-17), never a hand list.
	// ret pins the statement that FOLLOWS the gate — matched as a PREFIX
	// of the source from the (exactly one) gate call, anchored on the
	// closing brace (`return …, err }`) — so neither a substituted
	// expression (`errPairCollecting`, `errors.New(…)`, `errors.Join(err,
	// …)`), a statement slipped in between, nor a decorative first gate
	// with a well-formed copy after the write can pass. Two shapes: the
	// three entry points return the gate's error UNWRAPPED — the pin
	// rejects even a `%w` wrap, which would keep errors.Is working,
	// because none of them has anything to add to the gate's own message
	// and both CLIs classify on the sentinel (dedup-repos stops with
	// "precondition unmet"; reconcile-repos counts each refused repo for
	// its nonzero exit; prelim never inspects it); the list dedup
	// instead classifies on the sentinel FIRST and then wraps `%w` with
	// its step label for the migrate collector.
	for _, site := range []struct{ file, fn, needle, ret, why string }{
		{"repo_dedup.go", "func DedupCaseVariantReposBatch(", `emailMessageFKIndexesReadyFor(ctx, store.pool, "repos")`, "return 0, 0, err }",
			`dedup-repos classifies on the sentinel — a substituted or rewrapped error would log "batch dedup errored mid-way" for a refusal`},
		{"stranded_repos.go", "func DedupRenamedRepoPair(", `emailMessageFKIndexesReadyFor(ctx, store.pool, "repos")`, "return err }",
			"reconcile-repos classifies on the sentinel to exit nonzero after a refused run"},
		{"rename_duplicate_heal.go", "func (s *PostgresStore) HealRenamedDuplicate(", `emailMessageFKIndexesReadyFor(ctx, s.pool, "repos")`, "return false, err }",
			"reconcile-repos classifies on the sentinel to exit nonzero after a refused run (prelim treats any error as heal-unavailable)"},
		{"email_message_fk_indexes.go", "func dedupRepoGroupsListServe(", `emailMessageFKIndexesReadyFor(ctx, pg.pool, "repo_groups_list_serve")`, "if errors.Is(err, ErrEmailMessageIndexesNotReady) {",
			"a rewrap before the classification would turn the WARN skip into a failed migrate step"},
	} {
		fnBody := srctest.FuncBody(t, readSourceFile(t, site.file), site.fn)
		if n := strings.Count(fnBody, site.needle); n != 1 {
			t.Errorf("%s must pass the gate exactly once, found %d", site.fn, n)
			continue
		}
		gate := strings.Index(fnBody, site.needle)
		write := strings.Index(fnBody, "Begin(ctx)")
		if write < 0 {
			write = strings.Index(fnBody, "dedupOnePair(")
		}
		if write < 0 || gate > write {
			t.Errorf("%s must pass %s BEFORE it opens a transaction or hands off to dedupOnePair", site.fn, site.needle)
			continue
		}
		if !strings.HasPrefix(srctest.NormalizeWS(fnBody[gate:]), srctest.NormalizeWS(site.needle+"; err != nil { "+site.ret)) {
			t.Errorf("%s must follow the gate with exactly `%s` — %s", site.fn, site.ret, site.why)
		}
	}
	gateBody := srctest.FuncBody(t, src, "func emailMessageFKIndexesReadyFor(")
	if !strings.Contains(gateBody, "range emailMessageFKIndexes") || !strings.Contains(gateBody, "idx.indexName") || strings.Contains(gateBody, `"idx_email_message_`) {
		t.Error("the gate must derive its columns and index names from emailMessageFKIndexes — no hand-spelled index names (SR-17)")
	}
	if !strings.Contains(gateBody, "probed == 0") {
		t.Error("an unknown parent must never read as ready")
	}
	if !strings.Contains(gateBody, "%w") || !strings.Contains(gateBody, "ErrEmailMessageIndexesNotReady") {
		t.Error("the not-ready refusal must wrap ErrEmailMessageIndexesNotReady so callers can tell it from a probe error")
	}
	// The batch window EXCLUDES mid-collection pairs (a batchSize-long
	// collecting head stalled the rerun-until-0 contract); the dry-run
	// sample keeps them in view, flagged.
	dedupSrc := readSourceFile(t, "repo_dedup.go")
	if !strings.Contains(srctest.FuncBody(t, dedupSrc, "func DedupCaseVariantReposBatch("), "sampleCaseVariantRepoDups(ctx, store, batchSize, true)") {
		t.Error("DedupCaseVariantReposBatch must read its window with excludeCollecting = true")
	}
	if !strings.Contains(srctest.FuncBody(t, dedupSrc, "func SampleCaseVariantRepoDups("), "sampleCaseVariantRepoDups(ctx, store, limit, false)") {
		t.Error("SampleCaseVariantRepoDups (the dry-run read) must keep collecting pairs in view")
	}
	if !strings.Contains(dedupSrc, "WHERE NOT ($2::boolean AND collecting)") {
		t.Error("repoDupCandidatesSQL must filter collecting pairs on $2")
	}
	// The list dedup is gated on a VALID idx_email_message_rgls_id (a
	// failed CONCURRENTLY build is only recorded — the decorative-gate
	// class) through THE shared gate — no inline probe, no hand-spelled
	// index name — and a not-ready refusal is a WARN skip, a probe error
	// a failed step.
	outerBody := srctest.FuncBody(t, src, "func dedupRepoGroupsListServe(")
	if strings.Contains(outerBody, "leadingColumnIndexValid(") || strings.Contains(outerBody, "idx_email_message_rgls_id") {
		t.Error("dedupRepoGroupsListServe must use emailMessageFKIndexesReadyFor, not an inline probe or a hand-spelled index name")
	}
	if !strings.Contains(outerBody, "errors.Is(err, ErrEmailMessageIndexesNotReady)") {
		t.Error("dedupRepoGroupsListServe must distinguish the not-ready skip from a probe error")
	}
	// THE rule: another aveloxis-serve connected → consolidate NOTHING
	// (the drain holds no lock, so no lock-age rule can call a row idle);
	// probed before and again after the FOR UPDATE. No PID/boot-id rule:
	// PIDs are namespaced and boot ids host-global in containers; no
	// lock-age rule: mlls_locked_at is stamped by the scan only.
	if strings.Contains(src, "pidfile.IsRunning") || strings.Contains(src, "hostid.BootID") {
		t.Error("no same-host PID/boot-id liveness rule — it is wrong in both directions under the container deployment (tenth pass)")
	}
	if code := srctest.StripGoComments(src); strings.Contains(code, "mlls_locked_at") || strings.Contains(code, "MailingListStaleLock") {
		t.Error("no lock-age rule — the drain never stamps a lock, so 'no young lock' never meant 'idle' (eleventh pass)")
	}
	if strings.Count(body, "serveBackendsBeyondOwnPool(ctx, tx, ownPIDs)") != 2 {
		t.Error("the serve probe must run before the FOR UPDATE and again on the locked set")
	}
	if strings.Contains(readSourceFile(t, "../../cmd/aveloxis/main.go"), `ConnectionStringWithAppName("aveloxis-serve")`) || !strings.Contains(readSourceFile(t, "../../cmd/aveloxis/main.go"), "ConnectionStringWithAppName(db.ServeApplicationName)") {
		t.Error("runServe must tag its pool with db.ServeApplicationName (one shared spelling — the probe counts 0 forever if the literals drift)")
	}
	if !strings.Contains(src, "func serveBackendsBeyondOwnPool(ctx context.Context, tx pgx.Tx, ownPIDs func() []int32)") {
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

// The gate never reads "ready" for a parent it has no index registered
// for — a wrong caller must not pass by accident (the loop would be a
// no-op otherwise). No database: the loop never queries.
func TestEmailMessageIndexGateRejectsUnknownParent(t *testing.T) {
	err := emailMessageFKIndexesReadyFor(context.Background(), nil, "not_a_parent")
	if err == nil || !strings.Contains(err.Error(), `"not_a_parent"`) {
		t.Fatalf("unknown parent must error naming it, got %v", err)
	}
	for _, parent := range []string{"repos", "repo_groups_list_serve"} {
		n := 0
		for _, idx := range emailMessageFKIndexes {
			if idx.parent == parent {
				n++
			}
		}
		if n == 0 {
			t.Errorf("emailMessageFKIndexes must register at least one index for parent %q", parent)
		}
	}
}
