// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Enforces SR-2 (migration-only introducing indexes on fleet-scale
// tables): the v0.28.15 repo_groups FK-child indexes are created by
// ensureRepoGroupFKIndexes via CONCURRENTLY and are NOT declared in
// schema.sql, and the ensure call precedes the v0.27.17 consolidation
// whose loser DELETE they exist to make tractable.
func TestRepoGroupFKIndexesRunBeforeConsolidation(t *testing.T) {
	migrate := readSourceFile(t, "migrate.go")
	schema := readSourceFile(t, "schema.sql")
	helper := readSourceFile(t, "repo_group_fk_indexes.go")

	if !strings.Contains(helper, "execCreateIndexConcurrently(") {
		t.Fatal("repo_group_fk_indexes.go must build its indexes via execCreateIndexConcurrently (CONCURRENTLY + INVALID recovery)")
	}
	for _, idx := range repoGroupFKIndexes {
		if !strings.Contains(helper, idx.indexName) {
			t.Errorf("repo_group_fk_indexes.go missing index %s", idx.indexName)
		}
		if strings.Contains(schema, idx.indexName) {
			t.Errorf("schema.sql declares %s — SR-2: the base DDL runs before any migration step, so a plain declaration block-builds on a live fleet; keep it migration-only", idx.indexName)
		}
	}

	// v0.28.16: the consolidation lives in consolidateRepoGroups; the
	// ensure call must precede the consolidation CALL (same function,
	// so file order is execution order), and that call must sit inside
	// the readiness gate — an index step that only records its failure
	// would otherwise let the loser DELETE run unindexed anyway (the
	// decorative-gate class; Copilot round on PR #191).
	call := strings.Index(migrate, "ensureRepoGroupFKIndexes(ctx, pg, logger, errs)")
	consolidation := strings.Index(migrate, "consolidateRepoGroups(ctx, pg, logger, errs)")
	if call < 0 || consolidation < 0 {
		t.Fatalf("expected both the ensureRepoGroupFKIndexes call (%d) and the consolidateRepoGroups call (%d) in migrate.go", call, consolidation)
	}
	if call > consolidation {
		t.Errorf("ensureRepoGroupFKIndexes (pos %d) must run BEFORE consolidateRepoGroups (pos %d): the loser DELETE's deferred FK checks are the whole reason the indexes exist", call, consolidation)
	}
	if strings.Count(migrate, "consolidateRepoGroups(ctx, pg, logger, errs)") != 1 {
		t.Errorf("consolidateRepoGroups must be called from exactly one site (the gated one)")
	}
	between := migrate[call:consolidation]
	if !strings.Contains(between, "repoGroupFKIndexesReady(ctx, pg)") {
		t.Errorf("the consolidation call must be gated on repoGroupFKIndexesReady between the index build and the call — found no readiness probe in between")
	}
	// v0.28.18 (sixth pass): a duplicate (group, list) partition a live
	// worker lock kept the stage-2 dedup from consolidating collides on
	// the UNIQUE the moment the consolidation repoints it.
	if !strings.Contains(between, "listDedupPending(ctx, pg.pool, MailingListStaleLock.String())") {
		t.Errorf("the consolidation call must also be gated on listDedupPending (pending duplicate list partitions collide on idx_rgls_group_email)")
	}
	if !strings.Contains(between, "*errs = append(*errs") {
		t.Errorf("a not-ready / probe-error skip must still record an error so the migrate fails closed (v0.19.4)")
	}
	if !strings.Contains(migrate, `"v0.27.17 repoint repos.repo_group_id to canonical group per rg_name"`) {
		t.Errorf("the v0.27.17 repoint label vanished from migrate.go — consolidateRepoGroups must keep the block verbatim")
	}
}

// Historical DROP INDEX steps run on every migrate (v0.25.6 lesson), so
// a reused name would be rebuilt each run.
func TestRepoGroupFKIndexNamesNotDropTargets(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	dropStmt := regexp.MustCompile(`(?i)DROP INDEX[^;` + "`" + `"]*`)
	for _, stmt := range dropStmt.FindAllString(src, -1) {
		for _, idx := range repoGroupFKIndexes {
			if strings.Contains(stmt, idx.indexName) {
				t.Errorf("%s appears in a DROP INDEX statement (%q) — use a different name", idx.indexName, stmt)
			}
		}
	}
}

// The class-kill: every schema.sql column that REFERENCES repo_groups
// must be indexed — either by this file's list or by an existing index
// that leads with the column. A new repo_groups child FK cannot ship
// unindexed again (the v0.25.34 / v0.28.15 recurrence).
func TestEveryRepoGroupsFKChildIsIndexed(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	migrate := readSourceFile(t, "migrate.go")

	tableRe := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS\s+aveloxis_data\.(\w+)\s*\((.*?)\n\);`)
	fkRe := regexp.MustCompile(`(?m)^\s*(\w+)\s+\w+[^\n]*REFERENCES\s+aveloxis_data\.repo_groups\s*\(`)
	listed := map[string]string{}
	for _, idx := range repoGroupFKIndexes {
		listed[idx.table+"."+idx.column] = idx.indexName
	}
	found := 0
	for _, tm := range tableRe.FindAllStringSubmatch(schema, -1) {
		table, body := tm[1], tm[2]
		for _, cm := range fkRe.FindAllStringSubmatch(body, -1) {
			col := cm[1]
			found++
			if _, ok := listed[table+"."+col]; ok {
				continue
			}
			covering, ok := repoGroupFKCoveredElsewhere[table]
			if !ok {
				t.Errorf("%s.%s REFERENCES repo_groups but is not in repoGroupFKIndexes (or repoGroupFKCoveredElsewhere) — deleting a group would seq-scan %s per row", table, col, table)
				continue
			}
			if covering.column != col {
				t.Errorf("%s: repoGroupFKCoveredElsewhere names column %s but the FK column is %s", table, covering.column, col)
			}
			lead := regexp.MustCompile(`(?s)` + regexp.QuoteMeta(covering.indexName) + `\s+ON\s+aveloxis_data\.` + table + `\s*\(\s*` + col + `\s*[,)]`)
			if !lead.MatchString(schema) && !lead.MatchString(migrate) {
				t.Errorf("%s.%s claims coverage by %s, but no CREATE INDEX with %s in LEADING position was found", table, col, covering.indexName, col)
			}
		}
	}
	if found < 5 {
		t.Fatalf("schema scan found only %d repo_groups FK columns — the regex broke (expected ≥ 5)", found)
	}
}

// End-to-end (AVELOXIS_TEST_DB): after Migrate, every listed index
// exists on its table and is valid.
func TestRepoGroupFKIndexesEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	for _, idx := range repoGroupFKIndexes {
		var valid bool
		err := store.pool.QueryRow(ctx, `
			SELECT x.indisvalid
			FROM pg_index x
			JOIN pg_class i ON i.oid = x.indexrelid
			JOIN pg_class c ON c.oid = x.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'aveloxis_data' AND c.relname = $1 AND i.relname = $2`,
			idx.table, idx.indexName).Scan(&valid)
		if err != nil {
			t.Errorf("%s on %s: not found after Migrate (%v)", idx.indexName, idx.table, err)
			continue
		}
		if !valid {
			t.Errorf("%s on %s exists but is INVALID", idx.indexName, idx.table)
		}
	}
	ready, err := repoGroupFKIndexesReady(ctx, store)
	if err != nil {
		t.Fatalf("repoGroupFKIndexesReady: %v", err)
	}
	if !ready {
		t.Error("repoGroupFKIndexesReady must report true after Migrate built every index")
	}
}

// The readiness probe must cover the covered-elsewhere children too
// (Copilot round 2 on PR #191): a probe that only checks this file's
// own list would run the consolidation against an unindexed
// repo_groups_list_serve if idx_rgls_group_email failed to build.
func TestReadinessProbeCoversEveryRepoGroupsFKChild(t *testing.T) {
	body := srctest.FuncBody(t, readSourceFile(t, "repo_group_fk_indexes.go"), "func repoGroupFKIndexesReady(")
	if !strings.Contains(body, "repoGroupFKCoveredElsewhere") {
		t.Error("repoGroupFKIndexesReady must include repoGroupFKCoveredElsewhere in its validity probe")
	}
	if !strings.Contains(body, "valid == pairs") {
		t.Error("readiness must compare against the FULL probed set (own list + covered), not len(repoGroupFKIndexes)")
	}
	// v0.28.18: by LEADING COLUMN, never by index name — a UNIQUE that
	// cannot build on a duplicate-bearing fleet must not keep the gate
	// shut when the column is indexed under another name, and the probe
	// must not be satisfiable by an index whose column is not leading.
	if !strings.Contains(body, "x.indkey[0]") || !strings.Contains(body, "a.attname = want.col") {
		t.Error("readiness must probe pg_index for a VALID index whose LEADING column (indkey[0]) is the FK column")
	}
	if strings.Contains(body, "i.relname = want.idx") {
		t.Error("readiness must not match by index NAME (the pre-v0.28.18 shape)")
	}
}
