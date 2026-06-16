// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the pg_trgm GIN index on aveloxis_data.repos
// (v0.18.30 monitor performance fix #3).
//
// At v0.18.29, ListQueuePage's search filter used `repo_owner ILIKE '%q%'
// OR repo_name ILIKE '%q%'`. The leading wildcard means no B-tree index
// can help; every keystroke against a 100K-repo fleet does a full
// sequential scan. The pg_trgm extension provides trigram indexing that
// supports `LIKE` and `ILIKE` with leading wildcards, turning the search
// from O(n) into O(log n + results).

package db

import (
	"os"
	"strings"
	"testing"
)

// TestSchemaCreatesTrigramExtension pins the migration-side contract:
// the migrate flow must run `CREATE EXTENSION IF NOT EXISTS pg_trgm`
// at startup. Without it, the GIN index can't be built.
func TestSchemaCreatesTrigramExtension(t *testing.T) {
	src := mustReadStoreSource(t, "migrate.go")
	if !strings.Contains(src, "CREATE EXTENSION IF NOT EXISTS pg_trgm") {
		t.Error("migrate.go must run `CREATE EXTENSION IF NOT EXISTS pg_trgm` so the " +
			"trigram GIN index on aveloxis_data.repos can be built. Without the " +
			"extension every search keystroke against a 100K-repo fleet runs a " +
			"full sequential scan.")
	}
}

// TestSchemaCreatesRepoNameTrigramIndex pins the index creation. We
// require the index on the concatenated owner/name expression so a
// search query like 'aveloxis/aveloxis' or 'awesome/lib' gets indexed
// hits across both columns in one lookup.
func TestSchemaCreatesRepoNameTrigramIndex(t *testing.T) {
	src := mustReadStoreSource(t, "migrate.go")
	hasIndex := strings.Contains(src, "idx_repos_owner_name_trgm") &&
		strings.Contains(src, "USING GIN") &&
		strings.Contains(src, "gin_trgm_ops")
	if !hasIndex {
		t.Error("migrate.go must create idx_repos_owner_name_trgm — a GIN index using gin_trgm_ops on " +
			"(repo_owner || '/' || repo_name) on aveloxis_data.repos. This is what makes the leading-" +
			"wildcard ILIKE search fast.")
	}
}

// TestTrigramIndexSchemaQualifiesOperatorClass pins the v0.25.30 fix:
// the GIN index must reference gin_trgm_ops SCHEMA-QUALIFIED with the
// schema discovered from the catalog, so it resolves regardless of the
// session search_path. The extension can be registered in pg_extension
// while its operator class lives in a schema not on the connecting
// role's search_path (observed on kate 2026-06-13 with SQLSTATE 42704);
// an unqualified reference fails there and used to fatally block serve
// startup over a perf index. A future refactor that reverts to an
// unqualified `gin_trgm_ops` would re-introduce that bug; this test
// fails the build first.
func TestTrigramIndexSchemaQualifiesOperatorClass(t *testing.T) {
	src := mustReadStoreSource(t, "migrate.go")

	if !strings.Contains(src, "func ginTrgmOpsSchema(") {
		t.Error("migrate.go must define ginTrgmOpsSchema — the catalog probe that " +
			"discovers which schema contains the gin_trgm_ops operator class.")
	}
	if !strings.Contains(src, "ginTrgmOpsSchema(ctx, pg)") {
		t.Error("the idx_repos_owner_name_trgm creation must call ginTrgmOpsSchema(ctx, pg) " +
			"so the operator class can be schema-qualified (and so an absent opclass " +
			"skips-with-warning instead of failing migration fatally).")
	}
	// The opclass reference in the index DDL must be schema-qualified via
	// the discovered schema, not a bare `gin_trgm_ops`.
	if !strings.Contains(src, "%s.gin_trgm_ops") {
		t.Error("the index DDL must schema-qualify the operator class with the " +
			"discovered schema (an fmt verb followed by .gin_trgm_ops), so resolution " +
			"does not depend on the session search_path.")
	}
	if !strings.Contains(src, "quoteIdent(schema)") {
		t.Error("the discovered schema must be passed through quoteIdent before " +
			"interpolation into the index DDL.")
	}
}

// TestListQueuePageSearchUsesIndexedExpression pins the query side:
// ListQueuePage's search must filter against the same `(repo_owner ||
// '/' || repo_name)` expression the index covers, with `ILIKE
// '%search%'`. The pre-fix `repo_owner ILIKE %s OR repo_name ILIKE %s`
// pattern can't use the GIN index even when it exists.
func TestListQueuePageSearchUsesIndexedExpression(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatalf("read queue.go: %v", err)
	}
	src := string(data)
	body := extractBatchFunc(src, "ListQueuePage")
	if body == "" {
		t.Fatal("could not locate ListQueuePage")
	}

	// The exact concatenation form must appear in the WHERE clause so
	// the planner uses the GIN index on the expression.
	if !strings.Contains(body, "(repo_owner || '/' || repo_name)") &&
		!strings.Contains(body, "(repo_owner||'/'||repo_name)") {
		t.Error("ListQueuePage search must filter on (repo_owner || '/' || repo_name) — the same " +
			"expression covered by idx_repos_owner_name_trgm. The pre-fix per-column ILIKE pattern " +
			"can't use the GIN index even when the index exists.")
	}
}
