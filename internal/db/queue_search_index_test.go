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
