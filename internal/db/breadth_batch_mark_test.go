// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.8 — batched mark-attempted + the cntrb_last_breadth_at index.
//
// The pre-v0.27.8 breadth worker issued ONE single-row UPDATE per
// contributor (18,000 UPDATEs per cycle at the operator's
// breadth_batch_size=18000), and GetContributorsForBreadth's
// `ORDER BY cntrb_last_breadth_at ASC NULLS FIRST LIMIT N` ran a full
// sort over 2.3M contributor rows every cycle because the column had
// no index. v0.27.8 adds MarkBreadthAttemptedBatch (ANY($1::uuid[])
// in chunks) and idx_contributors_last_breadth (declared ASC NULLS
// FIRST so a forward index scan matches the claim query's ORDER BY
// exactly — Postgres's ASC default is NULLS LAST, which would NOT
// serve this ORDER BY).

package db

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func readFileT(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// --- source-contract tier -------------------------------------------------

func TestMarkBreadthAttemptedBatchExists(t *testing.T) {
	src := readFileT(t, "breadth_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) MarkBreadthAttemptedBatch(") {
		t.Fatal("breadth_store.go must define MarkBreadthAttemptedBatch — the v0.27.8 " +
			"batched replacement for per-contributor single-row UPDATEs (18,000/cycle " +
			"at fleet batch sizes)")
	}
	if !strings.Contains(src, "ANY($1::uuid[])") {
		t.Error("MarkBreadthAttemptedBatch must stamp via " +
			"`WHERE cntrb_id = ANY($1::uuid[])` — one UPDATE per chunk, not one per row")
	}
	// The single-row method stays for compatibility (interface stability;
	// callers outside the breadth worker's hot path).
	if !strings.Contains(src, "func (s *PostgresStore) MarkBreadthAttempted(") {
		t.Error("the single-row MarkBreadthAttempted must be kept for compatibility — " +
			"v0.27.8 adds the batch method alongside it, it does not replace it")
	}
}

func TestMarkBreadthAttemptedBatchChunks(t *testing.T) {
	src := readFileT(t, "breadth_store.go")
	if !strings.Contains(src, "breadthMarkChunkSize") {
		t.Error("MarkBreadthAttemptedBatch must chunk its input (breadthMarkChunkSize " +
			"named constant) so a fleet-sized cycle (18K+ IDs) doesn't ship one " +
			"giant array parameter in a single statement")
	}
}

func TestSchemaDeclaresBreadthIndex(t *testing.T) {
	schema := readFileT(t, "schema.sql")
	idx := strings.Index(schema, "idx_contributors_last_breadth")
	if idx < 0 {
		t.Fatal("schema.sql must declare idx_contributors_last_breadth ON " +
			"aveloxis_data.contributors (cntrb_last_breadth_at ...) — without it " +
			"GetContributorsForBreadth full-sorts 2.3M rows every cycle")
	}
	window := schema[idx:min(idx+300, len(schema))]
	if !strings.Contains(window, "cntrb_last_breadth_at") {
		t.Error("idx_contributors_last_breadth must be on cntrb_last_breadth_at")
	}
	if !strings.Contains(window, "NULLS FIRST") {
		t.Error("idx_contributors_last_breadth must be declared ASC NULLS FIRST: the claim " +
			"query orders `cntrb_last_breadth_at ASC NULLS FIRST` and Postgres's ASC " +
			"default is NULLS LAST — a default-order index cannot serve the ORDER BY " +
			"(a backward scan gives DESC NULLS FIRST, still no match)")
	}
	// schema.sql gets the plain (non-CONCURRENTLY) form per house convention:
	// CONCURRENTLY cannot run inside the schema-exec transaction; fresh installs
	// build it instantly on the empty table, existing fleets get the
	// CONCURRENTLY build from migrate.go.
	if strings.Contains(window, "CONCURRENTLY") {
		t.Error("schema.sql must declare the index WITHOUT CONCURRENTLY (house " +
			"convention: plain form in schema.sql for fresh installs, CONCURRENTLY " +
			"in migrate.go for existing fleets — see idx_commits_cmt_ght_author_id)")
	}
}

func TestMigrationBuildsBreadthIndexConcurrently(t *testing.T) {
	src := readFileT(t, "migrate.go")
	idx := strings.Index(src, "idx_contributors_last_breadth")
	if idx < 0 {
		t.Fatal("migrate.go must build idx_contributors_last_breadth for existing " +
			"fleets — schema.sql's plain CREATE INDEX only covers fresh installs")
	}
	region := src[max(0, idx-600):min(idx+600, len(src))]
	if !strings.Contains(region, "execCreateIndexConcurrently") {
		t.Error("idx_contributors_last_breadth must be built via " +
			"execCreateIndexConcurrently — a blocking CREATE INDEX on the 2.3M-row " +
			"(fleet-scale) contributors table would stall every collection worker")
	}
	if !strings.Contains(region, "NULLS FIRST") {
		t.Error("the migrate.go CREATE INDEX CONCURRENTLY must carry ASC NULLS FIRST — " +
			"same reasoning as the schema.sql declaration")
	}
}

// TestBreadthIndexMatchesClaimQueryOrder pins that the claim query's
// ORDER BY and the index declaration agree — the entire point of the
// index is serving that ORDER BY without a sort node. If either side
// drifts (query loses NULLS FIRST, index loses it, column renamed),
// this fails before the regression ships.
func TestBreadthIndexMatchesClaimQueryOrder(t *testing.T) {
	store := readFileT(t, "breadth_store.go")
	schema := readFileT(t, "schema.sql")

	const orderShape = "ORDER BY cntrb_last_breadth_at ASC NULLS FIRST"
	if !strings.Contains(store, orderShape) {
		t.Errorf("GetContributorsForBreadth must keep %q — the v0.27.8 index is "+
			"declared to serve exactly this shape", orderShape)
	}
	idx := strings.Index(schema, "idx_contributors_last_breadth")
	if idx < 0 {
		t.Fatal("index missing from schema.sql")
	}
	window := schema[idx:min(idx+300, len(schema))]
	if !strings.Contains(window, "ASC NULLS FIRST") {
		t.Errorf("index must be declared ASC NULLS FIRST to match the query's %q", orderShape)
	}
}

// --- integration tier (AVELOXIS_TEST_DB) ----------------------------------

// TestMarkBreadthAttemptedBatchStampsAll seeds contributors with NULL
// cntrb_last_breadth_at, batch-marks a subset, and verifies exactly
// that subset is stamped.
func TestMarkBreadthAttemptedBatchStampsAll(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()

	slug := fmt.Sprintf("avbreadthbatch_%d", time.Now().UnixNano())
	var ids []string
	for i := range 3 {
		var id string
		err := store.pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_login, gh_login)
			VALUES ($1, $1) RETURNING cntrb_id::text`,
			fmt.Sprintf("%s_%d", slug, i)).Scan(&id)
		if err != nil {
			t.Fatalf("seed contributor %d: %v", i, err)
		}
		ids = append(ids, id)
	}
	t.Cleanup(func() {
		_, _ = store.pool.Exec(ctx,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE $1`, slug+"%")
	})

	// Mark the first two; leave the third untouched.
	if err := store.MarkBreadthAttemptedBatch(ctx, ids[:2]); err != nil {
		t.Fatalf("MarkBreadthAttemptedBatch: %v", err)
	}

	var stamped, unstamped int
	if err := store.pool.QueryRow(ctx, `
		SELECT COUNT(*) FILTER (WHERE cntrb_last_breadth_at IS NOT NULL),
		       COUNT(*) FILTER (WHERE cntrb_last_breadth_at IS NULL)
		FROM aveloxis_data.contributors
		WHERE cntrb_login LIKE $1`, slug+"%").Scan(&stamped, &unstamped); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if stamped != 2 || unstamped != 1 {
		t.Errorf("stamped=%d unstamped=%d, want 2/1 — the batch must stamp exactly "+
			"the IDs it was given", stamped, unstamped)
	}

	// Empty input is a no-op, not an error.
	if err := store.MarkBreadthAttemptedBatch(ctx, nil); err != nil {
		t.Errorf("empty batch must be a no-op, got %v", err)
	}
}

// TestBreadthIndexExistsAfterMigrate runs the migration and verifies
// the index landed with the NULLS FIRST ordering in its definition.
func TestBreadthIndexExistsAfterMigrate(t *testing.T) {
	store, ctx := realignConnect(t)
	defer store.Close()
	store.SetMatviewSkip(true)
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	var indexdef string
	err := store.pool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE schemaname = 'aveloxis_data' AND indexname = 'idx_contributors_last_breadth'`).
		Scan(&indexdef)
	if err != nil {
		t.Fatalf("idx_contributors_last_breadth not found after migrate: %v", err)
	}
	if !strings.Contains(indexdef, "NULLS FIRST") {
		t.Errorf("live index definition must carry NULLS FIRST, got: %s", indexdef)
	}
}
