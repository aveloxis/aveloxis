// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestO11IndexesServeTheSlowShapes — v0.29.71 (O11, operator go-ahead
// 2026-09-30, measured on kate: summary/43 §2 item 9). The top-contributors
// messages arm read every message row of a repository to keep the window's
// (pytorch: 1.24M rows for 347k), the reviews arm likewise, and the weekly
// merged/closed series read every PR/issue row. migrate builds four indexes
// CONCURRENTLY (SR-2), and each query shape can use its index (checked with
// sequential scans off: a small test table would otherwise be seq-scanned).
func TestO11IndexesServeTheSlowShapes(t *testing.T) {
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
	testMigrate(ctx, t, store)

	want := map[string][]string{
		"idx_messages_repo_ts_cntrb":          {"ON aveloxis_data.messages", "(repo_id, msg_timestamp) INCLUDE (cntrb_id)"},
		"idx_pr_reviews_repo_submitted_cntrb": {"ON aveloxis_data.pull_request_reviews", "(repo_id, submitted_at) INCLUDE (cntrb_id)"},
		"idx_pull_requests_repo_merged":       {"ON aveloxis_data.pull_requests", "(repo_id, merged_at)", "WHERE (merged_at IS NOT NULL)"},
		"idx_issues_repo_closed":              {"ON aveloxis_data.issues", "(repo_id, closed_at)", "WHERE (closed_at IS NOT NULL)"},
	}
	for name, parts := range want {
		var def string
		var valid bool
		err := store.pool.QueryRow(ctx, `
			SELECT pg_get_indexdef(i.indexrelid), i.indisvalid
			FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'aveloxis_data' AND c.relname = $1`, name).Scan(&def, &valid)
		if err != nil {
			t.Errorf("%s: not built by migrate (%v)", name, err)
			continue
		}
		if !valid {
			t.Errorf("%s: built INVALID", name)
		}
		for _, p := range parts {
			if !strings.Contains(def, p) {
				t.Errorf("%s = %q, want it to contain %q", name, def, p)
			}
		}
	}

	for index, q := range map[string]string{
		"idx_messages_repo_ts_cntrb": `SELECT m.cntrb_id, COUNT(*) FROM aveloxis_data.messages m
			WHERE m.repo_id = 1 AND m.cntrb_id IS NOT NULL AND m.msg_timestamp >= now() - interval '2 years' AND m.msg_timestamp < now() GROUP BY 1`,
		"idx_pr_reviews_repo_submitted_cntrb": `SELECT prr.cntrb_id, COUNT(*) FROM aveloxis_data.pull_request_reviews prr
			WHERE prr.repo_id = 1 AND prr.cntrb_id IS NOT NULL AND prr.submitted_at >= now() - interval '2 years' AND prr.submitted_at < now() GROUP BY 1`,
		"idx_pull_requests_repo_merged": `SELECT date_trunc('week', merged_at AT TIME ZONE 'UTC'), COUNT(*) FROM aveloxis_data.pull_requests
			WHERE repo_id = 1 AND merged_at >= now() - interval '1 year' AND merged_at < now() AND merged_at IS NOT NULL GROUP BY 1`,
		"idx_issues_repo_closed": `SELECT date_trunc('week', closed_at AT TIME ZONE 'UTC'), COUNT(*) FROM aveloxis_data.issues
			WHERE repo_id = 1 AND closed_at >= now() - interval '1 year' AND closed_at < now() GROUP BY 1`,
	} {
		plan := explainWithOnlyIndex(ctx, t, store, index, q)
		if !strings.Contains(plan, index) {
			t.Errorf("the query shape cannot use %s:\n%s", index, plan)
		}
	}
}

// explainWithOnlyIndex plans q in a transaction that turns sequential scans
// off and drops every other index on the index's table (DDL is
// transactional; the transaction rolls back), so the plan shows whether the
// index SERVES the shape — on a near-empty test table the planner would
// otherwise pick an older (repo_id) index at equal cost. Constraint-backed
// indexes go too, through their constraint with CASCADE (child foreign
// keys depend on them): leaving the (repo_id, platform_issue_id) unique in
// place made the pin flaky, won or lost on the statistics other tests left.
func explainWithOnlyIndex(ctx context.Context, t *testing.T, store *PostgresStore, index, q string) string {
	t.Helper()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SET LOCAL enable_seqscan = off`); err != nil {
		t.Fatal(err)
	}
	// The drops take ACCESS EXCLUSIVE on the table and its FK children: fail
	// loudly rather than wait behind another test's transaction.
	if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '30s'`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, `
		SELECT ic.relname, COALESCE((SELECT c.conname FROM pg_constraint c WHERE c.conindid = i.indexrelid AND c.conrelid = i.indrelid), '')
		FROM pg_index i
		JOIN pg_class ic ON ic.oid = i.indexrelid
		JOIN pg_class tc ON tc.oid = i.indrelid
		JOIN pg_namespace n ON n.oid = tc.relnamespace
		WHERE n.nspname = 'aveloxis_data'
		  AND tc.oid = (SELECT indrelid FROM pg_index WHERE indexrelid = ('aveloxis_data.' || $1)::regclass)
		  AND ic.relname <> $1`, index)
	if err != nil {
		t.Fatal(err)
	}
	type other struct{ index, constraint string }
	var others []other
	for rows.Next() {
		var o other
		if err := rows.Scan(&o.index, &o.constraint); err != nil {
			t.Fatal(err)
		}
		others = append(others, o)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	table := ""
	if err := tx.QueryRow(ctx, `SELECT indrelid::regclass::text FROM pg_index WHERE indexrelid = ('aveloxis_data.' || $1)::regclass`, index).Scan(&table); err != nil {
		t.Fatal(err)
	}
	for _, o := range others {
		stmt := `DROP INDEX aveloxis_data.` + o.index
		if o.constraint != "" {
			stmt = `ALTER TABLE ` + table + ` DROP CONSTRAINT ` + o.constraint + ` CASCADE`
		}
		if _, err := tx.Exec(ctx, stmt); err != nil {
			t.Fatalf("%s inside the rolled-back transaction: %v", stmt, err)
		}
	}
	prow, err := tx.Query(ctx, "EXPLAIN "+q)
	if err != nil {
		t.Fatalf("%s: %v", index, err)
	}
	defer prow.Close()
	var plan strings.Builder
	for prow.Next() {
		var line string
		if err := prow.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line + "\n")
	}
	return plan.String()
}
