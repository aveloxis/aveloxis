// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/model"
)

// TestTrafficTablesSurviveRepeatedMigrations builds a database from
// scratch, migrates it twice (every `aveloxis migrate` re-runs the base
// schema, so CREATE TABLE IF NOT EXISTS must stay a no-op) and checks the
// five GitHub traffic tables against Postgres's own catalog: the unique key
// each has, the traffic_kind CHECK, and the repo_id foreign key and index
// under the names fkExtraConstraints and fkExtraIndexes use, which only the
// catalog can confirm.
func TestTrafficTablesSurviveRepeatedMigrations(t *testing.T) {
	base := os.Getenv("AVELOXIS_TEST_DB")
	if base == "" {
		t.Skip("AVELOXIS_TEST_DB not set — skipping integration test")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, emptyTestDatabase(t, ctx, base), logger)
	if err != nil {
		t.Fatalf("NewPostgresStore: %v", err)
	}
	t.Cleanup(store.Close)
	for run := 1; run <= 2; run++ {
		if err := RunMigrations(ctx, store, logger); err != nil {
			t.Fatalf("RunMigrations run %d: %v", run, err)
		}
	}

	wantUnique := map[string][]string{
		"repo_views":             {"repo_id", "view_timestamp"},
		"repo_traffic_paths":     {"repo_id", "snapshot_date", "path"},
		"repo_traffic_referrers": {"repo_id", "snapshot_date", "referrer"},
		"repo_traffic_windows":   {"repo_id", "traffic_kind", "window_start", "window_end"},
		"repo_traffic_weeks":     {"repo_id", "traffic_kind", "week_start"},
	}
	for table, want := range wantUnique {
		t.Run(table, func(t *testing.T) {
			var exists bool
			if err := store.pool.QueryRow(ctx,
				`SELECT to_regclass('aveloxis_data.' || $1) IS NOT NULL`, table).Scan(&exists); err != nil {
				t.Fatal(err)
			}
			if !exists {
				t.Fatalf("aveloxis_data.%s does not exist after two migrations", table)
			}

			keys := uniqueConstraintColumns(ctx, t, store, table)
			if len(keys) != 1 || !slices.Equal(keys[0], want) {
				t.Errorf("aveloxis_data.%s unique constraints = %v, want exactly one on %v", table, keys, want)
			}

			fkName := ""
			for _, fk := range fkExtraConstraints {
				if fk.table == table && fk.column == "repo_id" {
					fkName = fk.constraintName
				}
			}
			matches, fkExists, err := constraintStateMatches(ctx, store, fkName, "CASCADE", "RESTRICT", true)
			if err != nil {
				t.Fatal(err)
			}
			if !fkExists || !matches {
				t.Errorf("fkExtraConstraints names %q for %s.repo_id: exists=%v, ON UPDATE CASCADE ON DELETE RESTRICT DEFERRABLE=%v; "+
					"the name must be the one Postgres gives the inline REFERENCES", fkName, table, fkExists, matches)
			}

			indexName := ""
			for _, idx := range fkExtraIndexes {
				if idx.table == table && idx.column == "repo_id" {
					indexName = idx.indexName
				}
			}
			var indexed bool
			if err := store.pool.QueryRow(ctx,
				`SELECT $1 <> '' AND to_regclass('aveloxis_data.' || $1) IS NOT NULL`, indexName).Scan(&indexed); err != nil {
				t.Fatal(err)
			}
			if !indexed {
				t.Errorf("fkExtraIndexes names %q for %s.repo_id, which does not exist after migrating", indexName, table)
			}
		})
	}

	for _, table := range []string{"repo_traffic_windows", "repo_traffic_weeks"} {
		t.Run(table+" traffic_kind CHECK", func(t *testing.T) {
			var defs []string
			rows, err := store.pool.Query(ctx, `
				SELECT pg_get_constraintdef(oid) FROM pg_constraint
				WHERE conrelid = ('aveloxis_data.' || $1)::regclass AND contype = 'c'`, table)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			for rows.Next() {
				var def string
				if err := rows.Scan(&def); err != nil {
					t.Fatal(err)
				}
				defs = append(defs, def)
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			if len(defs) != 1 || !strings.Contains(defs[0], "traffic_kind") {
				t.Fatalf("aveloxis_data.%s CHECK constraints = %v, want one on traffic_kind", table, defs)
			}

			// The CHECK behaves as declared: both model kinds are accepted, any
			// other value is refused at INSERT time. The repo_id FK is deferred
			// to COMMIT, so a rolled-back insert needs no repository row.
			for kind, wantCode := range map[string]string{
				string(model.TrafficKindViews):  "",
				string(model.TrafficKindClones): "",
				"stars":                         "23514",
			} {
				if got := insertTrafficKind(ctx, t, store, table, kind); got != wantCode {
					t.Errorf("INSERT traffic_kind %q into %s: SQLSTATE %q, want %q", kind, table, got, wantCode)
				}
			}
		})
	}
}

// uniqueConstraintColumns lists each UNIQUE constraint of an aveloxis_data
// table as its columns in key order.
func uniqueConstraintColumns(ctx context.Context, t *testing.T, store *PostgresStore, table string) [][]string {
	t.Helper()
	rows, err := store.pool.Query(ctx, `
		SELECT array_agg(att.attname::text ORDER BY k.ord)
		FROM pg_constraint con
		CROSS JOIN LATERAL unnest(con.conkey) WITH ORDINALITY AS k(attnum, ord)
		JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = k.attnum
		WHERE con.conrelid = ('aveloxis_data.' || $1)::regclass AND con.contype = 'u'
		GROUP BY con.oid
		ORDER BY con.oid`, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var keys [][]string
	for rows.Next() {
		var cols []string
		if err := rows.Scan(&cols); err != nil {
			t.Fatal(err)
		}
		keys = append(keys, cols)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return keys
}

// insertTrafficKind inserts one row with the given traffic_kind into a
// window or week table inside a transaction it always rolls back, and
// returns the SQLSTATE of the INSERT's error ("" when it succeeded).
func insertTrafficKind(ctx context.Context, t *testing.T, store *PostgresStore, table, kind string) string {
	t.Helper()
	tx, err := store.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	sql := `INSERT INTO aveloxis_data.repo_traffic_weeks (repo_id, traffic_kind, week_start)
		VALUES (-1, $1, DATE '2026-10-12')`
	if table == "repo_traffic_windows" {
		sql = `INSERT INTO aveloxis_data.repo_traffic_windows (repo_id, traffic_kind, window_start, window_end)
			VALUES (-1, $1, DATE '2026-10-05', DATE '2026-10-18')`
	}
	_, err = tx.Exec(ctx, sql, kind)
	if err == nil {
		return ""
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		t.Fatalf("INSERT into %s: %v", table, err)
	}
	return pgErr.Code
}
