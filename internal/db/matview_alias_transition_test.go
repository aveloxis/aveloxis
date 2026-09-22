// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
)

// TestPlainMigrateRecreatesViewsWhateverKindTheAliasesAre — Copilot on
// PR #210. matviews.sql runs as ONE Exec, and PostgreSQL's typed DROP fails
// on the wrong relation kind even with IF EXISTS ("is not a materialized
// view", 42809), so a single mismatch rolls back every DROP/CREATE in the
// file and every view keeps its previous definition. The migrate's view
// block only WARNs, so the run still stamps the schema and those old
// definitions stay — which is what v0.29.57's deploy step, a plain
// `aveloxis migrate` for explorer_libyear_summary's NULLS LAST ordering,
// depends on.
//
// The two alias relations have changed kind across releases
// (explorer_libyear_all: matview → view in v0.25.5; augur_new_contributors:
// matview → dropped → view in v0.25.6), and an operator or an older fleet
// can hold either kind. Each shape below is a database the plain migrate
// must still re-create every view on.
func TestPlainMigrateRecreatesViewsWhateverKindTheAliasesAre(t *testing.T) {
	base := os.Getenv("AVELOXIS_TEST_DB")
	if base == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	for _, shape := range []struct {
		name  string
		setup []string // run after a fresh migrate, before the plain migrate
	}{
		{"aliases as this binary leaves them", nil},
		{"aliases as independent materialized views (pre-v0.25.5 / v0.25.6)", []string{
			`CREATE MATERIALIZED VIEW aveloxis_data.explorer_libyear_all AS SELECT 1::bigint AS repo_id`,
			`CREATE MATERIALIZED VIEW aveloxis_data.augur_new_contributors AS SELECT 1::bigint AS repo_id`,
		}},
		{"aliases as independent plain views", []string{
			`CREATE VIEW aveloxis_data.explorer_libyear_all AS SELECT 1::bigint AS repo_id`,
			`CREATE VIEW aveloxis_data.augur_new_contributors AS SELECT 1::bigint AS repo_id`,
		}},
	} {
		t.Run(shape.name, func(t *testing.T) {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(io.Discard, nil))
			dsn := emptyTestDatabase(t, ctx, base)
			store, err := NewPostgresStore(ctx, dsn, logger)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(store.Close)
			// This test is ABOUT the views, so it asks for them: since
			// v0.29.57 they are optional and a store nobody configured
			// builds none.
			store.SetMatviewMode(MatviewsRebuild)
			if err := RunMigrations(ctx, store, logger); err != nil {
				t.Fatalf("first migrate: %v", err)
			}
			// A pre-v0.29.57 fleet: the summary carries the OLD ordering, and
			// the aliases whatever kind that release left (dropping the
			// summary CASCADEs to the dependent alias, so each shape below
			// re-creates the aliases itself).
			pre := append([]string{
				`DROP MATERIALIZED VIEW aveloxis_data.explorer_libyear_summary CASCADE`,
				`CREATE MATERIALIZED VIEW aveloxis_data.explorer_libyear_summary AS SELECT 1::bigint AS repo_id, NULL::numeric AS avg_libyear ORDER BY 2 DESC`,
				`DROP VIEW IF EXISTS aveloxis_data.augur_new_contributors CASCADE`,
			}, shape.setup...)
			for _, stmt := range pre {
				if _, err := store.pool.Exec(ctx, stmt); err != nil {
					t.Fatalf("%s: %v", stmt, err)
				}
			}
			var before string
			if err := store.pool.QueryRow(ctx, `SELECT pg_get_viewdef('aveloxis_data.explorer_libyear_summary')`).Scan(&before); err != nil {
				t.Fatal(err)
			}
			if strings.Contains(before, "NULLS LAST") {
				t.Fatal("the pre-release state still has the new ordering — this test would pass without re-creating anything")
			}
			if shape.setup == nil { // the dependent alias this binary creates
				if _, err := store.pool.Exec(ctx, `CREATE VIEW aveloxis_data.explorer_libyear_all AS SELECT * FROM aveloxis_data.explorer_libyear_summary`); err != nil {
					t.Fatal(err)
				}
				if _, err := store.pool.Exec(ctx, `CREATE VIEW aveloxis_data.augur_new_contributors AS SELECT * FROM aveloxis_data.explorer_contributor_actions`); err != nil {
					t.Fatal(err)
				}
			}

			// The deploy step: a plain `aveloxis migrate` on this database.
			store.SetMatviewMode(MatviewsRebuild)
			if err := RunMigrations(ctx, store, logger); err != nil {
				t.Fatalf("plain migrate: %v", err)
			}

			// Every view is back, and the new ordering is in the stored
			// definition (what the checklist's pg_get_viewdef step reads).
			for _, name := range matviewNames {
				var exists bool
				schema, view, _ := strings.Cut(name, ".")
				if err := store.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE schemaname = $1 AND matviewname = $2)`, schema, view).Scan(&exists); err != nil {
					t.Fatal(err)
				}
				if !exists {
					t.Errorf("%s is missing after the plain migrate — the view block failed and only WARNed", name)
				}
			}
			var defn string
			if err := store.pool.QueryRow(ctx, `SELECT pg_get_viewdef('aveloxis_data.explorer_libyear_summary')`).Scan(&defn); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(defn, "NULLS LAST") {
				t.Error("explorer_libyear_summary kept its old definition — v0.29.57's deploy step would print f")
			}
			// The aliases are plain views again, whatever they were.
			for _, alias := range MatviewAliasNames { // the one list (SR-17)
				var kind string
				if err := store.pool.QueryRow(ctx, `SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'aveloxis_data' AND c.relname = $1`, alias).Scan(&kind); err != nil {
					t.Fatal(fmt.Errorf("%s: %w", alias, err))
				}
				if kind != "v" {
					t.Errorf("%s is relkind %q after the migrate, want a plain view", alias, kind)
				}
			}
		})
	}
}
