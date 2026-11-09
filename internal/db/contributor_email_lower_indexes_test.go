// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// contributor_email_lower_indexes_test.go — 0.29.141: expression indexes on
// lower(<email column>) for the three contributor tables, requested by
// aveloxis-cve. Its author resolution matches a commit's e-mail
// case-insensitively, with exactly these predicates:
//
//	lower(ct.cntrb_email) = $email   -- aveloxis_data.contributors
//	lower(a.alias_email)  = $email   -- aveloxis_data.contributors_aliases
//	lower(i.email)        = $email   -- aveloxis_data.contributor_identities
//
// and nothing indexed those expressions (idx_contributors_email_lookup is on
// the plain column; alias_email's UNIQUE is too; contributor_identities.email
// had no index), so each call, one per 5,000-commit sync batch, read all three
// tables in full. aveloxis-cve may not create objects in Aveloxis's schemas,
// so the indexes are Aveloxis's. The planner matches an expression index only
// when the expression is IDENTICAL: no trim(), no COLLATE, no partial WHERE.

import (
	"context"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// emailLowerIndexes is the local fixture (not shared with production code,
// the anti-rename-drift pattern): a refactor that renames or reshapes a
// production index fails here by name.
var emailLowerIndexes = []struct {
	name, table, column string
}{
	{"idx_contributors_email_lower", "contributors", "cntrb_email"},
	{"idx_contributors_aliases_email_lower", "contributors_aliases", "alias_email"},
	{"idx_contributor_identities_email_lower", "contributor_identities", "email"},
}

// The wiring: each index is migration-owned, built in stage 10 (the stage
// that holds the sibling email-lookup indexes and most steps since v0.27),
// CONCURRENTLY through execCreateIndexConcurrently (SR-2: contributors is fleet-scale; the helper
// drops an INVALID leftover of an interrupted build first), with exactly the
// requested statement, and is never declared in schema.sql.
func TestContributorEmailLowerIndexesAreMigrationBuilt(t *testing.T) {
	schema := srctest.StripSQLComments(srctest.Read(t, "internal/db/schema.sql"))
	migrate := srctest.StripGoComments(srctest.PackageFunc(t, "internal/db", "func migrateStage10RecentReleases("))
	for _, idx := range emailLowerIndexes {
		want := "CREATE INDEX CONCURRENTLY IF NOT EXISTS " + idx.name + " ON aveloxis_data." + idx.table + " (lower(" + idx.column + "))"
		if !srctest.ContainsNormalized(migrate, want) {
			t.Errorf("migrate.go must build %q", want)
		}
		if !srctest.ContainsNormalized(migrate, `execCreateIndexConcurrently(ctx, pg, logger, errs, "aveloxis_data", "`+idx.name+`",`) {
			t.Errorf("%s must go through execCreateIndexConcurrently", idx.name)
		}
		if strings.Contains(schema, idx.name) {
			t.Errorf("%s must be migration-owned, not declared in schema.sql (SR-2)", idx.name)
		}
		// SR-4: the historical DROP steps run on every migrate, so a name
		// they drop would be dropped and rebuilt at every run.
		if pkg := srctest.StripGoComments(srctest.PackageSource(t, "internal/db", 20)); strings.Contains(strings.ToUpper(pkg), "DROP INDEX IF EXISTS AVELOXIS_DATA."+strings.ToUpper(idx.name)) ||
			strings.Contains(strings.ToUpper(pkg), "DROP INDEX CONCURRENTLY IF EXISTS AVELOXIS_DATA."+strings.ToUpper(idx.name)) {
			t.Errorf("%s is a DROP INDEX target: the drop steps run on every migrate (SR-4)", idx.name)
		}
	}
}

// The behavior, against a migrated database: each index exists, is valid,
// is stored as exactly btree (lower(<column>)) on its table (no WHERE, no
// COLLATE, nothing around the column), and the planner can use it for both
// forms of the comparison: against a parameter (`lower(<column>) = $1`, a
// generic plan) and against a join variable (`lower(t.<column>) = k.email`,
// k an unnested array of addresses: aveloxis-cve's form). The second is
// written as a LATERAL subquery behind OFFSET 0, so the table is probed once
// per address whatever the table's size; which join a production plan picks
// depends on its statistics and is not this test's claim. enable_seqscan is
// off, so the test proves the expression MATCHES, which an index on a
// different expression (trim(), a COLLATE, a partial WHERE) cannot do:
// without a matching index the planner picks a scan with a Filter, which does
// not name the index. Index-only scans are off too: PostgreSQL cannot answer
// from an expression index alone, so on a small all-visible contributors
// table a full index-only scan of idx_contributors_email_lookup with a
// Filter could cost less than the matching probe (the closing suite saw it
// under one shuffle order) — table state, not the claim under test.
func TestContributorEmailLowerIndexesServeTheLowerPredicate(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	conn, err := store.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(conn.Release)
	for _, s := range []string{`SET enable_seqscan = off`, `SET enable_indexonlyscan = off`, `SET plan_cache_mode = force_generic_plan`} {
		if _, err := conn.Exec(ctx, s); err != nil {
			t.Fatalf("%s: %v", s, err)
		}
	}
	t.Cleanup(func() {
		for _, s := range []string{`RESET enable_seqscan`, `RESET enable_indexonlyscan`, `RESET plan_cache_mode`} {
			_, _ = conn.Exec(context.Background(), s)
		}
	})
	for _, idx := range emailLowerIndexes {
		t.Run(idx.name, func(t *testing.T) {
			var valid bool
			var def string
			err := conn.QueryRow(ctx, `
				SELECT x.indisvalid, pg_get_indexdef(x.indexrelid)
				  FROM pg_index x
				  JOIN pg_class i ON i.oid = x.indexrelid
				  JOIN pg_class c ON c.oid = x.indrelid
				  JOIN pg_namespace n ON n.oid = c.relnamespace
				 WHERE n.nspname = 'aveloxis_data' AND c.relname = $1 AND i.relname = $2`, idx.table, idx.name).Scan(&valid, &def)
			if err != nil {
				t.Fatalf("%s missing after Migrate: %v", idx.name, err)
			}
			if !valid {
				t.Errorf("%s exists but is INVALID", idx.name)
			}
			want := "CREATE INDEX " + idx.name + " ON aveloxis_data." + idx.table + " USING btree (lower(" + idx.column + "))"
			if def != want {
				t.Errorf("stored definition must be exactly %q (non-unique, no WHERE, no COLLATE); got %q", want, def)
			}
			explain := func(stmt, prepare, execute string) string {
				t.Helper()
				if _, err := conn.Exec(ctx, prepare); err != nil {
					t.Fatal(err)
				}
				defer func() { _, _ = conn.Exec(context.Background(), `DEALLOCATE `+stmt) }()
				rows, err := conn.Query(ctx, `EXPLAIN (COSTS OFF) EXECUTE `+execute)
				if err != nil {
					t.Fatal(err)
				}
				defer rows.Close()
				var b strings.Builder
				for rows.Next() {
					var line string
					if err := rows.Scan(&line); err != nil {
						t.Fatal(err)
					}
					b.WriteString(line)
					b.WriteString("\n")
				}
				if err := rows.Err(); err != nil {
					t.Fatal(err)
				}
				return b.String()
			}
			scalar := "avemail_" + idx.table
			plan := explain(scalar,
				`PREPARE `+scalar+`(text) AS SELECT 1 FROM aveloxis_data.`+idx.table+` t WHERE lower(t.`+idx.column+`) = $1`,
				scalar+`('dev@example.org')`)
			if !strings.Contains(plan, "$1") {
				t.Fatalf("expected a generic plan ($1 in the condition); got:\n%s", plan)
			}
			if !strings.Contains(plan, idx.name) || !strings.Contains(plan, "Index Cond: (lower(") {
				t.Errorf("lower(%s) = $1 must be an index probe on %s; plan:\n%s", idx.column, idx.name, plan)
			}
			join := "avemail_join_" + idx.table
			plan = explain(join,
				`PREPARE `+join+`(text[]) AS SELECT 1 FROM unnest($1::text[]) AS k(email)
				 CROSS JOIN LATERAL (SELECT 1 FROM aveloxis_data.`+idx.table+` t WHERE lower(t.`+idx.column+`) = k.email OFFSET 0) m
				 WHERE k.email <> ''`,
				join+`(ARRAY['dev@example.org', 'ops@example.org'])`)
			wantCond := "Index Cond: (lower(" + idx.column + ") = k.email)"
			if !strings.Contains(plan, idx.name) || !strings.Contains(plan, wantCond) {
				t.Errorf("lower(t.%s) = k.email must probe %s per address (%q); plan:\n%s", idx.column, idx.name, wantCond, plan)
			}
		})
	}
}
