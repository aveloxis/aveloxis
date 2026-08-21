// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// contributor_email_lookup_indexes_test.go — TDD suite for the v0.27.54
// email/canonical lookup indexes on contributors.
//
// Motivating incident (2026-07-29, aveloxis_large): the hourly
// mailing-list sender-resolve candidates query
// (GetMailingListSenderResolveCandidates) ran 30-50 minutes occupying 5
// backends. EXPLAIN showed its NOT EXISTS against contributors planned as
// a nested-loop anti-join whose inner side was a full parallel seq scan
// of the 9.4 GB contributors heap ONCE PER OUTER ROW — the OR-equality
// (cntrb_email = X OR cntrb_canonical = X) cannot hash-anti-join and
// neither column had an index. History: cntrb_email never had one;
// cntrb_canonical's was dropped in v0.25.6 as "no reader" — correct that
// day, then v0.25.7+ shipped the mailing-list machinery with new readers
// of exactly these columns (this query, ResolveContributorIDByEmail's
// join, CreateEmailOnlyContributor's probe).

import (
	"regexp"
	"strings"
	"testing"
)

// emailLookupIndexes is the local fixture (deliberately NOT shared with
// production code, per the anti-rename-drift test pattern): a refactor
// that renames the production index still fails here by name.
var emailLookupIndexes = []struct {
	name   string
	column string
}{
	{"idx_contributors_email_lookup", "cntrb_email"},
	{"idx_contributors_canonical_lookup", "cntrb_canonical"},
}

// Both indexes must be declared in schema.sql for fresh installs, and
// must be NON-partial: the hot probes compare against a JOIN column
// (em.sender_email), and the planner cannot prove a partial predicate
// like "cntrb_email != """ from a join variable at plan time — a partial
// index would be silently ignored for exactly the anti-join this fix
// targets. (The v0.19.9 partial gh_login index is fine because those
// probes are plan-time-known parameters; this one is not.)
func TestSchemaDeclaresContributorEmailLookupIndexes(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	flat := strings.Join(strings.Fields(schema), " ")
	for _, idx := range emailLookupIndexes {
		decl := regexp.MustCompile(
			`CREATE INDEX IF NOT EXISTS ` + idx.name +
				` ON aveloxis_data\.contributors \(` + idx.column + `\)([^;]*);`)
		m := decl.FindStringSubmatch(flat)
		if m == nil {
			t.Errorf("schema.sql must declare CREATE INDEX IF NOT EXISTS %s ON aveloxis_data.contributors (%s)", idx.name, idx.column)
			continue
		}
		if strings.Contains(strings.ToUpper(m[1]), "WHERE") {
			t.Errorf("%s must be NON-partial: a WHERE predicate cannot be proven from the join-variable probes (em.sender_email) and the planner would ignore the index for the sender-resolve anti-join — the exact query this index exists to fix", idx.name)
		}
	}
}

// Existing fleets get the indexes via a CONCURRENTLY build in migrate.go
// (the schema.sql plain form only covers fresh installs — base DDL runs
// inside a transaction where CONCURRENTLY is not allowed).
func TestMigrationCreatesEmailLookupIndexesConcurrently(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	for _, idx := range emailLookupIndexes {
		pos := strings.Index(src, `"`+idx.name+`"`)
		if pos < 0 {
			t.Errorf("migrate.go must build %s (pass the name to execCreateIndexConcurrently so the INVALID-index self-heal can find it)", idx.name)
			continue
		}
		window := src[max(0, pos-300):pos]
		if !strings.Contains(window, "execCreateIndexConcurrently") {
			t.Errorf("%s must be created via execCreateIndexConcurrently (non-blocking on live fleets, self-healing on interrupted builds), not a plain CREATE INDEX", idx.name)
		}
	}
}

// The v0.25.6 index-drop steps still run on every migrate (idempotent
// history). The new indexes must not collide with ANY DROP INDEX target
// in migrate.go — a name collision would drop-and-rebuild a ~2.5M-row
// index on every single migrate run.
func TestEmailLookupIndexNamesNotDropTargets(t *testing.T) {
	src := readSourceFile(t, "migrate.go")
	dropStmt := regexp.MustCompile(`(?i)DROP INDEX[^;` + "`" + `"]*`)
	for _, stmt := range dropStmt.FindAllString(src, -1) {
		for _, idx := range emailLookupIndexes {
			if strings.Contains(stmt, idx.name) {
				t.Errorf("%s appears in a DROP INDEX statement (%q) — historical drop steps run on every migrate, so this would rebuild the index each run; use a different name", idx.name, stmt)
			}
		}
	}
}

// End-to-end (gated on AVELOXIS_TEST_DB): after Migrate, both indexes
// exist on contributors and are valid.
func TestContributorEmailLookupIndexesEndToEnd(t *testing.T) {
	store, ctx := v0251Connect(t)
	t.Cleanup(store.Close)
	for _, idx := range emailLookupIndexes {
		var valid bool
		err := store.pool.QueryRow(ctx, `
			SELECT x.indisvalid
			FROM pg_index x
			JOIN pg_class i ON i.oid = x.indexrelid
			JOIN pg_class c ON c.oid = x.indrelid
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = 'aveloxis_data' AND c.relname = 'contributors' AND i.relname = $1`,
			idx.name).Scan(&valid)
		if err != nil {
			t.Fatalf("%s missing after Migrate: %v", idx.name, err)
		}
		if !valid {
			t.Errorf("%s exists but is INVALID — interrupted CONCURRENTLY build not self-healed", idx.name)
		}
	}
}
