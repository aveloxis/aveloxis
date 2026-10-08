// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// SR-2, made bidirectional (0.29.77 round 1: the two new covering indexes
// were declared plainly in schema.sql AND built CONCURRENTLY in migrate —
// the base DDL runs first, so an upgraded fleet would have block-built
// them with a SHARE lock on issues and pull_requests, and the CONCURRENTLY
// steps were no-ops; the fixed-name pins could not see it). Every index
// migrate builds CONCURRENTLY is derived from migrate.go itself; one that
// schema.sql also declares is a violation unless it is a legacy shape every
// fleet already had before the rule (the allowlist below, which only
// shrinks). A fresh install gets the CONCURRENTLY build from migrate — an
// instant on an empty table — so schema.sql needs no plain form. Scope
// (stated, round 3): every `CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT
// EXISTS <name>` LITERAL in every non-test file of this package. The three
// helpers that build names through fmt.Sprintf from a table
// (fkExtraIndexes in fk_indexes_extra.go, repo_group_fk_indexes.go,
// email_message_fk_indexes.go) are outside this scan: their v0.20.x names
// are a legacy dual-path set of their own — repo_group_fk_indexes_test.go
// pins the repo-group names OUT of schema.sql, TestSchemaDeclaresExtraFKIndexes
// pins fkExtraIndexes INTO it (the dual path is that helper's pinned
// contract), and the email_message helper has no schema pin — so a new
// entry added to one of those tables AND to schema.sql would escape this
// pin (the 0.29.77 ledger records the decision still to take: keep or end
// the dual path for fkExtraIndexes, and a pin for the email_message helper).
var legacyIndexesInBothPlaces = map[string]bool{
	"idx_clh_cntrb": true, "idx_clh_login": true, "idx_commits_cmt_ght_author_id": true,
	"idx_contributors_activity_checked": true, "idx_contributors_canonical_lookup": true,
	"idx_contributors_email_lookup": true, "idx_contributors_gh_login_lower": true,
	"idx_contributors_history_backfilled": true, "idx_contributors_last_breadth": true,
	"idx_em_proj_pending_keyed": true, "idx_em_proj_pending_threaded": true,
	"idx_email_message_linked_issue": true, "idx_email_message_linked_pr": true,
	"idx_email_message_linked_review": true, "idx_email_message_thread_root": true,
	"idx_lockfile_packages_pkg": true, "idx_messages_node_id": true,
	"idx_pull_request_review_message_ref_msg_id": true, "idx_staging_repo_id": true,
}

func TestConcurrentlyBuiltIndexesAreNotDeclaredInSchemaSQL(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var sources strings.Builder
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		sources.WriteString(readFileForTest(t, f))
		sources.WriteString("\n")
	}
	concurrently := concurrentlyBuiltIndexes(sources.String())
	declared := map[string]bool{}
	for _, name := range schemaDeclaredIndexes(readFileForTest(t, "schema.sql")) {
		declared[name] = true
	}
	if len(concurrently) < 50 {
		t.Fatalf("expected the package's CONCURRENTLY builds to be scanned, found %d", len(concurrently))
	}
	seen := map[string]bool{}
	var both []string
	for _, name := range concurrently {
		if declared[name] && !seen[name] {
			seen[name] = true
			both = append(both, name)
		}
	}
	sort.Strings(both)
	for _, name := range both {
		if !legacyIndexesInBothPlaces[name] {
			t.Errorf("%s is built CONCURRENTLY by migrate AND declared in schema.sql: the base DDL runs first, so an existing fleet block-builds it and the CONCURRENTLY step is a no-op (SR-2). Remove the schema.sql declaration; the CONCURRENTLY build covers fresh installs too", name)
		}
	}
	// The allowlist only shrinks: a name no longer in both places leaves it.
	for name := range legacyIndexesInBothPlaces {
		if !seen[name] {
			t.Errorf("legacyIndexesInBothPlaces names %s, which is no longer declared in both places — remove it from the list", name)
		}
	}
}

// concurrentlyBuiltIndexes names every live `CREATE [UNIQUE] INDEX
// CONCURRENTLY IF NOT EXISTS` in Go source: Go comments are stripped, and
// then SQL comments (the statements sit in raw strings, where a `--` line
// is still Go text). PR #226 review 5458301284: the raw scan counted
// commented-out builds.
func concurrentlyBuiltIndexes(goSrc string) []string {
	live := srctest.StripSQLComments(srctest.StripGoComments(goSrc))
	var names []string
	for _, m := range regexp.MustCompile(`CREATE (?:UNIQUE )?INDEX CONCURRENTLY IF NOT EXISTS (\w+)`).FindAllStringSubmatch(live, -1) {
		names = append(names, m[1])
	}
	return names
}

// schemaDeclaredIndexes names every live plain `CREATE [UNIQUE] INDEX IF NOT
// EXISTS` in schema.sql (SQL comments stripped).
func schemaDeclaredIndexes(schema string) []string {
	var names []string
	for _, m := range regexp.MustCompile(`CREATE (?:UNIQUE )?INDEX IF NOT EXISTS (\w+)`).FindAllStringSubmatch(srctest.StripSQLComments(schema), -1) {
		names = append(names, m[1])
	}
	return names
}

func TestIndexScansIgnoreCommentedOutStatements(t *testing.T) {
	goSrc := "package db\n" +
		"// execCreateIndexConcurrently(ctx, pg, logger, errs, \"aveloxis_data\", \"idx_go_line\", `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_go_line ON t (a)`)\n" +
		"/* `CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_go_block ON t (a)` */\n" +
		"var a = `\n-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sql_line ON t (a)\nCREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_live ON t (a)`\n"
	if got := concurrentlyBuiltIndexes(goSrc); len(got) != 1 || got[0] != "idx_live" {
		t.Errorf("concurrentlyBuiltIndexes = %v, want only [idx_live]: commented-out builds are not builds", got)
	}
	schema := "-- CREATE INDEX IF NOT EXISTS idx_schema_commented ON t (a);\n/* CREATE INDEX IF NOT EXISTS idx_schema_block ON t (a); */\nCREATE INDEX IF NOT EXISTS idx_schema_live ON t (a);\n"
	if got := schemaDeclaredIndexes(schema); len(got) != 1 || got[0] != "idx_schema_live" {
		t.Errorf("schemaDeclaredIndexes = %v, want only [idx_schema_live]: a commented-out declaration is not a declaration", got)
	}
}
