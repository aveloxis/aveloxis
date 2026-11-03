// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// ---------------------------------------------------------------------------
// Statements: extraction + comment stripping + splitting + attribution
// ---------------------------------------------------------------------------

func TestStatementsSplitsAndAttributes(t *testing.T) {
	files := map[string]string{
		"b.go": "package db\nvar q = `INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1)`\n",
		"a.go": "package db\nfunc f() {\n\texec(`UPDATE aveloxis_data.repos SET repo_name = $1;\n\t\tDELETE FROM aveloxis_data.staging WHERE repo_id = $2`)\n}\n",
	}
	stmts := Statements(files)
	if len(stmts) != 3 {
		t.Fatalf("want 3 statements, got %d: %#v", len(stmts), stmts)
	}
	// Deterministic order: files sorted by name, statements in source order.
	if stmts[0].File != "a.go" || !strings.Contains(stmts[0].SQL, "UPDATE") {
		t.Errorf("stmt 0 should be a.go's UPDATE, got %s: %q", stmts[0].File, stmts[0].SQL)
	}
	if stmts[1].File != "a.go" || !strings.Contains(stmts[1].SQL, "DELETE") {
		t.Errorf("stmt 1 should be a.go's DELETE (split at the top-level semicolon), got %s: %q", stmts[1].File, stmts[1].SQL)
	}
	if stmts[2].File != "b.go" || !strings.Contains(stmts[2].SQL, "INSERT") {
		t.Errorf("stmt 2 should be b.go's INSERT, got %s: %q", stmts[2].File, stmts[2].SQL)
	}
}

func TestStatementsStripsSQLComments(t *testing.T) {
	// The v0.27.89 incident class: a `);` INSIDE a SQL comment must not
	// truncate or split the statement (StripSQLComments is literal-aware).
	files := map[string]string{
		"x.go": "package db\nvar q = `UPDATE aveloxis_data.repos\n" +
			"-- guard (the old shape was: WHERE x IN (...); now a comment with ); inside\n" +
			"SET repo_name = $1 WHERE repo_id = $2`\n",
	}
	stmts := Statements(files)
	if len(stmts) != 1 {
		t.Fatalf("comment containing ';' split the statement: got %d statements", len(stmts))
	}
	if strings.Contains(stmts[0].SQL, "guard (the old shape") {
		t.Error("SQL comments must be stripped from the statement text")
	}
	if !strings.Contains(stmts[0].SQL, "SET repo_name = $1") {
		t.Error("statement body lost during comment stripping")
	}
}

func TestStatementsIgnoresSemicolonsInsideQuotesParensAndDollarBlocks(t *testing.T) {
	files := map[string]string{
		"m.go": "package db\nvar a = `UPDATE aveloxis_data.repos SET repo_name = 'a;b' WHERE repo_id = $1`\n" +
			"var b = `DO $$\nBEGIN\n  PERFORM 1;\n  PERFORM 2;\nEND $$`\n",
	}
	stmts := Statements(files)
	if len(stmts) != 2 {
		t.Fatalf("semicolons inside a quoted literal or $$ block must not split: got %d statements: %#v", len(stmts), stmts)
	}
	if !strings.Contains(stmts[0].SQL, "'a;b'") {
		t.Error("quoted semicolon mangled")
	}
	if !strings.Contains(stmts[1].SQL, "PERFORM 2;") {
		t.Error("$$ block shredded — the DO-block body must stay one statement")
	}
}

func TestStatementsDropsEmptyPieces(t *testing.T) {
	files := map[string]string{
		"y.go": "package db\nvar q = `CREATE TABLE t (id INT);\n\n;`\n",
	}
	stmts := Statements(files)
	if len(stmts) != 1 {
		t.Fatalf("empty/whitespace-only pieces must be dropped: got %d", len(stmts))
	}
}

// ---------------------------------------------------------------------------
// FindWrites
// ---------------------------------------------------------------------------

func TestFindWritesFiltersByTable(t *testing.T) {
	stmts := []Stmt{
		{File: "a.go", SQL: "INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1)"},
		{File: "a.go", SQL: "UPDATE aveloxis_data.repos SET repo_name = $1"},
		{File: "b.go", SQL: "UPDATE aveloxis_data.repo_info SET commit_count = $1"},
		{File: "b.go", SQL: "SELECT repo_name FROM aveloxis_data.repos"},
		// prefix trap: repos vs repo_info — \b must not cross the underscore
		{File: "c.go", SQL: "UPDATE aveloxis_data.repos_extra SET x = 1"},
		// v0.29.94: the ONLY and MERGE spellings are writes too
		{File: "d.go", SQL: "UPDATE ONLY aveloxis_data.repos SET repo_name = $1"},
		{File: "d.go", SQL: "MERGE INTO aveloxis_data.repos r USING s ON r.repo_id = s.repo_id WHEN MATCHED THEN UPDATE SET repo_name = s.repo_name"},
	}
	got := FindWrites(stmts, "aveloxis_data.repos")
	if len(got) != 4 {
		t.Fatalf("want the 4 repos write statements (INSERT, UPDATE, UPDATE ONLY, MERGE INTO), got %d: %#v", len(got), got)
	}
	for _, s := range got {
		if strings.Contains(s.SQL, "SELECT") || strings.Contains(s.SQL, "repo_info") || strings.Contains(s.SQL, "repos_extra") {
			t.Errorf("FindWrites leaked a non-repos or read-only statement: %q", s.SQL)
		}
	}
}

// ---------------------------------------------------------------------------
// WritesColumn — the 8 fixtures moved wholesale from
// internal/db/column_writer_tripwire_test.go (v0.27.116 round-10 finding:
// predicate-only / RETURNING-only / RHS-only appearances must NOT count).
// ---------------------------------------------------------------------------

func TestWritesColumnFixtures(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		tbl  string
		col  string
		want bool
	}{
		{"set-lhs", `UPDATE aveloxis_data.repos SET repo_name = $1 WHERE repo_id = $2`, "aveloxis_data.repos", "repo_name", true},
		{"where-only", `UPDATE aveloxis_data.repos SET repo_name = $1 WHERE whitespace_head_hash = ''`, "aveloxis_data.repos", "whitespace_head_hash", false},
		{"insert-list", `INSERT INTO aveloxis_data.repos (repo_git, repo_name) VALUES ($1, $2) RETURNING repo_id`, "aveloxis_data.repos", "repo_git", true},
		{"returning-only", `INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1) RETURNING repo_id`, "aveloxis_data.repos", "repo_id", false},
		{"conflict-set", `INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1) ON CONFLICT (repo_git) DO UPDATE SET repo_name = EXCLUDED.repo_name`, "aveloxis_data.repos", "repo_name", true},
		{"rhs-only", `UPDATE aveloxis_data.repos SET repo_name = COALESCE(NULLIF(repo_git, ''), repo_name)`, "aveloxis_data.repos", "repo_git", false},
		{"multi-assign", `UPDATE aveloxis_data.repos SET repo_name = $1, repo_owner = $2 WHERE repo_id = $3`, "aveloxis_data.repos", "repo_owner", true},
		{"qualified-predicate", `UPDATE aveloxis_data.commits c SET cmt_added = v.added FROM (SELECT 1) v WHERE c.cmt_filename = v.filename`, "aveloxis_data.commits", "cmt_filename", false},
		// aveloxis_ops now expressible (the old helper hardcoded aveloxis_data)
		{"ops-schema", `UPDATE aveloxis_ops.collection_queue SET last_issues = $1`, "aveloxis_ops.collection_queue", "last_issues", true},
		// PostgreSQL's multiple-column assignment, SET (a, b) = (...)
		// (RecordCommitBounds, round 2 finding 3 of 0.29.123): every
		// listed column is written; one read only on its right side is not.
		{"column-list-first", `UPDATE aveloxis_data.repos r SET (first_commit_at, last_commit_at) = (SELECT 1, 2) WHERE r.repo_id = $1`, "aveloxis_data.repos", "first_commit_at", true},
		{"column-list-last", "UPDATE aveloxis_data.repos r SET\n (first_commit_at, last_commit_at, data_changed_at) = (\n SELECT b.f) WHERE r.repo_id = $1", "aveloxis_data.repos", "data_changed_at", true},
		{"column-list-after-a-set", `UPDATE aveloxis_data.repos SET repo_name = $1, (repo_owner, repo_git) = ($2, $3)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-rhs-only", `UPDATE aveloxis_data.repos r SET (repo_name, repo_owner) = (SELECT r.repo_git, COALESCE(last_commit_at, NOW()))`, "aveloxis_data.repos", "last_commit_at", false},
		// A list member as the UPDATE grammar spells it: column_name,
		// quoted or not, then any [subscript] or .field (round 5 of 0.29.123).
		{"column-list-quoted", `UPDATE aveloxis_data.repos SET ("repo_git", repo_name) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-quoted-last", `UPDATE aveloxis_data.repos SET (repo_name, "repo_git") = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-subfield", `UPDATE aveloxis_data.repos SET (repo_git.f, repo_name) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-subscript", `UPDATE aveloxis_data.repos SET (repo_name, repo_git[1]) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-other-subfield", `UPDATE aveloxis_data.repos SET (repo_name, other.repo_git) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		// Round 6: whitespace and quoted fields in the indirection are the
		// grammar's too; a quoted name is that exact name (case kept), and a
		// quoted name holding the column's letters is another column.
		{"column-list-spaced-subscript", `UPDATE aveloxis_data.repos SET (a, repo_git [1]) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-spaced-field", `UPDATE aveloxis_data.repos SET (a, repo_git . f) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-quoted-field", `UPDATE aveloxis_data.repos SET (a, repo_git."F") = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-quoted-other-case", `UPDATE aveloxis_data.repos SET (a, "REPO_GIT") = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-quoted-comma", `UPDATE aveloxis_data.repos SET ("x,repo_git", a) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-open-quote", `UPDATE aveloxis_data.repos SET ("repo_git, a) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		// Round 7: a quoted name holding commas, a parenthesised row after a
		// comma outside the SET clause, and a non-ASCII fold are no member.
		{"column-list-quoted-commas", `UPDATE aveloxis_data.repos SET ("x,repo_git,y", a) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-literal-subscript", `UPDATE aveloxis_data.repos SET (a, x[',repo_git,']) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		{"row-in-coalesce", `UPDATE aveloxis_data.repos SET flag = COALESCE($1, (repo_git) = $2)`, "aveloxis_data.repos", "repo_git", false},
		{"row-in-select-list", `INSERT INTO aveloxis_data.repos (repo_name) SELECT n, (o, repo_git) = ($1, $2) FROM s`, "aveloxis_data.repos", "repo_git", false},
		{"row-in-where-call", `UPDATE aveloxis_data.repos SET a = $1 WHERE x AND COALESCE($2, (b, repo_git) = ($3, $4))`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-kelvin", "UPDATE aveloxis_data.repos SET (a, repo_\u212Aey) = ($1, $2)", "aveloxis_data.repos", "repo_key", false},
		{"column-list-paren-subscript", `UPDATE aveloxis_data.repos SET (tags[array_length(tags, 1)], repo_git) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		// Round 8: brackets in an earlier assignment, IS DISTINCT FROM, E''
		// escapes and dollar quotes, and the clause's RETURNING / FROM ends.
		{"after-array-literal", `UPDATE aveloxis_data.repos SET tags = ARRAY[$1, $2], (x, repo_git) = ($3, $4)`, "aveloxis_data.repos", "repo_git", true},
		{"row-in-array", `UPDATE aveloxis_data.repos SET flags = ARRAY[true, (repo_git) = $1]`, "aveloxis_data.repos", "repo_git", false},
		{"after-is-distinct-from", `UPDATE aveloxis_data.repos SET changed = repo_name IS DISTINCT FROM $1, (x, repo_git) = ($2, $3)`, "aveloxis_data.repos", "repo_git", true},
		{"after-is-not-distinct-from", `UPDATE aveloxis_data.repos SET changed = (b) IS NOT DISTINCT FROM c, (x, repo_git) = ($2, $3)`, "aveloxis_data.repos", "repo_git", true},
		{"after-e-escape", `UPDATE aveloxis_data.repos SET x = E'\'', (a, repo_git) = (1, 2)`, "aveloxis_data.repos", "repo_git", true},
		{"after-dollar-quote", `UPDATE aveloxis_data.repos SET x = $$it's$$, (a, repo_git) = (1, 2)`, "aveloxis_data.repos", "repo_git", true},
		{"after-tagged-dollar-quote", `UPDATE aveloxis_data.repos SET x = $q$it's$q$, (a, repo_git) = (1, 2)`, "aveloxis_data.repos", "repo_git", true},
		{"set-in-e-escape", `UPDATE aveloxis_data.repos SET a = E'\' SET (repo_git) = (1)', b = 2`, "aveloxis_data.repos", "repo_git", false},
		{"set-in-dollar-quote", `UPDATE aveloxis_data.repos SET x = $$ SET (repo_git) = (1) $$`, "aveloxis_data.repos", "repo_git", false},
		{"param-is-no-dollar-quote", `UPDATE aveloxis_data.repos SET x = $1, (a, repo_git) = ($2, $3)`, "aveloxis_data.repos", "repo_git", true},
		{"row-in-returning", `UPDATE aveloxis_data.repos SET a = 1 RETURNING id, (repo_git) = $1 AS same`, "aveloxis_data.repos", "repo_git", false},
		{"row-in-from", `UPDATE aveloxis_data.repos r SET a = 1 FROM s, (SELECT 1, repo_git FROM t) v WHERE (v.a) = 1`, "aveloxis_data.repos", "repo_git", false},
		// Round 9: a DO or function body is executed SQL, not a value; a
		// qualified column named distinct is no IS DISTINCT FROM; a $ inside
		// an identifier is part of it, and an identifier ending in e before
		// a quote is no E'…' prefix.
		{"list-in-do-body", `DO $$ BEGIN UPDATE aveloxis_data.repos SET (a, repo_git) = (1, repos.repo_git) WHERE x = 1; END $$`, "aveloxis_data.repos", "repo_git", true},
		{"list-in-tagged-do-body", `DO $body$ BEGIN UPDATE aveloxis_data.repos SET (a, repo_git) = (1, 2); END $body$`, "aveloxis_data.repos", "repo_git", true},
		{"list-in-function-body", `CREATE FUNCTION f() RETURNS void AS $$ UPDATE aveloxis_data.repos SET (a, repo_git) = (1, 2) $$ LANGUAGE sql`, "aveloxis_data.repos", "repo_git", true},
		{"qualified-distinct-column", `UPDATE aveloxis_data.repos SET a = s.distinct FROM s, (SELECT 1, repo_git FROM t) v WHERE (v.a) = 1`, "aveloxis_data.repos", "repo_git", false},
		{"spaced-qualified-distinct-column", `UPDATE aveloxis_data.repos SET a = s . distinct FROM s, (SELECT 1, repo_git FROM t) v WHERE (v.a) = 1`, "aveloxis_data.repos", "repo_git", false},
		{"dollar-in-identifier", `UPDATE aveloxis_data.repos SET a = x$y$, (b, repo_git) = (1, 2)`, "aveloxis_data.repos", "repo_git", true},
		{"typed-literal-no-escape", `UPDATE aveloxis_data.repos SET d = name'a\', (b, repo_git) = (1, 2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-comma-in-subscript", `UPDATE aveloxis_data.repos SET (a, x[f(1, repo_git)]) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-nested-subscript", `UPDATE aveloxis_data.repos SET (a, repo_git[b[1]]) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-upper", `UPDATE aveloxis_data.repos SET (a, REPO_GIT) = ($1, $2)`, "aveloxis_data.repos", "repo_git", true},
		{"column-list-in-cte", `WITH u AS (UPDATE aveloxis_data.repos SET (repo_git, a) = ($1, $2) RETURNING 1) SELECT 1`, "aveloxis_data.repos", "repo_git", true},
		{"set-in-a-literal", `UPDATE aveloxis_data.repos SET a = 'x SET (repo_git) = y' WHERE b`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-qualified", `INSERT INTO aveloxis_data.repos (repo_git) SELECT x FROM s ORDER BY s.c, (s.added_at) = $1`, "aveloxis_data.repos", "added_at", false},
		{"column-list-substring", `UPDATE aveloxis_data.repos SET (repo_name, repo_git_old) = ($1, $2)`, "aveloxis_data.repos", "repo_git", false},
		{"column-list-in-where", `UPDATE aveloxis_data.repos SET repo_name = $1 WHERE (repo_owner, repo_git) = ($2, $3)`, "aveloxis_data.repos", "repo_git", false},
	}
	for _, c := range cases {
		s := Stmt{File: "fixture.go", SQL: c.sql}
		if got := s.WritesColumn(c.tbl, c.col); got != c.want {
			t.Errorf("%s: WritesColumn(%q, %q) = %v, want %v", c.name, c.tbl, c.col, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// SetExprs — the depth-counting RHS extractor. Fixture #1 is the VERBATIM
// multi-policy SET from internal/db/repo_metadata.go (v0.27.104/117/122
// shapes with inline comments), run through the FULL pipeline.
// ---------------------------------------------------------------------------

const repoMetadataUpdateGo = "package db\nvar q = `\n" +
	"\t\t\t\tUPDATE aveloxis_data.repos\n" +
	"\t\t\t\tSET repo_description = $2,\n" +
	"\t\t\t\t    primary_language = $3,\n" +
	"\t\t\t\t    languages        = $4::jsonb,\n" +
	"\t\t\t\t    -- v0.27.50: propagate the forge's archived status\n" +
	"\t\t\t\t    repo_archived    = $5,\n" +
	"\t\t\t\t    forked_from      = $6,\n" +
	"\t\t\t\t    -- v0.27.117: fill-empty-only (prefer the STORED value).\n" +
	"\t\t\t\t    platform_repo_id = COALESCE(NULLIF(repos.platform_repo_id, ''), $7),\n" +
	"\t\t\t\t    created_at = COALESCE(repos.created_at, $8),\n" +
	"\t\t\t\t    updated_at = GREATEST(COALESCE($9, repos.updated_at), COALESCE(repos.updated_at, $9)),\n" +
	"\t\t\t\t    data_collection_date = NOW()\n" +
	"\t\t\t\tWHERE repo_id = $1\n" +
	"\t\t\t\tRETURNING COALESCE(platform_repo_id, '')`\n"

func TestSetExprsOnRepoMetadataUpdate(t *testing.T) {
	stmts := Statements(map[string]string{"repo_metadata.go": repoMetadataUpdateGo})
	if len(stmts) != 1 {
		t.Fatalf("want 1 statement, got %d", len(stmts))
	}
	s := stmts[0]
	cases := []struct {
		col  string
		want string
	}{
		{"repo_description", "$2"},
		{"platform_repo_id", "COALESCE(NULLIF(repos.platform_repo_id, ''), $7)"},
		{"created_at", "COALESCE(repos.created_at, $8)"},
		{"updated_at", "GREATEST(COALESCE($9, repos.updated_at), COALESCE(repos.updated_at, $9))"},
		{"data_collection_date", "NOW()"},
	}
	for _, c := range cases {
		exprs := s.SetExprs(c.col)
		if len(exprs) != 1 {
			t.Errorf("SetExprs(%q): want 1 expr, got %d: %#v", c.col, len(exprs), exprs)
			continue
		}
		if exprs[0] != c.want {
			t.Errorf("SetExprs(%q) = %q, want %q", c.col, exprs[0], c.want)
		}
	}
	// The WHERE-clause and RETURNING references must NOT yield exprs.
	if got := s.SetExprs("repo_id"); len(got) != 0 {
		t.Errorf("repo_id appears only in WHERE/RETURNING — SetExprs must be empty, got %#v", got)
	}
}

func TestSetExprsHostileShapes(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		col  string
		want []string
	}{
		{"comma-in-quotes", `UPDATE t SET x = 'a,b', y = $1`, "x", []string{"'a,b'"}},
		{"nested-parens", `UPDATE t SET x = COALESCE(f(a, g(b, c)), $2), y = 1`, "x", []string{"COALESCE(f(a, g(b, c)), $2)"}},
		{"ends-at-where", `UPDATE t SET x = a || b WHERE id = 1`, "x", []string{"a || b"}},
		{"ends-at-from", `UPDATE t SET x = cand.v FROM cand WHERE t.id = cand.id`, "x", []string{"cand.v"}},
		{"ends-at-returning", `UPDATE t SET x = $1 RETURNING id`, "x", []string{"$1"}},
		{"ends-at-eos", `UPDATE t SET x = NOW()`, "x", []string{"NOW()"}},
		{"keyword-like-ident", `UPDATE t SET x = fromage.v, y = 1`, "x", []string{"fromage.v"}},
		{"conflict-clause", `INSERT INTO t (x) VALUES ($1) ON CONFLICT (id) DO UPDATE SET x = GREATEST(t.x, EXCLUDED.x)`, "x", []string{"GREATEST(t.x, EXCLUDED.x)"}},
		{"absent-column", `UPDATE t SET y = 1`, "x", nil},
		{"comparison-not-assignment", `UPDATE t SET y = 1 WHERE x <= 5`, "x", nil},
	}
	for _, c := range cases {
		s := Stmt{File: "f.go", SQL: c.sql}
		got := s.SetExprs(c.col)
		if len(got) != len(c.want) {
			t.Errorf("%s: SetExprs(%q) = %#v, want %#v", c.name, c.col, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("%s: expr[%d] = %q, want %q", c.name, i, got[i], c.want[i])
			}
		}
	}
}

// ---------------------------------------------------------------------------
// WhereGuardsEmpty — the guarded fill-empty-only shape
// (SetPlatformRepoIDIfEmptySeen: bare `SET col = $2` legitimized by
// `WHERE ... COALESCE(col, '') = ''`).
// ---------------------------------------------------------------------------

func TestWhereGuardsEmpty(t *testing.T) {
	cases := []struct {
		name string
		sql  string
		col  string
		want bool
	}{
		{"coalesce-guard", `UPDATE aveloxis_data.repos SET platform_repo_id = $2 WHERE repo_id = $1 AND COALESCE(platform_repo_id, '') = ''`, "platform_repo_id", true},
		{"is-null-guard", `UPDATE aveloxis_data.repos SET forked_from = $2 WHERE repo_id = $1 AND forked_from IS NULL`, "forked_from", true},
		{"qualified-guard", `UPDATE aveloxis_data.repos r SET platform_repo_id = $2 WHERE r.repo_id = $1 AND COALESCE(r.platform_repo_id, '') = ''`, "platform_repo_id", true},
		{"no-guard", `UPDATE aveloxis_data.repos SET platform_repo_id = $2 WHERE repo_id = $1`, "platform_repo_id", false},
		{"guard-on-other-column", `UPDATE aveloxis_data.repos SET platform_repo_id = $2 WHERE COALESCE(forked_from, '') = ''`, "platform_repo_id", false},
		{"no-where-at-all", `UPDATE aveloxis_data.repos SET platform_repo_id = $2`, "platform_repo_id", false},
	}
	for _, c := range cases {
		s := Stmt{File: "f.go", SQL: c.sql}
		if got := s.WhereGuardsEmpty(c.col); got != c.want {
			t.Errorf("%s: WhereGuardsEmpty(%q) = %v, want %v", c.name, c.col, got, c.want)
		}
	}
}

// v0.27.130 (Copilot round 18, suppressed — real): backticked SQL inside
// Go COMMENTS must not become statements — the corpus really contains
// one (internal/db/scancode_worker_store.go doc comment carries a
// backticked UPDATE), and counting documentation as a writer would let
// a removed real write pass the tripwires. Statements strips Go
// comments FIRST (StripGoComments preserves raw strings).
func TestStatementsIgnoreCommentEmbeddedLiterals(t *testing.T) {
	files := map[string]string{
		"doc.go": "package db\n" +
			"// The old shape was `UPDATE aveloxis_data.repos SET repo_name = $1` —\n" +
			"// kept here for history only.\n" +
			"var q = `INSERT INTO aveloxis_data.repos (repo_git) VALUES ($1)`\n",
	}
	stmts := Statements(files)
	if len(stmts) != 1 {
		t.Fatalf("want 1 statement (the real literal), got %d: %#v", len(stmts), stmts)
	}
	if strings.Contains(stmts[0].SQL, "UPDATE") {
		t.Fatalf("the comment-embedded backtick text leaked into the corpus: %q", stmts[0].SQL)
	}
}
