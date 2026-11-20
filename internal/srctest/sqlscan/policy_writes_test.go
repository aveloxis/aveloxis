// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// A statement assembled in Go is read whole when every part of it is fixed
// text, and reported when a part is not (v0.29.203; summary/71's 0.29.166
// item). Until then such a statement reached the write-policy check in
// pieces, or not at all.

// The stored value may be named in full: schema.table.column (the Jira
// comment upsert, read whole for the first time in 0.29.203, names it so).
func TestStoredValueNamedInFull(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.m", Column: "u", Policy: GreatestNonNull, Reason: "r"}}
	ok := Stmt{File: "f.go", SQL: "INSERT INTO aveloxis_data.m (id, u) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET u = GREATEST(COALESCE(EXCLUDED.u, aveloxis_data.m.u), COALESCE(aveloxis_data.m.u, EXCLUDED.u))"}
	if r := Check([]Stmt{ok}, registry, nil); len(r.Violations) != 0 {
		t.Errorf("the full name of the stored value was not recognised: %+v", r.Violations)
	}
	// Another table's column of that name is not the stored value, and
	// EXCLUDED in any case is the incoming one.
	for _, bad := range []string{
		"GREATEST(COALESCE(EXCLUDED.u, aveloxis_data.other.x), COALESCE(aveloxis_data.other.x, EXCLUDED.u))",
		"GREATEST(COALESCE(EXCLUDED.u, a.b.c.u), COALESCE(a.b.c.u, EXCLUDED.u))",
	} {
		s := Stmt{File: "f.go", SQL: "INSERT INTO aveloxis_data.m (id, u) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET u = " + bad}
		if r := Check([]Stmt{s}, registry, nil); len(r.Violations) != 1 {
			t.Errorf("%s: %d violations, want 1", bad, len(r.Violations))
		}
	}
}

// One statement can write two tables (a CTE clears a repository's gone mark
// and the statement proper upserts its queue row; read whole for the first
// time in 0.29.203). An assignment belongs to the write it stands in: the
// queue's `updated_at = NOW()` is not the repository's updated_at.
func TestAnAssignmentBelongsToItsOwnWrite(t *testing.T) {
	const two = `WITH cleared AS (
		UPDATE aveloxis_data.repos SET gone = NULL, updated_at = GREATEST(COALESCE($2, repos.updated_at), COALESCE(repos.updated_at, $2))
		WHERE repo_id = $1 RETURNING repo_id)
	INSERT INTO aveloxis_ops.queue (repo_id, note) SELECT $1, 'UPDATE aveloxis_data.repos SET x' WHERE EXISTS (SELECT 1 FROM cleared)
	ON CONFLICT (repo_id) DO UPDATE SET status = 'queued', updated_at = NOW()`
	s := Stmt{File: "f.go", SQL: two}
	if got := s.SetExprsOf("aveloxis_data.repos", "updated_at"); len(got) != 1 || !strings.HasPrefix(got[0], "GREATEST(") {
		t.Errorf("the repository's updated_at: %q, want its own one assignment", got)
	}
	if got := s.SetExprsOf("aveloxis_ops.queue", "updated_at"); len(got) != 1 || got[0] != "NOW()" {
		t.Errorf("the queue's updated_at: %q, want NOW()", got)
	}
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "updated_at", Policy: GreatestNonNull, Reason: "r"}}
	if r := Check([]Stmt{s}, registry, nil); len(r.Violations) != 0 {
		t.Errorf("the queue's assignment was judged as the repository's: %+v", r.Violations)
	}
	// The repository's own assignment is still judged, wherever it stands.
	for what, sql := range map[string]string{
		"in the CTE":            strings.Replace(two, "GREATEST(COALESCE($2, repos.updated_at), COALESCE(repos.updated_at, $2))", "NOW()", 1),
		"a plain update":        "UPDATE aveloxis_data.repos SET updated_at = NOW() WHERE repo_id = $1",
		"UPDATE ONLY":           "UPDATE ONLY aveloxis_data.repos SET a = 1, updated_at = NOW()",
		"lower case":            "update aveloxis_data.repos set updated_at = NOW()",
		"a conflict's update":   "INSERT INTO aveloxis_data.repos (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET updated_at = NOW()",
		"after another's write": "WITH q AS (INSERT INTO aveloxis_ops.queue (id) VALUES ($1) ON CONFLICT (id) DO UPDATE SET updated_at = NOW() RETURNING id) UPDATE aveloxis_data.repos SET updated_at = NOW() FROM q",
		// Where the write cannot be told, the assignment counts (fail closed).
		"a table by alias only": "WITH w AS (UPDATE aveloxis_data.repos SET a = 1 RETURNING id) UPDATE r SET updated_at = NOW() FROM w",
		"MERGE":                 "MERGE INTO aveloxis_data.repos r USING s ON r.id = s.id WHEN MATCHED THEN UPDATE SET updated_at = NOW()",
	} {
		if r := Check([]Stmt{{File: "f.go", SQL: sql}}, registry, nil); len(r.Violations) != 1 {
			t.Errorf("%s: %d violations, want 1", what, len(r.Violations))
		}
	}
}

// A column assigned by its quoted name is that column (the pre-1970 repair
// writes `SET "col" = ...` for each row of a list; a mutant that added a
// protected column to the list passed until 0.29.203). A quoted name is
// exact: "Updated_At" is another column.
func TestAQuotedColumnIsAssigned(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "updated_at", Policy: GreatestNonNull, Reason: "r"}}
	for sql, want := range map[string]int{
		`UPDATE aveloxis_data.repos SET "updated_at" = NOW() WHERE "updated_at" IS NOT NULL`: 1,
		`UPDATE aveloxis_data.repos SET a = 1, "updated_at" = NOW()`:                         1,
		`UPDATE aveloxis_data.repos SET "Updated_At" = NOW()`:                                0,
		`UPDATE aveloxis_data.repos SET a = 1 WHERE "updated_at" = NOW()`:                    0,
		`UPDATE aveloxis_data.repos SET "not_updated_at" = NOW()`:                            0,
	} {
		s := Stmt{File: "f.go", SQL: sql}
		if got := len(s.SetExprsOf("aveloxis_data.repos", "updated_at")); got != want {
			t.Errorf("%s: %d assignments read, want %d", sql, got, want)
		}
		if r := Check([]Stmt{s}, registry, nil); len(r.Violations) != want {
			t.Errorf("%s: %d violations, want %d", sql, len(r.Violations), want)
		}
		if want == 1 && !s.WritesColumn("aveloxis_data.repos", "updated_at") {
			t.Errorf("%s: WritesColumn says no", sql)
		}
	}
}

// An exception that names no file is for ONE statement, whole: its Match
// is the statement's entire text (whitespace aside), not a needle in it. A
// change to the statement (its guard removed, say) ends the exception.
func TestAnExceptionWithNoFileMatchesTheWholeStatement(t *testing.T) {
	registry := []Registered{{Table: "aveloxis_data.repos", Column: "a", Policy: FillEmptyOnly, Reason: "r"}}
	const guarded = `UPDATE aveloxis_data.repos SET "a" = NULL WHERE "a" IS NOT NULL AND EXTRACT(YEAR FROM "a") < 1970`
	ex := []Exception{{Table: "aveloxis_data.repos", Column: "a", Match: guarded, Reason: "garbage only"}}
	if r := Check([]Stmt{{File: "f.go", SQL: "UPDATE aveloxis_data.repos\n\tSET \"a\" = NULL WHERE \"a\" IS NOT NULL AND EXTRACT(YEAR FROM \"a\") < 1970"}}, registry, ex); len(r.Violations) != 0 || len(r.StaleExceptions) != 0 {
		t.Errorf("the statement itself: %+v, stale %+v", r.Violations, r.StaleExceptions)
	}
	for what, sql := range map[string]string{
		"its guard removed":  `UPDATE aveloxis_data.repos SET "a" = NULL WHERE "a" IS NOT NULL`,
		"a longer statement": guarded + ` OR TRUE`,
		"a prefix":           `UPDATE aveloxis_data.repos SET "a" = NULL`,
	} {
		if r := Check([]Stmt{{File: "f.go", SQL: sql}}, registry, ex); len(r.Violations) != 1 || len(r.StaleExceptions) != 1 {
			t.Errorf("%s: %d violations, %d stale; want the violation, and the exception stale", what, len(r.Violations), len(r.StaleExceptions))
		}
	}
}
