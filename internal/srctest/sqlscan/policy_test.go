// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package sqlscan

import (
	"strings"
	"testing"
)

// The founding incident (PR #184 round 11, v0.27.117): UpsertRepo's ON
// CONFLICT and UpdateRepoMetadata both carried prefer-nonempty-INCOMING
// on platform_repo_id while the intended policy was fill-empty-only —
// a non-empty incoming ID overwrote a different stored one, silently
// erasing the round-10 delete-and-recreate mismatch signal. The
// VERBATIM shipped bug expression is the permanent canary below: if it
// ever conforms to FillEmptyOnly, detection power died.
const shippedBugExpr = `COALESCE(NULLIF(EXCLUDED.platform_repo_id, ''), repos.platform_repo_id)`

func stmtFor(sql string) Stmt { return Stmt{File: "fixture.go", SQL: sql} }

func TestExprConformsFillEmptyOnly(t *testing.T) {
	col := "platform_repo_id"
	plain := stmtFor(`UPDATE aveloxis_data.repos SET platform_repo_id = X WHERE repo_id = $1`)
	cases := []struct {
		name string
		stmt Stmt
		expr string
		want bool
	}{
		{"stored-first-excluded", plain, `COALESCE(NULLIF(repos.platform_repo_id, ''), EXCLUDED.platform_repo_id)`, true},
		{"stored-first-param", plain, `COALESCE(NULLIF(repos.platform_repo_id, ''), $7)`, true},
		{"CANARY-shipped-bug-incoming-first", plain, shippedBugExpr, false},
		{"swapped-nullable", plain, `COALESCE($8, repos.created_at)`, false},
		{"bare-param-without-guard", plain, `$2`, false},
		{"bare-param-with-guard", stmtFor(`UPDATE aveloxis_data.repos SET platform_repo_id = $2 WHERE repo_id = $1 AND COALESCE(platform_repo_id, '') = ''`), `$2`, true},
		{"unrecognized-shape", plain, `some_function(repos.platform_repo_id)`, false},
	}
	for _, c := range cases {
		got, detail := exprConforms(c.stmt, c.expr, col, FillEmptyOnly)
		if got != c.want {
			t.Errorf("%s: conforms=%v want %v (detail: %s)", c.name, got, c.want, detail)
		}
	}
	// Nullable form on its own column.
	if ok, _ := exprConforms(plain, `COALESCE(repos.created_at, $8)`, "created_at", FillEmptyOnly); !ok {
		t.Error("nullable fill-empty form COALESCE(stored, incoming) must conform")
	}
}

func TestExprConformsPreferPolicies(t *testing.T) {
	plain := stmtFor(`UPDATE aveloxis_data.repos SET x = 1`)
	if ok, _ := exprConforms(plain, shippedBugExpr, "platform_repo_id", PreferNonemptyIncoming); !ok {
		t.Error("the incoming-first NULLIF shape IS PreferNonemptyIncoming — the canary must classify, not vanish")
	}
	if ok, _ := exprConforms(plain, `COALESCE(NULLIF(repos.forked_from, ''), EXCLUDED.forked_from)`, "forked_from", PreferNonemptyIncoming); ok {
		t.Error("stored-first must NOT conform to PreferNonemptyIncoming (order-sensitivity)")
	}
	if ok, _ := exprConforms(plain, `COALESCE($9, repos.updated_at)`, "updated_at", PreferNonNullIncoming); !ok {
		t.Error("COALESCE(incoming, stored) is PreferNonNullIncoming")
	}
	if ok, _ := exprConforms(plain, `COALESCE(repos.updated_at, $9)`, "updated_at", PreferNonNullIncoming); ok {
		t.Error("COALESCE(stored, incoming) must NOT conform to PreferNonNullIncoming (order-sensitivity)")
	}
}

func TestExprConformsGreatestNonNull(t *testing.T) {
	plain := stmtFor(`UPDATE aveloxis_data.repos SET x = 1`)
	col := "updated_at"
	real := `GREATEST(COALESCE($9, repos.updated_at), COALESCE(repos.updated_at, $9))`
	if ok, _ := exprConforms(plain, real, col, GreatestNonNull); !ok {
		t.Error("the v0.27.122 nil-safe GREATEST pair must conform")
	}
	excluded := `GREATEST(COALESCE(EXCLUDED.updated_at, repos.updated_at), COALESCE(repos.updated_at, EXCLUDED.updated_at))`
	if ok, _ := exprConforms(plain, excluded, col, GreatestNonNull); !ok {
		t.Error("the EXCLUDED-flavored GREATEST pair must conform")
	}
	sameOrder := `GREATEST(COALESCE($9, repos.updated_at), COALESCE($9, repos.updated_at))`
	if ok, _ := exprConforms(plain, sameOrder, col, GreatestNonNull); ok {
		t.Error("same-order-twice is NOT the nil-safe pair — one side loses its fallback")
	}
	if ok, _ := exprConforms(plain, `GREATEST($9, repos.updated_at)`, col, GreatestNonNull); ok {
		t.Error("plain GREATEST without the COALESCE pair is not nil-safe (NULL poisons GREATEST? no — but the registered shape is the pair; unrecognized must fail closed)")
	}
}

func TestExprConformsAlwaysRefresh(t *testing.T) {
	plain := stmtFor(`UPDATE aveloxis_data.repos SET x = 1`)
	col := "forked_from"
	for _, e := range []string{`$6`, `NOW()`, `EXCLUDED.forked_from`} {
		if ok, _ := exprConforms(plain, e, col, AlwaysRefresh); !ok {
			t.Errorf("%q must conform to AlwaysRefresh (no stored reference)", e)
		}
	}
	if ok, _ := exprConforms(plain, `COALESCE(NULLIF(EXCLUDED.forked_from, ''), repos.forked_from)`, col, AlwaysRefresh); ok {
		t.Error("an expression referencing the STORED value is not AlwaysRefresh")
	}
}

func TestCheckInsertOnlyAndExceptions(t *testing.T) {
	stmts := []Stmt{
		{File: "a.go", SQL: `INSERT INTO aveloxis_data.repos (repo_git, added_at) VALUES ($1, NOW())`},
		{File: "migrate.go", SQL: `UPDATE aveloxis_data.repos SET added_at = COALESCE(data_collection_date, created_at, NOW()) WHERE added_at IS NULL`},
	}
	reg := []Registered{{Table: "aveloxis_data.repos", Column: "added_at", Policy: InsertOnly, Reason: "fleet-entry stamp"}}

	// Without an exception: the migrate backfill is a violation.
	rep := Check(stmts, reg, nil)
	if len(rep.Violations) != 1 || rep.Violations[0].File != "migrate.go" {
		t.Fatalf("want 1 InsertOnly violation attributed to migrate.go, got %#v", rep.Violations)
	}
	if len(rep.Unwritten) != 0 {
		t.Fatalf("added_at HAS writers — Unwritten must be empty, got %#v", rep.Unwritten)
	}

	// With the exception: suppressed, and the exception is not stale.
	exc := []Exception{{Table: "aveloxis_data.repos", Column: "added_at", File: "migrate.go",
		Match: "SET added_at = COALESCE(data_collection_date", Reason: "one-shot backfill"}}
	rep = Check(stmts, reg, exc)
	if len(rep.Violations) != 0 {
		t.Fatalf("exception must suppress the violation, got %#v", rep.Violations)
	}
	if len(rep.StaleExceptions) != 0 {
		t.Fatalf("a suppressing exception is not stale, got %#v", rep.StaleExceptions)
	}

	// A never-matching exception is STALE (allowlist-rot reverse check).
	exc = append(exc, Exception{Table: "aveloxis_data.repos", Column: "added_at", File: "gone.go",
		Match: "nonexistent needle", Reason: "rotted"})
	rep = Check(stmts, reg, exc)
	if len(rep.StaleExceptions) != 1 || rep.StaleExceptions[0].File != "gone.go" {
		t.Fatalf("the never-matching exception must be reported stale, got %#v", rep.StaleExceptions)
	}
}

func TestCheckUnwrittenAndViolationDetail(t *testing.T) {
	stmts := []Stmt{
		{File: "postgres.go", SQL: `INSERT INTO aveloxis_data.repos (repo_git, platform_repo_id) VALUES ($1, $2)
			ON CONFLICT (repo_git) DO UPDATE SET platform_repo_id = ` + shippedBugExpr},
	}
	reg := []Registered{
		{Table: "aveloxis_data.repos", Column: "platform_repo_id", Policy: FillEmptyOnly, Reason: "forge ID never changes"},
		{Table: "aveloxis_data.repos", Column: "ghost_col", Policy: FillEmptyOnly, Reason: "registry rot probe"},
	}
	rep := Check(stmts, reg, nil)
	if len(rep.Violations) != 1 {
		t.Fatalf("the shipped-bug shape must violate FillEmptyOnly, got %#v", rep.Violations)
	}
	// The detail must NAME the policy the expression actually matches —
	// the round-11 postmortem hinged on recognizing "this is
	// prefer-nonempty-INCOMING" at a glance.
	if !strings.Contains(rep.Violations[0].Detail, "PreferNonemptyIncoming") {
		t.Errorf("violation detail should classify the actual shape, got %q", rep.Violations[0].Detail)
	}
	if len(rep.Unwritten) != 1 || rep.Unwritten[0].Column != "ghost_col" {
		t.Fatalf("a registered column with no writers must be reported (registry rot), got %#v", rep.Unwritten)
	}
}
