// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package writejudge

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest/sqlscan"
)

// Round 7 (0.29.214): an Exception pins the statement it excuses, whole.
// The registry's own entries are held to that here, where they are kept;
// that each still excuses a statement of the store is the store's test
// (internal/db, TestColumnWritePolicies: a stale Exception fails it).

// Every Exception of the registry is well formed by the rule: it names its
// file and declaration, and pins one whole statement in which its Fragment
// is an assignment of its column.
func TestEveryExceptionPinsItsWholeStatement(t *testing.T) {
	if len(Exceptions) < 16 {
		t.Fatalf("%d exceptions; the registry held 16", len(Exceptions))
	}
	for _, ex := range Exceptions {
		if ex.File == "" || ex.Decl == "" || ex.Reason == "" || ex.Statement == "" {
			t.Errorf("exception %s.%s in %s: a file, a declaration, a reason and the statement, whole, are all required", ex.Table, ex.Column, ex.Decl)
			continue
		}
		// It pins its own statement, and CheckIn finds nothing wrong with
		// its form (a malformed one is reported and excuses nothing).
		pinned, err := ex.Pins(ex.Statement)
		if err != nil || !pinned {
			t.Errorf("exception %s.%s in %s does not pin its own statement: %v", ex.Table, ex.Column, ex.Decl, err)
		}
	}
	rep := sqlscan.CheckIn(sqlscan.NewCatalog(), nil, nil, Exceptions)
	for _, bad := range rep.BadExceptions {
		t.Error(bad)
	}
}

// The three Exceptions whose REASON rests on the statement's WHERE (round
// 7's finding): each stops excusing the statement when that condition is
// taken out, at run time as in the static check. The second is the edit
// that shipped as the 0.29.140 regression.
func TestAnExceptionDoesNotOutliveTheConditionItsReasonRestsOn(t *testing.T) {
	cat := sqlscan.NewCatalog().Table("aveloxis_data.repo_info")
	for _, p := range Policies {
		cat.Table(p.Table)
	}
	for _, c := range []struct{ decl, column, condition string }{
		{"migrateStage10RecentReleases", "added_at", "WHERE added_at IS NULL"},
		{"migrateStage10RecentReleases", "repo_archived", "AND r.repo_gone_at IS NULL"},
		{"PostgresStore.adoptForgeIDInTx", "platform_repo_id", "AND platform_repo_id = $2"},
	} {
		var ex *sqlscan.Exception
		for i := range Exceptions {
			if Exceptions[i].Decl == c.decl && Exceptions[i].Column == c.column {
				ex = &Exceptions[i]
			}
		}
		if ex == nil || strings.Count(ex.Statement, c.condition) != 1 {
			t.Errorf("%s %s: no exception whose statement holds %q once", c.decl, c.column, c.condition)
			continue
		}
		send := func(sql string) []string {
			return Violations(cat, []*Sent{{SQL: sql, Frames: []Frame{{File: ex.File, Line: 1, Decl: ex.Decl}}}}, Policies, Exceptions)
		}
		if held := send(ex.Statement); len(held) != 0 {
			t.Errorf("%s %s: the excepted statement itself is held: %q", c.decl, c.column, held)
		}
		without := strings.Replace(ex.Statement, c.condition, "", 1)
		if held := send(without); len(held) != 1 || !strings.Contains(held[0], c.column) {
			t.Errorf("%s %s: with %q taken out the statement is still excused: %q\n  %s", c.decl, c.column, c.condition, held, without)
		}
	}
}

// The gone-clear statements: one Exception for each, none for a clearer
// that is not one of them.
func TestTheGoneClearExceptionsAreOnePerStatement(t *testing.T) {
	n := 0
	seen := map[string]bool{}
	for _, ex := range Exceptions {
		if ex.Decl != "clearInTxUnderRowLock" {
			continue
		}
		n++
		if seen[ex.Statement] {
			t.Error("two gone-clear exceptions pin the same statement")
		}
		seen[ex.Statement] = true
	}
	if n != len(goneClearStatements) || n < 4 {
		t.Errorf("%d gone-clear exceptions for %d statements", n, len(goneClearStatements))
	}
}
