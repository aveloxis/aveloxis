// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// TestOnConflictClausesHaveRealArbiters is the permanent tripwire for
// the dead-ON-CONFLICT class, which has now produced three production
// incidents in one month:
//
//   - repo_labor (v0.27.7): blanket ON CONFLICT DO NOTHING with no
//     unique — table grew unbounded to 2.0M rows / 29 GB.
//   - pull_request_review_message_ref (v0.27.15): same shape — 5.26M
//     duplicate bridge rows (50% of the table).
//   - repo_groups (v0.27.17): the lazy 'Default' creation's bare
//     clause never fired, so RETURNING always yielded a fresh row —
//     93,912 'Default' groups on production, one per repo, silently
//     shattering every repo_group_id rollup.
//
// The mechanism: `ON CONFLICT DO NOTHING` without a conflict target
// only does anything if SOME unique constraint/index exists on the
// table; a named target `(cols)` requires a MATCHING unique or the
// statement errors (42P10) at runtime. The idempotency suite
// (TestAllDataInsertTablesHaveOnConflict) checks that the TEXT is
// present — it cannot see that the clause is inert. This test closes
// that hole: every ON CONFLICT in the store layer must arbitrate
// against a unique that actually exists (in schema.sql or created by
// a migration).
func TestOnConflictClausesHaveRealArbiters(t *testing.T) {
	// ---- 1. Collect every unique arbiter that exists.
	type arb struct {
		cols     map[string]bool // nil = wildcard (LIKE INCLUDING ALL)
		isSerial bool
	}
	uniques := map[string][]arb{} // table -> arbiters
	addCols := func(table, colList string, serial bool) {
		cols := map[string]bool{}
		for _, c := range strings.Split(colList, ",") {
			cols[strings.Fields(strings.TrimSpace(c))[0]] = true
		}
		uniques[table] = append(uniques[table], arb{cols: cols, isSerial: serial})
	}

	schemaRaw, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	schema := regexp.MustCompile(`--[^\n]*`).ReplaceAllString(string(schemaRaw), "")

	tableRe := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS\s+([a-z_]+\.[a-z_]+)\s*\((.*?)\n\);`)
	for _, m := range tableRe.FindAllStringSubmatch(schema, -1) {
		table, body := m[1], m[2]
		for _, um := range regexp.MustCompile(`UNIQUE\s*\(([^)]+)\)`).FindAllStringSubmatch(body, -1) {
			addCols(table, um[1], false)
		}
		for _, pm := range regexp.MustCompile(`PRIMARY KEY\s*\(([^)]+)\)`).FindAllStringSubmatch(body, -1) {
			addCols(table, pm[1], false)
		}
		for _, cm := range regexp.MustCompile(`(?m)^\s*([a-z_]+)\s+[A-Z].*?(PRIMARY KEY|UNIQUE)`).FindAllStringSubmatch(body, -1) {
			line := cm[0]
			serial := strings.Contains(line, "SERIAL")
			addCols(table, cm[1], serial)
		}
		if strings.Contains(body, "LIKE ") && strings.Contains(body, "INCLUDING ALL") {
			uniques[table] = append(uniques[table], arb{cols: nil}) // inherits parent's indexes
		}
	}

	// Migration-created (and schema-declared) unique indexes, from
	// schema.sql plus every non-test go file in this package.
	sources := schema
	goFiles, _ := filepath.Glob("*.go")
	for _, f := range goFiles {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		sources += string(b)
	}
	uniqIdxRe := regexp.MustCompile(`CREATE UNIQUE INDEX(?:\s+CONCURRENTLY)?(?:\s+IF NOT EXISTS)?\s+\S+\s*\n?\s*ON\s+([a-z_]+\.[a-z_]+)\s*(?:USING \w+\s*)?\(([^)]+)\)`)
	for _, m := range uniqIdxRe.FindAllStringSubmatch(sources, -1) {
		cleaned := regexp.MustCompile(`\([^)]*\)`).ReplaceAllString(m[2], "")
		addCols(m[1], cleaned, false)
	}

	if len(uniques) < 100 {
		t.Fatalf("unique-arbiter extraction found only %d tables — the regexes have rotted; fix the test, do not trust this run", len(uniques))
	}

	// ---- 2. Every INSERT ... ON CONFLICT in the store layer.
	//
	// Allowlist for deliberate exceptions. Keep this EMPTY unless a
	// bare clause is genuinely correct; every entry needs a rationale.
	allowBare := map[string]string{
		// (none — all three known cases are fixed)
	}

	insertRe := regexp.MustCompile(`INSERT INTO\s+(aveloxis_\w+\.\w+)`)
	conflictRe := regexp.MustCompile(`ON CONFLICT\s*(\(([^)]*)\))?`)
	inserts := 0
	for _, f := range goFiles {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		for _, loc := range insertRe.FindAllStringSubmatchIndex(src, -1) {
			table := src[loc[2]:loc[3]]
			// Window to the end of the SQL literal.
			window := src[loc[0]:min(loc[0]+2200, len(src))]
			if end := strings.Index(window[12:], "`"); end > 0 {
				window = window[:12+end]
			}
			inserts++
			oc := conflictRe.FindStringSubmatch(window)
			if oc == nil {
				continue // no ON CONFLICT at all — the idempotency suite governs those
			}
			arbs := uniques[table]
			hasNonSerial := false
			wildcard := false
			for _, a := range arbs {
				if a.cols == nil {
					wildcard = true
				} else if !a.isSerial {
					hasNonSerial = true
				}
			}
			if oc[2] == "" { // bare ON CONFLICT
				if !hasNonSerial && !wildcard {
					if why, ok := allowBare[table]; ok {
						t.Logf("allowlisted bare ON CONFLICT on %s (%s): %s", table, f, why)
						continue
					}
					t.Errorf("%s (%s): bare ON CONFLICT with NO non-serial unique on the table — the clause is DEAD and the insert duplicates on every re-run (repo_labor/review_msg_ref/repo_groups class)", table, f)
				}
				continue
			}
			// Named target: must match an existing unique's column set
			// exactly (partial-index predicates don't change the set).
			want := map[string]bool{}
			for _, c := range strings.Split(oc[2], ",") {
				c = strings.TrimSpace(c)
				if i := strings.Index(c, " "); i > 0 { // strip WHERE etc.
					c = c[:i]
				}
				want[c] = true
			}
			matched := wildcard
			for _, a := range arbs {
				if a.cols == nil || len(a.cols) != len(want) {
					continue
				}
				same := true
				for c := range want {
					if !a.cols[c] {
						same = false
						break
					}
				}
				if same {
					matched = true
					break
				}
			}
			if !matched {
				t.Errorf("%s (%s): ON CONFLICT (%s) names an arbiter with no matching unique constraint/index — this errors 42P10 at runtime (or the unique was never created)", table, f, oc[2])
			}
		}
	}
	if inserts < 80 {
		t.Fatalf("only %d INSERT sites parsed — the extraction regex has rotted; fix the test", inserts)
	}
}
