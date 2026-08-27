// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// bulkDeletedParents are the tables a migration or operator heal DELETEs
// rows from (or repoints children away from) in bulk: repos (dedup-repos,
// the rename-duplicate heal, mark-gone), repo_groups (the v0.27.17
// consolidation), repo_groups_list_serve (the v0.28.18 list dedup). Every
// FK child column of these parents must be the LEADING column of some
// index — a plain index, a composite led by it, a PK or a UNIQUE — or the
// parent delete's DEFERRED FK check sequential-scans the child per deleted
// row at COMMIT (the v0.25.34 class; measured 5.3 s per scan of the 12 GB
// email_message on the 2026-08-26 `aveloxis` DB migrate).
var bulkDeletedParents = []string{"repos", "repo_groups", "repo_groups_list_serve"}

// fkIndexExemptions: children deliberately left unindexed, with the
// reason at the site. Only tiny tables belong here.
var fkIndexExemptions = map[string]string{}

// TestFKChildrenOfBulkDeletedParentsAreIndexed — v0.28.18, the
// generalization of TestEveryRepoGroupsFKChildIsIndexed to every parent
// the tree bulk-deletes from. Index sources: schema.sql plus every
// non-test file in internal/db (migration-owned CONCURRENTLY builds live
// in Go), PK/UNIQUE table constraints and column-level PRIMARY KEY /
// UNIQUE in the CREATE TABLE. LIKE-cloned history tables carry no
// REFERENCES of their own and fall out naturally.
func TestFKChildrenOfBulkDeletedParentsAreIndexed(t *testing.T) {
	schema := readSourceFile(t, "schema.sql")
	var goSrc strings.Builder
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		goSrc.Write(b)
		goSrc.WriteString("\n")
	}
	corpus := schema + "\n" + goSrc.String()

	// leading[schema.table] = set of leading index columns.
	leading := map[string]map[string]bool{}
	mark := func(qualified, col string) {
		if leading[qualified] == nil {
			leading[qualified] = map[string]bool{}
		}
		leading[qualified][col] = true
	}
	idxRe := regexp.MustCompile(`(?is)CREATE\s+(?:UNIQUE\s+)?INDEX\s+(?:CONCURRENTLY\s+)?(?:IF\s+NOT\s+EXISTS\s+)?\w+\s+ON\s+(aveloxis_\w+)\.(\w+)\s*(?:USING\s+\w+\s*)?\(\s*(\w+)`)
	for _, m := range idxRe.FindAllStringSubmatch(corpus, -1) {
		mark(m[1]+"."+m[2], m[3])
	}
	// Migration-owned index LISTS build their CREATE INDEX via fmt.Sprintf,
	// so the text scan cannot see them — credit the same lists the
	// migration iterates (probe/tripwire parity, L13).
	for _, idx := range repoGroupFKIndexes {
		mark("aveloxis_data."+idx.table, idx.column)
	}
	for _, idx := range emailMessageFKIndexes {
		mark("aveloxis_data.email_message", idx.column)
	}
	tableRe := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS\s+(aveloxis_\w+)\.(\w+)\s*\((.*?)\n\);`)
	constraintRe := regexp.MustCompile(`(?i)(?:PRIMARY KEY|UNIQUE)\s*\(\s*(\w+)`)
	colConstraintRe := regexp.MustCompile(`(?im)^\s*(\w+)\s+\w+[^\n]*\b(?:PRIMARY KEY|UNIQUE)\b`)
	type child struct{ table, column, parent string }
	var children []child
	for _, tm := range tableRe.FindAllStringSubmatch(schema, -1) {
		qualified, body := tm[1]+"."+tm[2], tm[3]
		for _, cm := range constraintRe.FindAllStringSubmatch(body, -1) {
			mark(qualified, cm[1])
		}
		for _, cm := range colConstraintRe.FindAllStringSubmatch(body, -1) {
			mark(qualified, cm[1])
		}
		for _, parent := range bulkDeletedParents {
			fkRe := regexp.MustCompile(`(?m)^\s*(\w+)\s+\w+[^\n]*REFERENCES\s+aveloxis_data\.` + parent + `\s*\(`)
			for _, cm := range fkRe.FindAllStringSubmatch(body, -1) {
				children = append(children, child{table: qualified, column: cm[1], parent: parent})
			}
		}
	}
	if len(children) < 40 {
		t.Fatalf("schema scan found only %d FK children of %v — the regex broke", len(children), bulkDeletedParents)
	}
	var missing []string
	for _, c := range children {
		id := c.table + "." + c.column
		if leading[c.table][c.column] {
			continue
		}
		if _, ok := fkIndexExemptions[id]; ok {
			continue
		}
		missing = append(missing, fmt.Sprintf("%s → %s: no index led by %s (a bulk delete of %s rows seq-scans %s per row at COMMIT); add a migration-owned CONCURRENTLY index or an exemption with a reason", id, c.parent, c.column, c.parent, c.table))
	}
	sort.Strings(missing)
	for _, m := range missing {
		t.Error(m)
	}
	for id := range fkIndexExemptions {
		found := false
		for _, c := range children {
			if c.table+"."+c.column == id {
				found = true
			}
		}
		if !found {
			t.Errorf("stale exemption %s — no such FK child of %v", id, bulkDeletedParents)
		}
	}
}
