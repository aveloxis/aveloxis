// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestEveryTableIsDocumentedInSchemaMD (Copilot's review of PR #246 on
// 0.29.125): aveloxis_ops.page_cache_replay (0.29.122) had no entry in
// docs/schema.md although the schema-change checklist
// (docs/contributing/schema-migrations.md, step 8) requires the Source
// column for every column added — and the table was the 44th of 164 missing.
// Every `CREATE TABLE` of schema.sql now needs a `#### <table>` section, or a
// line in scripts/schema_docs_baseline.txt. The baseline is the undocumented
// backlog at 0.29.126 and only shrinks: a table documented later is removed
// from it, and a new table is never added to it.
func TestEveryTableIsDocumentedInSchemaMD(t *testing.T) {
	sql := srctest.Read(t, "internal/db/schema.sql")
	doc := srctest.Read(t, "docs/schema.md")
	baseline := map[string]bool{}
	for _, line := range strings.Split(srctest.Read(t, "scripts/schema_docs_baseline.txt"), "\n") {
		if line = strings.TrimSpace(line); line != "" && !strings.HasPrefix(line, "#") {
			baseline[line] = true
		}
	}
	tables := map[string]bool{}
	for _, m := range regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (aveloxis_(?:data|ops|scan)\.[a-z_0-9]+)`).FindAllStringSubmatch(sql, -1) {
		tables[m[1]] = true
	}
	srctest.MinCount(t, "tables in schema.sql", len(tables), 150)
	// A bare name that two schemas share (repos_fetch_log, working_commits)
	// is told apart the way the document does: aveloxis_data's heading is
	// bare, every other schema's carries its `(schema)` suffix — a bare
	// heading must not count for both (L10 round 1 of 0.29.126, F4).
	schemasOf := map[string]int{}
	for table := range tables {
		schemasOf[table[strings.Index(table, ".")+1:]]++
	}
	documented := func(table string) bool {
		schema, name := table[:strings.Index(table, ".")], table[strings.Index(table, ".")+1:]
		if schemasOf[name] > 1 && schema != "aveloxis_data" {
			return strings.Contains(doc, "\n#### "+name+" ("+schema+")\n")
		}
		return strings.Contains(doc, "\n#### "+name+"\n") || strings.Contains(doc, "\n#### "+name+" (") || strings.Contains(doc, "\n#### "+table+"\n")
	}
	var missing, stale []string
	for table := range tables {
		switch {
		case documented(table) && baseline[table]:
			stale = append(stale, table)
		case !documented(table) && !baseline[table]:
			missing = append(missing, table)
		}
	}
	for table := range baseline {
		if !tables[table] {
			stale = append(stale, table+" (no longer in schema.sql)")
		}
	}
	sort.Strings(missing)
	sort.Strings(stale)
	for _, table := range missing {
		t.Errorf("%s has no `#### %s` section in docs/schema.md: document its columns with their Source (schema-migrations.md step 8); the baseline takes no new table", table, table[strings.Index(table, ".")+1:])
	}
	for _, table := range stale {
		t.Errorf("%s is in scripts/schema_docs_baseline.txt but no longer needs to be: remove the line (the baseline only shrinks)", table)
	}
	if !strings.Contains(doc, "\n#### page_cache_replay (aveloxis_ops)\n") {
		t.Error("docs/schema.md lost the page_cache_replay section (the finding that introduced this test)")
	}
}
