// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.25.35 tripwire: the 2026-07-08 audit found the materialized-view
// count stated FOUR different wrong ways across the docs (22 / 19 / 18
// vs actual 20) and table counts frozen at "108 across two schemas"
// (actual: 129 across three — aveloxis_scan was missing everywhere).
// These tests compute the real counts from schema.sql / matviews.sql
// and pin every "<N> materialized views" / "<N> tables" phrase in the
// operator-facing docs to them, so adding a table or view forces the
// same-commit doc update (the v0.20.12 philosophy, extended to values).

package db

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// allDocsMarkdown returns every .md file under docs/ (excluding build
// artifacts) plus the top-level operator surfaces. v0.28.12: the
// 2026-08-23 docs audit found that every count a tripwire guarded was
// correct and every count in an UNGUARDED file was stale (overview.md
// said 84/24/19, quickstart said 108/19/two-schemas, ...) — so the
// guard now covers the whole published docs tree, not a hand list.
func allDocsMarkdown(t *testing.T) []string {
	t.Helper()
	paths := []string{"../../README.md", "../../docs/guide/commands.md"}
	err := filepath.WalkDir("../../docs", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "_build" || d.Name() == "__pycache__" || d.Name() == "logos" {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".md") {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) < 30 {
		t.Fatalf("docs walk found only %d files — the corpus scan broke", len(paths))
	}
	return paths
}

func schemaCounts(t *testing.T) (data, ops, scan, matviews int) {
	t.Helper()
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	data = strings.Count(string(schema), "CREATE TABLE IF NOT EXISTS aveloxis_data.")
	ops = strings.Count(string(schema), "CREATE TABLE IF NOT EXISTS aveloxis_ops.")
	scan = strings.Count(string(schema), "CREATE TABLE IF NOT EXISTS aveloxis_scan.")
	mv, err := os.ReadFile("matviews.sql")
	if err != nil {
		t.Fatal(err)
	}
	matviews = strings.Count(string(mv), "CREATE MATERIALIZED VIEW")
	if data == 0 || ops == 0 || scan == 0 || matviews == 0 {
		t.Fatalf("count scan broke: data=%d ops=%d scan=%d matviews=%d", data, ops, scan, matviews)
	}
	return data, ops, scan, matviews
}

var matviewPhraseRe = regexp.MustCompile(`(\d+) materialized views`)

func TestDocsMatviewCountsMatchSchema(t *testing.T) {
	_, _, _, matviews := schemaCounts(t)

	for _, path := range append(allDocsMarkdown(t),
		"../../cmd/aveloxis/main.go", // refresh-views / migrate help text
	) {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, m := range matviewPhraseRe.FindAllStringSubmatch(string(src), -1) {
			n, _ := strconv.Atoi(m[1])
			if n != matviews {
				t.Errorf("%s says %q but matviews.sql defines %d materialized views — "+
					"update the doc in the same commit as the view change.",
					path, m[0], matviews)
			}
		}
	}
}

var tablePhraseRe = regexp.MustCompile(`(\d+) tables`)

func TestDocsTableCountsMatchSchema(t *testing.T) {
	data, ops, scan, _ := schemaCounts(t)
	total := data + ops + scan
	valid := map[int]bool{data: true, ops: true, scan: true, total: true}

	for _, path := range allDocsMarkdown(t) {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		for _, m := range tablePhraseRe.FindAllStringSubmatch(string(src), -1) {
			n, _ := strconv.Atoi(m[1])
			if !valid[n] {
				t.Errorf("%s says %q but no schema matches that count (data=%d ops=%d "+
					"scan=%d total=%d) — stale table count.", path, m[0], data, ops, scan, total)
			}
		}
		// The primary operator surfaces must also acknowledge all
		// three schemas (not required of every docs page).
		if path == "../../README.md" || path == "../../docs/guide/commands.md" {
			if !strings.Contains(string(src), "aveloxis_scan") {
				t.Errorf("%s never mentions the aveloxis_scan schema — table counts that "+
					"omit it undercount by %d.", path, scan)
			}
		}
	}

	// CLAUDE.md's headline claim gets a coarse pin: it must not
	// understate the fleet ("108+ tables across two schemas" was stale).
	// CLAUDE.md is an internal dev doc that public release builds
	// deliberately exclude — its absence means "not a dev checkout",
	// not a failure. Any other read error still fails loudly.
	claude, err := os.ReadFile("../../CLAUDE.md")
	if os.IsNotExist(err) {
		t.Log("CLAUDE.md not present (release build) — skipping the dev-tree-only pin")
		return
	}
	if err != nil {
		t.Fatalf("read CLAUDE.md: %v", err)
	}
	if strings.Contains(string(claude), "across two schemas") {
		t.Error("CLAUDE.md still says \"across two schemas\" — there are three " +
			"(aveloxis_data, aveloxis_ops, aveloxis_scan).")
	}
	_ = fmt.Sprintf // keep fmt imported for future use in failure messages
}
