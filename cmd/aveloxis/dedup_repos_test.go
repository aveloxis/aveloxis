// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Source-contract tests for the `aveloxis dedup-repos` command wiring
// (v0.25.32). Mirrors the merge_cntrb_collisions_test.go style: pin the
// registration, flag surface, and db-layer call sites so a refactor
// can't silently disconnect the command from the store implementation.

package main

import (
	"os"
	"strings"
	"testing"
)

func readDedupReposSources(t *testing.T) string {
	t.Helper()
	var sb strings.Builder
	for _, f := range []string{"dedup_repos.go", "../../internal/db/repo_dedup.go"} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		sb.Write(b)
	}
	return sb.String()
}

func TestDedupReposCommandRegistered(t *testing.T) {
	mainSrc, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatalf("read main.go: %v", err)
	}
	if !strings.Contains(string(mainSrc), "dedupReposCmd(&cfgPath)") {
		t.Error("main.go must register dedupReposCmd in the root.AddCommand block — " +
			"without registration the operator cannot invoke `aveloxis dedup-repos`.")
	}
}

func TestDedupReposFlagSurface(t *testing.T) {
	src, err := os.ReadFile("dedup_repos.go")
	if err != nil {
		t.Fatalf("read dedup_repos.go: %v", err)
	}
	code := string(src)

	for _, needle := range []string{`"dry-run"`, `"batch-size"`, `"limit"`, `"dedup-repos"`} {
		if !strings.Contains(code, needle) {
			t.Errorf("dedup_repos.go must declare %s — the operator workflow is "+
				"`--dry-run` first, then a `--limit N` canary, then the full run.", needle)
		}
	}
}

func TestDedupReposCallsStoreFunctions(t *testing.T) {
	combined := readDedupReposSources(t)

	for _, fn := range []string{
		"CountCaseVariantRepoDups",
		"SampleCaseVariantRepoDups",
		"DedupCaseVariantReposBatch",
	} {
		if !strings.Contains(combined, fn) {
			t.Errorf("dedup-repos must be wired to db.%s — count → dry-run sample → "+
				"batched merge loop is the merge-cntrb-collisions command shape this "+
				"mirrors.", fn)
		}
	}
}

// v0.21.5 contract: only serve + migrate run store.Migrate.
func TestDedupReposDoesNotMigrate(t *testing.T) {
	src, err := os.ReadFile("dedup_repos.go")
	if err != nil {
		t.Fatalf("read dedup_repos.go: %v", err)
	}
	// Strip // line comments so the explanatory "intentionally NOT
	// called" comment at the call-site can't false-match the very
	// pattern it documents (same approach as
	// migrate_only_serve_and_migrate_test.go).
	var code strings.Builder
	for _, line := range strings.Split(string(src), "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code.WriteString(line)
		code.WriteString("\n")
	}
	if strings.Contains(code.String(), "store.Migrate(") {
		t.Error("dedup-repos must NOT call store.Migrate — per the v0.21.5 contract " +
			"only `serve` and `migrate` run schema migrations.")
	}
}
