// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package srctest is the shared engine for the repo's source-contract
// tests (v0.27.118 — the regression-infrastructure plan). Before it,
// ~40 near-identical read helpers, 23 function-body extractors (five
// incompatible variants with DIFFERENT scan windows), and duplicated
// comment strippers were scattered across ~385 test files; two review
// rounds found real coverage holes caused by exactly that duplication.
//
// Rules:
//   - stdlib-only imports (no import cycles possible, zero go.mod cost);
//   - imported ONLY from *_test.go files — a self-guard tripwire
//     (selfguard_test.go) fails the build if production code imports it,
//     and `go list -deps ./cmd/aveloxis` must never contain it;
//   - every helper carries its historical bug class as a negative
//     fixture in srctest_test.go. Fix helpers, never fixtures.
//
// Adoption is STRANGLER-ONLY: new source-contract tests use this
// package; legacy tests migrate when touched (tracked by the ratchet
// baseline in scripts/); there is deliberately NO bulk migration — see
// docs/contributing/testing.md.
package srctest

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

var rootOnce = sync.OnceValues(func() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", os.ErrNotExist
		}
		dir = parent
	}
})

// Root returns the absolute repository root (the directory containing
// go.mod), found by walking up from the working directory. Memoized —
// each test binary runs with its package directory as CWD, so the walk
// resolves identically for every test in the binary.
func Root(t testing.TB) string {
	t.Helper()
	root, err := rootOnce()
	if err != nil {
		t.Fatalf("srctest.Root: cannot locate go.mod above the working directory: %v", err)
	}
	return root
}

// Read returns the contents of the file at a REPO-ROOT-RELATIVE path,
// fataling on error. It exists to kill the "../../docs/..." fragility
// for cross-package reads; package-local files may keep using plain
// os.ReadFile.
func Read(t testing.TB, repoRelPath string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(Root(t), filepath.FromSlash(repoRelPath)))
	if err != nil {
		t.Fatalf("srctest.Read(%s): %v", repoRelPath, err)
	}
	return string(b)
}

// PackageFiles returns repo-root-relative path → source for every
// non-test .go file directly in repoRelDir (non-recursive — the
// flagship column-writer tripwire's corpus rules). The self-check
// guard is a REQUIRED parameter: fewer than minFiles read is a fatal
// "my corpus walk broke", never a silent vacuous pass.
func PackageFiles(t testing.TB, repoRelDir string, minFiles int) map[string]string {
	t.Helper()
	dir := filepath.Join(Root(t), filepath.FromSlash(repoRelDir))
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("srctest.PackageFiles(%s): %v", repoRelDir, err)
	}
	out := map[string]string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("srctest.PackageFiles: read %s: %v", name, err)
		}
		out[repoRelDir+"/"+name] = string(b)
	}
	MinCount(t, "non-test .go files in "+repoRelDir, len(out), minFiles)
	return out
}

// FuncBody returns the source of the function or method whose
// declaration contains sig (e.g. "func (s *PostgresStore) UpsertRepo("),
// from the DECLARATION through the MATCHING closing brace.
//
// This is THE one extractor (it replaced five incompatible variants
// whose scan windows disagreed): brace counting is string-, rune-, and
// comment-literal aware, so a brace inside a literal cannot desync the
// depth. Comments INSIDE the body are preserved (pins target them —
// compose with StripGoComments when a pin must not match comment text).
// Text AFTER the closing brace — trailing comments, the next function's
// doc comment — is excluded; the legacy cut-at-next-`func ` variants
// included it, which is a false-match window.
func FuncBody(t testing.TB, src, sig string) string {
	t.Helper()
	start := strings.Index(src, sig)
	if start < 0 {
		t.Fatalf("srctest.FuncBody: declaration %q not found", sig)
	}
	depth := 0
	opened := false
	i := start
	for i < len(src) {
		c := src[i]
		switch c {
		case '{':
			depth++
			opened = true
		case '}':
			depth--
			if opened && depth == 0 {
				return src[start : i+1]
			}
		case '/':
			if i+1 < len(src) {
				switch src[i+1] {
				case '/': // line comment
					if nl := strings.IndexByte(src[i:], '\n'); nl >= 0 {
						i += nl
					} else {
						i = len(src)
					}
				case '*': // block comment
					if end := strings.Index(src[i+2:], "*/"); end >= 0 {
						i += 2 + end + 1
					} else {
						i = len(src)
					}
				}
			}
		case '"', '\'': // interpreted string / rune literal
			q := c
			j := i + 1
			for j < len(src) {
				if src[j] == '\\' {
					j += 2
					continue
				}
				if src[j] == q || src[j] == '\n' {
					break
				}
				j++
			}
			i = j
		case '`': // raw string
			if end := strings.IndexByte(src[i+1:], '`'); end >= 0 {
				i += 1 + end
			} else {
				i = len(src)
			}
		}
		i++
	}
	t.Fatalf("srctest.FuncBody: unbalanced braces after %q", sig)
	return ""
}

// MinCount fatals when got < min, naming what was being counted — the
// standard "my own scan broke" guard so corpus-walking tests fail
// loudly instead of passing vacuously.
func MinCount(t testing.TB, what string, got, min int) {
	t.Helper()
	if got < min {
		t.Fatalf("srctest self-check: only %d %s found (need >= %d) — the scan itself broke; fix the test, do not trust this run", got, what, min)
	}
}
