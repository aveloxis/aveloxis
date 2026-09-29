// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// discardedWrite matches a write whose error is thrown away: a store method,
// an Exec on a pool, transaction or connection, or a file write.
var discardedWrite = regexp.MustCompile(`(?m)^\s*(?:_|_, _) = (?:[A-Za-z_][A-Za-z0-9_.]*\.store\.[A-Z][A-Za-z0-9]*\(|[A-Za-z_][A-Za-z0-9_.]*(?:\.pool|\.Pool\(\)|[Tt]x|[Cc]onn)\.Exec\(|os\.WriteFile\()`)

// TestNoDiscardedWriteErrors — old problems O2/O3 (2026-09-29, the
// CODING-STANDARDS conformance scan, rule ERR-1: "everything that errors is
// logged"): 17 writes discarded their error — a handler's group removal
// (the page redirected as if it had worked), the mailing-list and search
// resolvers' attempt stamps, the health status, the session-token cleanup,
// the migrate unlock, three best-effort releases on stop, and two file
// writes. A failed write must be logged or returned; best-effort is fine,
// silent is not.
func TestNoDiscardedWriteErrors(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "testdata" || d.Name() == "_build") {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			src := srctest.StripGoComments(string(b))
			rel, _ := filepath.Rel(root, path)
			for _, m := range discardedWrite.FindAllStringIndex(src, -1) {
				line := strings.Count(src[:m[0]], "\n") + 1
				t.Errorf("%s:%d: a write's error is discarded (%s) — log it or return it", rel, line, strings.TrimSpace(src[m[0]:m[1]]))
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files examined for discarded write errors", examined, 300)
}

func TestDiscardedWriteCorpus(t *testing.T) {
	for src, want := range map[string]bool{
		"\t_ = s.store.MarkSenderResolveAttempt(ctx, e)":         true,
		"\t_, _ = s.pool.Exec(ctx, `DELETE …`)":                  true,
		"\t_, _ = lockConn.Exec(context.Background(), `SELECT`)": true,
		"\t_ = os.WriteFile(p, b, 0o644)":                        true,
		"\t_ = w.store.ClearScancodeLock(relCtx, id)":            true,
		"\tdefer func() { _ = tx.Rollback(ctx) }()":              false,
		"\tif err := s.store.X(ctx); err != nil {":               false,
		"\t_ = sleepCtx(ctx, d)":                                 false,
	} {
		if got := discardedWrite.MatchString(src); got != want {
			t.Errorf("discardedWrite(%q) = %v; want %v", src, got, want)
		}
	}
}

// errorTextMatch is a decision taken on an error's TEXT.
var errorTextMatch = regexp.MustCompile(`strings\.(?:Contains|HasPrefix|HasSuffix|EqualFold)\([A-Za-z_][A-Za-z0-9_.]*\.Error\(\)`)

// TestNoDecisionsOnErrorText — old problem O4 (SR-5): the commit lookup
// read any error whose text contained "not found" as a definitive 404, and
// a 422 whose URL held those words was swallowed. Decide on the typed
// sentinel (errors.Is / errors.As), never on an error's text, in non-test
// Go.
func TestNoDecisionsOnErrorText(t *testing.T) {
	root := srctest.Root(t)
	examined := 0
	for _, dir := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && (d.Name() == "testdata" || d.Name() == "_build") {
				return filepath.SkipDir
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			b, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			examined++
			src := srctest.StripGoComments(string(b))
			rel, _ := filepath.Rel(root, path)
			for _, m := range errorTextMatch.FindAllStringIndex(src, -1) {
				t.Errorf("%s:%d: a decision on an error's text (%s) — use errors.Is/errors.As on a typed sentinel", rel, strings.Count(src[:m[0]], "\n")+1, src[m[0]:m[1]])
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files examined for error-text decisions", examined, 300)
}
