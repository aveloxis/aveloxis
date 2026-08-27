// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform_test

import (
	"io/fs"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestNoBareConditionalGetsOutsideThePaginator — the v0.28.17 rule, widened
// in v0.28.18 after a fresh-context L11 sweep found four readers the
// original forge-client-only glob never saw (the breadth worker's
// /user/{id} and /users/{login}/events readers and the commit resolver's
// /commits/{sha} and /users/{login} readers in internal/collector).
//
// The rule: `paginate` is the ONLY legitimate bare conditional reader —
// for a listing, 304 means "nothing new since the cached page", which the
// paginator turns into a clean empty result. Every OTHER direct
// `x.http.Get(...)` on a *platform.HTTPClient decodes a body or reads a
// header, and a body-decoding reader can never use a 304: on a repeat
// same-URL read inside one process it gets ErrNotModified (an error to
// its caller — "search failed", "events fetch failed") or an empty
// result (GitLab's countGitLabResource zeroed a repo's metadata counts).
// GetJSON is ETag-free by construction since v0.28.17; direct Gets must
// pass platform.WithoutETag(ctx) as their FIRST argument. Comment-stripped
// so prose cannot satisfy or trip it; the call may span lines.
func TestNoBareConditionalGetsOutsideThePaginator(t *testing.T) {
	root := srctest.Root(t)
	call := regexp.MustCompile(`(?s)\.http\.Get\(\s*([^,]+?)\s*,`)
	platformImport := regexp.MustCompile(`"github\.com/aveloxis/aveloxis/internal/platform"`)
	scanned, sites := 0, 0
	for _, dir := range []string{"internal", "cmd"} {
		err := filepath.WalkDir(filepath.Join(root, dir), func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel, _ := filepath.Rel(root, path)
			raw := srctest.Read(t, rel)
			if !platformImport.MatchString(raw) {
				return nil
			}
			scanned++
			src := srctest.StripGoComments(raw)
			for _, m := range call.FindAllStringSubmatchIndex(src, -1) {
				sites++
				arg := strings.TrimSpace(src[m[2]:m[3]])
				if strings.HasPrefix(arg, "platform.WithoutETag(") || strings.HasPrefix(arg, "WithoutETag(") {
					continue
				}
				line := strings.Count(src[:m[0]], "\n") + 1
				t.Errorf("%s:%d: bare conditional `.http.Get(%s, …)` — a body/header reader cannot use a 304; pass platform.WithoutETag(ctx) as the first argument (or route a listing through the paginator)", rel, line, arg)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	if scanned < 20 {
		t.Fatalf("scanned only %d platform-importing files — the walk broke", scanned)
	}
	if sites < 5 {
		t.Fatalf("found only %d direct .http.Get( sites — the regex broke (v0.28.18 tree has 7)", sites)
	}
}
