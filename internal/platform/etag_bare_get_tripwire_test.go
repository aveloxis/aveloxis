// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestForgeClientsNeverIssueBareConditionalGets — v0.28.17 class-kill
// companion to the ETag-free GetJSON: inside the forge clients, every
// direct http.Get is a single-object body or header reader (listings go
// through Paginate*), and none of them can use a 304 — the email
// searches would report "search failed" on a repeat query, and GitLab's
// countGitLabResource returns 0 on ANY error, silently zeroing a repo's
// metadata counts. So a bare `c.http.Get(ctx, …)` in github/ or gitlab/
// is banned; readers pass platform.WithoutETag(ctx). Comment-stripped.
func TestForgeClientsNeverIssueBareConditionalGets(t *testing.T) {
	root := srctest.Root(t)
	bare := regexp.MustCompile(`\.http\.Get\(ctx\s*,`)
	scanned := 0
	for _, dir := range []string{"internal/platform/github", "internal/platform/gitlab"} {
		files, err := filepath.Glob(filepath.Join(root, dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			scanned++
			rel, _ := filepath.Rel(root, f)
			src := srctest.StripGoComments(srctest.Read(t, rel))
			for i, line := range strings.Split(src, "\n") {
				if bare.MatchString(line) {
					t.Errorf("%s:%d issues a bare conditional Get — single-object readers cannot use a 304; use c.http.Get(platform.WithoutETag(ctx), …) or GetJSON", rel, i+1)
				}
			}
		}
	}
	if scanned < 10 {
		t.Fatalf("scanned only %d forge-client files — the glob broke", scanned)
	}
}
