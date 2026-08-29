// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pass 31 (v0.28.18): every per-item child listing on both forges —
// `List<X>(ctx, owner, repo, <n> int)` — reads a truth set on refresh
// and gap fill; its pages are ascending with no since, so a new child on
// the LAST page hides behind a byte-stable, cached page 1 (304 ends the
// walk). Each such reader bypasses the ETag cache as its first statement.
func TestPerItemChildReadersBypassTheETagCache(t *testing.T) {
	sig := regexp.MustCompile(`func \(c \*Client\) (List\w+)\(ctx context\.Context, owner, repo string, \w+ int\)`)
	total := 0
	for _, file := range []string{"internal/platform/github/client.go", "internal/platform/gitlab/client.go"} {
		src := srctest.StripGoComments(srctest.Read(t, file))
		for _, m := range sig.FindAllStringSubmatchIndex(src, -1) {
			total++
			name := src[m[2]:m[3]]
			body := srctest.NormalizeWS(srctest.FuncBody(t, src, "func (c *Client) "+name+"("))
			guard := strings.Index(body, "ctx = platform.WithoutETag(ctx)")
			firstUse := -1
			for _, anchor := range []string{"platform.Paginate", "c.http", "c.List", "c.fetch"} {
				if j := strings.Index(body, anchor); j >= 0 && (firstUse < 0 || j < firstUse) {
					firstUse = j
				}
			}
			if guard < 0 || firstUse < 0 || guard > firstUse {
				t.Errorf("%s: %s is a per-item child reader and must bypass the ETag cache before its first client use (guard=%d firstUse=%d)", file, name, guard, firstUse)
			}
		}
	}
	if total < 20 {
		t.Fatalf("found only %d per-item readers across both clients — the signature regex broke (v0.28.18 tree has 22)", total)
	}
}
