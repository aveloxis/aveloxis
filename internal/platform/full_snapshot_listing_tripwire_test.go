// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round 5 on PR #191 (v0.28.18): a paginated 304 means "nothing
// new" only for an INCREMENTAL listing. A full-snapshot listing — every
// `since`-taking iterator called with a zero since: the gap filler's
// set-diff, a force-full recollect — has a STABLE URL, so the second
// walk in one process would be answered from the ETag cache with an
// empty result: the gap filler would report no gaps against an empty
// expected set, a force-full recollect would stage nothing. The forge
// clients enforce the rule at every such entry point (SR-18): when
// since is zero the walk runs under WithoutETag. Comment-stripped.
func TestFullSnapshotListingsBypassTheETagCache(t *testing.T) {
	sig := regexp.MustCompile(`func \(c \*Client\) (\w+)\(ctx context\.Context, owner, repo string, since time\.Time\)`)
	total := 0
	for _, file := range []string{"internal/platform/github/client.go", "internal/platform/gitlab/client.go"} {
		src := srctest.StripGoComments(srctest.Read(t, file))
		for _, m := range sig.FindAllStringSubmatchIndex(src, -1) {
			total++
			name := src[m[2]:m[3]]
			body := srctest.FuncBody(t, src, "func (c *Client) "+name+"(")
			head := body
			if len(head) > 600 {
				head = head[:600]
			}
			if !strings.Contains(srctest.NormalizeWS(head), "if since.IsZero() { ctx = platform.WithoutETag(ctx) }") {
				t.Errorf("%s: %s takes a since but does not bypass the ETag cache when since is zero (a full-snapshot listing must never read as a 304)", file, name)
			}
		}
	}
	if total < 14 {
		t.Fatalf("found only %d since-taking listings across both clients — the signature regex broke (v0.28.18 tree has 15)", total)
	}
}
