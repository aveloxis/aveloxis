// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.26.3 source-contract pins for the single-pass event feed. See
// internal/platform/github/repo_events_single_pass_test.go for the
// mechanism; these pins make the structure durable:
//   - staged collectEvents consumes ONE ListRepoEvents stream
//   - the GitHub client no longer exposes the two-pass iterators at all
//   - GitLab keeps its two per-target-type iterators (DISTINCT URLs —
//     no aliasing) and composes them into ListRepoEvents

import (
	"os"
	"strings"
	"testing"
)

func TestStagedCollectEventsUsesSinglePass(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, ".ListRepoEvents(") {
		t.Error("staged.go collectEvents must consume client.ListRepoEvents — the " +
			"single-pass tagged-union event feed (v0.26.3)")
	}
	for _, old := range []string{".ListIssueEvents(", ".ListPREvents("} {
		if strings.Contains(code, old) {
			t.Errorf("staged.go must NOT call %s — two sequential paginations of the "+
				"same GitHub endpoint self-alias through the ETag cache and silently "+
				"drop the second kind's entire history on quiet repos (2026-07-09: "+
				"209 production repos with 50+ PRs and zero PR events)", old)
		}
	}
}

func TestGitHubClientHasNoTwoPassEventIterators(t *testing.T) {
	src, err := os.ReadFile("../platform/github/client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, old := range []string{
		"func (c *Client) ListIssueEvents(",
		"func (c *Client) ListPREvents(",
	} {
		if strings.Contains(code, old) {
			t.Errorf("github/client.go must not define %s — both kinds come from the "+
				"same /issues/events feed; only the single-pass ListRepoEvents may "+
				"exist so the ETag aliasing cannot be reintroduced", old)
		}
	}
	if !strings.Contains(code, "func (c *Client) ListRepoEvents(") {
		t.Error("github/client.go must define ListRepoEvents")
	}
}

func TestGitLabClientComposesRepoEvents(t *testing.T) {
	src, err := os.ReadFile("../platform/gitlab/client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "func (c *Client) ListRepoEvents(") {
		t.Error("gitlab/client.go must define ListRepoEvents (composing its two " +
			"per-target-type endpoint iterations — distinct URLs, no aliasing)")
	}
	// GitLab legitimately keeps the two underlying iterators: they hit
	// DIFFERENT URLs (?target_type=issue vs merge_request).
	for _, keep := range []string{
		"func (c *Client) ListIssueEvents(",
		"func (c *Client) ListPREvents(",
	} {
		if !strings.Contains(code, keep) {
			t.Errorf("gitlab/client.go should keep %s as the underlying per-target "+
				"fetcher that ListRepoEvents composes", keep)
		}
	}
}

func TestPlatformInterfaceDeclaresSinglePassEvents(t *testing.T) {
	src, err := os.ReadFile("../platform/platform.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	if !strings.Contains(code, "ListRepoEvents(ctx context.Context, owner, repo string, since time.Time) iter.Seq2[RepoEvent, error]") {
		t.Error("platform.Client must declare ListRepoEvents returning the RepoEvent union")
	}
	for _, old := range []string{
		"ListIssueEvents(ctx context.Context",
		"ListPREvents(ctx context.Context",
	} {
		if strings.Contains(code, old) {
			t.Errorf("platform.Client must NOT declare %s — the interface offers only "+
				"the single-pass feed so no consumer can recreate the two-pass ETag trap", old)
		}
	}
	if !strings.Contains(code, "type RepoEvent struct") {
		t.Error("platform must define the RepoEvent tagged union")
	}
}
