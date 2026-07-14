// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
)

// FetchIssueClosers looks up "who closed this issue" for a batch of
// issue numbers via the per-issue timeline
// (timelineItems(itemTypes:[CLOSED_EVENT], last:1)) — which, unlike
// the repo-wide /issues/events feed, is NOT subject to GitHub's
// event-history pagination cap (the cap left 3.23M production issues
// with unreachable closers; see
// summary/identity-attribution-audit-2026-07-09.md, Phase 3).
//
// Issues are aliased into ONE GraphQL query (~100 per call ≈ ~3
// points). The returned map contains an entry only for issues with a
// non-null closer actor: deleted-user closers and never-closed issues
// are absent (operator decision: no data → report no data).
func (c *Client) FetchIssueClosers(ctx context.Context, owner, repo string, numbers []int) (map[int]model.UserRef, error) {
	out := make(map[int]model.UserRef, len(numbers))
	if len(numbers) == 0 {
		return out, nil
	}

	var b strings.Builder
	b.WriteString("query($owner: String!, $repo: String!) { repository(owner: $owner, name: $repo) {")
	for i, n := range numbers {
		fmt.Fprintf(&b, `
  i%d: issue(number: %d) { timelineItems(itemTypes: [CLOSED_EVENT], last: 1) { nodes { __typename ... on ClosedEvent { actor { __typename login avatarUrl url ... on User { databaseId name email } } } } } }`, i, n)
	}
	b.WriteString(" } }")

	var resp struct {
		Repository map[string]json.RawMessage `json:"repository"`
	}
	if err := c.http.GraphQL(ctx, b.String(), map[string]any{"owner": owner, "repo": repo}, &resp); err != nil {
		return nil, fmt.Errorf("issue closers batch: %w", err)
	}
	if resp.Repository == nil {
		return out, nil
	}

	type timeline struct {
		TimelineItems struct {
			Nodes []struct {
				Actor *struct {
					Login      string `json:"login"`
					AvatarURL  string `json:"avatarUrl"`
					URL        string `json:"url"`
					DatabaseID int64  `json:"databaseId"`
					Name       string `json:"name"`
					Email      string `json:"email"`
				} `json:"actor"`
			} `json:"nodes"`
		} `json:"timelineItems"`
	}
	for i, n := range numbers {
		raw, ok := resp.Repository[fmt.Sprintf("i%d", i)]
		if !ok || string(raw) == "null" {
			continue // issue deleted / inaccessible
		}
		var t timeline
		if err := json.Unmarshal(raw, &t); err != nil {
			continue
		}
		if len(t.TimelineItems.Nodes) == 0 {
			continue // never closed (or closed event pruned)
		}
		a := t.TimelineItems.Nodes[0].Actor
		if a == nil || a.Login == "" {
			continue // deleted closer — stays NULL
		}
		out[n] = model.UserRef{
			PlatformID: a.DatabaseID,
			Login:      a.Login,
			Name:       a.Name,
			Email:      a.Email,
			AvatarURL:  a.AvatarURL,
			URL:        a.URL,
		}
	}
	return out, nil
}
