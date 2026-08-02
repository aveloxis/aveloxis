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

// contributor_activity.go — batched contributionsCollection fetch
// (v0.27.57). The GraphQL contributionsCollection is the only API
// surface that separates "publicly active" from "privately active but
// disclosed" (restrictedContributionsCount — the profile page's "N
// contributions in private repositories" line) from "quiet". The REST
// events feed the breadth worker uses returns an empty list for all of
// those states indistinguishably.
//
// GITHUB-ONLY by nature: GitLab has no restricted-contributions
// equivalent (private profiles are simply invisible), so this lives on
// *Client and is consumed through a narrow capability interface at the
// scheduler — NOT on platform.Client.

// contributorActivityBatchSize is how many aliased user() lookups ride
// one GraphQL query. 100 matches the FetchIssueClosers precedent and
// costs ~1 rate-limit point per query — a full 2.4M-contributor sweep
// is ~24K queries against a 5K-points/hour/token budget.
const contributorActivityBatchSize = 100

// FetchContributorActivity returns trailing-year contribution summaries
// for the given logins, keyed by login. Deleted/renamed users (per-path
// NOT_FOUND → null node) are ABSENT from the map — absence means
// "unknown", while presence with zeros means "confirmed quiet"; the
// activity classification depends on that distinction. Empty logins are
// skipped.
func (c *Client) FetchContributorActivity(ctx context.Context, logins []string) (map[string]model.ContributionActivity, error) {
	out := make(map[string]model.ContributionActivity, len(logins))
	var batch []string
	for _, l := range logins {
		if l == "" {
			continue
		}
		batch = append(batch, l)
		if len(batch) == contributorActivityBatchSize {
			if err := c.fetchActivityBatch(ctx, batch, out); err != nil {
				return out, err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := c.fetchActivityBatch(ctx, batch, out); err != nil {
			return out, err
		}
	}
	return out, nil
}

type activityNode struct {
	Login                   string `json:"login"`
	ContributionsCollection struct {
		RestrictedContributionsCount int   `json:"restrictedContributionsCount"`
		ContributionYears            []int `json:"contributionYears"`
		ContributionCalendar         struct {
			TotalContributions int `json:"totalContributions"`
		} `json:"contributionCalendar"`
	} `json:"contributionsCollection"`
}

func (c *Client) fetchActivityBatch(ctx context.Context, logins []string, out map[string]model.ContributionActivity) error {
	var b strings.Builder
	b.WriteString("query {")
	for i, login := range logins {
		fmt.Fprintf(&b, `
  u%d: user(login: %q) { login contributionsCollection { restrictedContributionsCount contributionYears contributionCalendar { totalContributions } } }`, i, login)
	}
	b.WriteString(" }")

	// The GraphQL helper logs per-path errors (NOT_FOUND for deleted
	// users) at WARN and still returns the data for the other aliases —
	// their nodes arrive as null and are skipped below.
	var resp map[string]json.RawMessage
	if err := c.http.GraphQL(ctx, b.String(), nil, &resp); err != nil {
		return fmt.Errorf("contributor activity batch: %w", err)
	}
	for i, login := range logins {
		raw, ok := resp[fmt.Sprintf("u%d", i)]
		if !ok || string(raw) == "null" {
			continue // deleted/renamed → absent from the result
		}
		var node activityNode
		if err := json.Unmarshal(raw, &node); err != nil {
			continue
		}
		out[login] = model.ContributionActivity{
			Login:             login,
			CalendarTotal:     node.ContributionsCollection.ContributionCalendar.TotalContributions,
			Restricted:        node.ContributionsCollection.RestrictedContributionsCount,
			ContributionYears: node.ContributionsCollection.ContributionYears,
		}
	}
	return nil
}
