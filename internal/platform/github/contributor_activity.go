// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
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
// one GraphQL query.
//
// NOT 100: contributionsCollection is resource-capped independently of
// the rate-limit point cost. The FetchIssueClosers precedent (100
// aliases) does NOT transfer — at 100 (and 50, and 40) GitHub answers
// EVERY alias with a per-path RESOURCE_LIMITS_EXCEEDED ("Resource
// limits for this query exceeded") and null nodes, which is how
// production stamped 216,000 contributors "checked, no data" on
// 2026-07-30/31 before v0.27.79. Live probe 2026-08-02 with real
// fleet logins: 40 → all fail, 35 → all succeed.
//
// v0.27.81: the edge MOVES — production RLE'd at 25 aliases on
// 2026-08-04 (query cost depends on how dense the batched accounts
// are, not just the alias count), which wedged classification: the
// deterministic NULL-first claim re-presented the same 2,500
// contributors every tick and the fixed-size fetch failed identically
// each time. No fixed batch size is safe, so the size is now just the
// STARTING point for fetchActivityWithSubdivide, which halves on
// RESOURCE_LIMITS_EXCEEDED down to size 1.
const contributorActivityBatchSize = 25

// FetchContributorActivity returns trailing-year contribution summaries
// for the given logins, keyed by login. Deleted/renamed users (per-path
// NOT_FOUND → null node) are ABSENT from the map — absence means
// "unknown", while presence with zeros means "confirmed quiet"; the
// activity classification depends on that distinction. Empty logins are
// skipped.
//
// v0.27.81 — accounts whose contributionsCollection is too expensive
// to resolve even ALONE (actions-user / ghost-class machine accounts)
// are also ABSENT: they log a WARN and ride the scheduler's existing
// absent→mark-only path, which stamps them checked without a class
// ('' = unknown) and retires them from the claim head. That reuse is
// deliberate — retrying an at-cap account next tick can never make it
// cheaper (the scancode at-cap lesson). The one shape that still
// FAILS the whole fetch is a full chunk where EVERY account skipped
// at size 1: 25 consecutive individually-unresolvable accounts is not
// a plausible account property, it's the systemic incident signature
// (2026-07-30/31: GitHub rejecting everything), and per the v0.27.79
// contract a resource-limit condition must never become an
// empty-but-successful result that mark-stamps the batch dataless.
func (c *Client) FetchContributorActivity(ctx context.Context, logins []string) (map[string]model.ContributionActivity, error) {
	// Subdivision is this function's retry strategy, so cap the inner
	// GraphQL retry budget (the 2026-08-04 pilot measured a single
	// dense-account query burning ~7 minutes of the full 10-retry
	// backoff chain before subdivision could even start).
	ctx = platform.WithGraphQLFastFail(ctx)
	out := make(map[string]model.ContributionActivity, len(logins))
	flush := func(batch []string) error {
		skipped, lastSkipErr, err := c.fetchActivityWithSubdivide(ctx, batch, out)
		if err != nil {
			return err
		}
		if skipped == len(batch) && skipped > 0 {
			return fmt.Errorf("contributor activity: all %d aliases in chunk unresolvable at size 1 — systemic resource-limit condition, refusing to mark the batch dataless: %w", skipped, lastSkipErr)
		}
		return nil
	}
	var batch []string
	for _, l := range logins {
		if l == "" {
			continue
		}
		batch = append(batch, l)
		if len(batch) == contributorActivityBatchSize {
			if err := flush(batch); err != nil {
				return out, err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		if err := flush(batch); err != nil {
			return out, err
		}
	}
	return out, nil
}

// fetchActivityWithSubdivide wraps fetchActivityBatch with the
// v0.27.81 transient-failure halving (the fetchPRBatchWithSubdivide
// pattern). ClassTransient covers BOTH observed shapes of "this query
// is too expensive for GitHub's resolver": in-body
// RESOURCE_LIMITS_EXCEEDED (the 18:51/19:06 UTC production wedge on
// 2026-08-04) and deterministic 500s until retry exhaustion (the same
// day's pilot — one dense sub-batch drew 10/10 server errors over ~7
// minutes; the history worker's actions-user windows show the
// identical shape). RATE_LIMITED (temporal, not an account property)
// and auth failures bubble unchanged so the scheduler's
// marks-nothing-retry-next-tick contract applies. At size 1 a
// transient failure means the account itself exceeds what GitHub will
// resolve: WARN, count it skipped, move on. Recursion depth is
// bounded by log2(contributorActivityBatchSize).
func (c *Client) fetchActivityWithSubdivide(ctx context.Context, logins []string, out map[string]model.ContributionActivity) (skipped int, lastSkipErr error, err error) {
	if len(logins) == 0 {
		return 0, nil, nil
	}
	batchErr := c.fetchActivityBatch(ctx, logins, out)
	if batchErr == nil {
		return 0, nil, nil
	}
	if platform.ClassifyError(batchErr) != platform.ClassTransient {
		return 0, nil, batchErr
	}
	if len(logins) == 1 {
		c.logger.Warn("contributor activity: account unresolvable even alone (resource limits or persistent server errors) — skipping (scheduler will mark-only stamp it)",
			"login", logins[0], "error", batchErr)
		return 1, batchErr, nil
	}
	mid := len(logins) / 2
	leftSkipped, leftErr, err := c.fetchActivityWithSubdivide(ctx, logins[:mid], out)
	if err != nil {
		return leftSkipped, leftErr, err
	}
	rightSkipped, rightErr, err := c.fetchActivityWithSubdivide(ctx, logins[mid:], out)
	lastSkipErr = rightErr
	if lastSkipErr == nil {
		lastSkipErr = leftErr
	}
	return leftSkipped + rightSkipped, lastSkipErr, err
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
