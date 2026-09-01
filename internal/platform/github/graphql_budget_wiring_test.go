// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Pins for the 2026-09-01 graphql-budget fixes (the pytorch incident).

// TestBackgroundSweepsCarryBackgroundBudget: the three background GraphQL
// fetchers must flag their contexts so key checkout leaves
// GraphQLBackgroundReserve headroom for foreground collection.
func TestBackgroundSweepsCarryBackgroundBudget(t *testing.T) {
	hist := srctest.Read(t, "internal/platform/github/contributor_history.go")
	for _, fn := range []string{"FetchContributorHistoryMeta", "FetchContributorDailyHistory"} {
		body := srctest.FuncBody(t, hist, "func (c *Client) "+fn+"(")
		if !strings.Contains(body, "platform.WithGraphQLBackgroundBudget(ctx)") {
			t.Errorf("%s must mark its ctx WithGraphQLBackgroundBudget — the history sweep's sustained load is what kept keys graphql-dry under pytorch's multi-day job", fn)
		}
	}
	act := srctest.Read(t, "internal/platform/github/contributor_activity.go")
	body := srctest.FuncBody(t, act, "func (c *Client) FetchContributorActivity(")
	if !strings.Contains(body, "platform.WithGraphQLBackgroundBudget(ctx)") {
		t.Error("FetchContributorActivity must mark its ctx WithGraphQLBackgroundBudget")
	}
}

// TestChildPaginationErrorCarriesForceFullNeedle: the pagination wrap must
// carry the "graphql PR batch" substring shouldForceFullRecollect keys on
// (v0.18.24) — pytorch's shard-41 pagination failure never armed
// force_full while its batch-fetch siblings did.
func TestChildPaginationErrorCarriesForceFullNeedle(t *testing.T) {
	src := srctest.Read(t, "internal/platform/github/graphql_pr_batch.go")
	if !strings.Contains(src, `"graphql PR batch: paginating children for PR #%d: %w"`) {
		t.Error(`the paginating-children wrap must start with "graphql PR batch: " (the shouldForceFullRecollect needle)`)
	}
	if strings.Contains(srctest.StripGoComments(src), `"paginating children for PR #%d: %w"`) {
		t.Error("the needle-less wrap form must not return")
	}
}
