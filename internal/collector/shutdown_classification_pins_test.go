// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// NOTE: the per-arm classification pin that lived here is GONE. The
// L10 pass proved it decorative — it asserted the classification TOKEN
// existed, so keeping the `if errors.Is(…)` and replacing its `return`
// with a bare `continue` escaped it while restoring the defect — and
// its derived site list silently excluded any arm whose verb was not
// upsert/insert/clear/set (it missed SetPRMetaLinks) or whose log was
// line-wrapped. scripts/shutdown_classification_test.go supersedes it
// repo-wide and asserts the real property: the log must be UNREACHABLE
// when the error is canceled. One strong gate beats two weak ones.

// RefreshOpenItems must not report a shutdown as a completed refresh:
// refreshIssues/refreshPRs return their COUNT on a cancel (the staging
// batch went unflushed), so without a ctx check the caller logged
// "open issues refreshed" and started the PR half anyway.
func TestRefreshOpenItemsDoesNotLogSuccessAfterShutdown(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/collector/refresh_open.go"),
		"func (r *OpenItemRefresher) RefreshOpenItems("))

	for _, tc := range []struct{ call, log string }{
		{"r.refreshIssues(ctx, repoID, owner, repo, openIssues)", `r.logger.Info("open issues refreshed"`},
		{"r.refreshPRs(ctx, repoID, owner, repo, openPRs)", `r.logger.Info("open PRs refreshed"`},
	} {
		at := strings.Index(body, tc.call)
		logAt := strings.Index(body, tc.log)
		if at < 0 || logAt < 0 {
			t.Fatalf("could not anchor %q (%d) / %q (%d) — re-anchor this pin", tc.call, at, tc.log, logAt)
		}
		// The guard must RETURN, not merely exist: the L10 pass escaped
		// the presence form by replacing the return with a Debug log,
		// which restores the "open issues refreshed" lie AND starts the
		// PR half on a dead ctx. (This success-log shape is outside the
		// scripts/ ratchet, which audits Warn/Error failure logs.)
		guard := strings.Index(body[at:logAt], "ctx.Err() != nil")
		if guard < 0 {
			t.Errorf("%s: a ctx.Err() check must sit between the helper and %s — "+
				"the helper returns its count on a cancel, so shutdown was logged as a completed refresh",
				tc.call, tc.log)
			continue
		}
		if !strings.Contains(body[at+guard:logAt], "return") {
			t.Errorf("%s: the ctx.Err() guard before %s must RETURN — a guard that only logs "+
				"still falls through to the success line and starts the next half on a dead ctx",
				tc.call, tc.log)
		}
	}
}
