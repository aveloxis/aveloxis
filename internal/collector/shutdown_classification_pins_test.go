// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Copilot round 8 on PR #191: the processor's child-upsert arms are
// warn-and-continue, so a `stop serve` landing mid-row emitted one WARN
// per remaining child — the v0.27.91 flood class. A PR with 200 commits
// produced 200 "failed to upsert PR commit" lines on a normal shutdown.
//
// The site list is DERIVED from the source, not hand-written: a new
// child-upsert arm added later joins the rule automatically instead of
// silently sitting outside a frozen list (the pass-24 lesson that
// Copilot round 7 cashed in on the ticker pin).
func TestEveryProcessorUpsertArmClassifiesShutdown(t *testing.T) {
	// Scope: the collection-path files whose store writes run under the
	// scheduler's cancellable job ctx. It is a SCOPE, not a site list —
	// the arms inside each file are derived, so a new one joins the rule
	// automatically. (Migration and one-shot-CLI files are deliberately
	// out: a WARN during `aveloxis migrate` is not a shutdown
	// misclassification.)
	files := []string{
		"internal/collector/staged.go",
		"internal/collector/analysis.go",
		"internal/collector/commit_resolver.go",
	}
	sites := 0
	for _, file := range files {
		sites += auditShutdownArms(t, file)
	}
	if sites < 15 {
		t.Fatalf("found only %d store-write failure arms across %v — the collection path moved; re-anchor this pin", sites, files)
	}
}

// auditShutdownArms checks every warn-and-continue store-write failure
// arm in one file and returns how many it found.
func auditShutdownArms(t *testing.T, file string) int {
	t.Helper()
	src := srctest.StripGoComments(srctest.Read(t, file))
	lines := strings.Split(src, "\n")

	warn := regexp.MustCompile(`\.logger\.Warn\("failed to (?:upsert|insert|clear|set) [^"]*",.*"error", (\w+)\)`)
	sites := 0
	for i, ln := range lines {
		m := warn.FindStringSubmatch(ln)
		if m == nil {
			continue
		}
		sites++
		errVar := m[1]
		// The classification must be one of the few lines directly
		// above, inside the same error arm — not merely somewhere in
		// the function (a neighbour's guard must not satisfy this one).
		lo := i - 6
		if lo < 0 {
			lo = 0
		}
		window := strings.Join(lines[lo:i], "\n")
		if !strings.Contains(window, "errors.Is("+errVar+", context.Canceled)") {
			t.Errorf("%s:%d %s: a warn-and-continue store-write arm must classify "+
				"errors.Is(%s, context.Canceled) and return before it logs — otherwise a "+
				"shutdown mid-pass emits one WARN per remaining item (v0.27.91 flood class)",
				file, i+1, strings.TrimSpace(ln), errVar)
		}
	}
	return sites
}

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
		if !strings.Contains(body[at:logAt], "ctx.Err() != nil") {
			t.Errorf("%s: a ctx.Err() check must sit between the helper and %s — "+
				"the helper returns its count on a cancel, so shutdown was logged as a completed refresh",
				tc.call, tc.log)
		}
	}
}
