// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.76 (Copilot review, PR #171): handleContributorActivity used
// to map EVERY store error to 404, which turned DB outages and query
// bugs into "contributor not found" — invisible to operators and
// wrong to callers. This pin keeps the split: 404 exclusively for the
// genuine no-rows lookup, logged 500 for everything else.

import (
	"strings"
	"testing"
)

func TestContributorActivityDistinguishesNotFoundFromFailure(t *testing.T) {
	src := mustReadFile(t, "contributor_elsewhere.go")
	body := extractFuncBody(t, src, "handleContributorActivity")
	for _, needle := range []string{
		"errors.Is(err, pgx.ErrNoRows)",
		"http.StatusNotFound",
		"http.StatusInternalServerError",
		"s.logger.Error(",
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("handleContributorActivity must contain %q — 404 only for the genuine no-rows case, logged 500 for operational failures", needle)
		}
	}
	// The 404 must be gated INSIDE the ErrNoRows branch, not the bare
	// error path: the not-found write has to appear AFTER the errors.Is
	// check in source order.
	if is, nf := strings.Index(body, "errors.Is(err, pgx.ErrNoRows)"), strings.Index(body, "http.StatusNotFound"); is < 0 || nf < is {
		t.Error("the StatusNotFound write must sit inside the errors.Is(err, pgx.ErrNoRows) branch")
	}
}
