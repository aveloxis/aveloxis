// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// TestExplorerNewContributorsGuardsMalformedAuthorDate pins the v0.25.10
// guard: the commit branch of explorer_new_contributors must filter
// cmt_author_date to well-formed ISO dates before to_timestamp() runs.
//
// Without it, a single commit whose cmt_author_date is the literal '%aI'
// (collected by a git < 2.2.0 that doesn't expand the strict-ISO placeholder)
// aborts the ENTIRE matview build with:
//
//	ERROR: invalid value "%aI" for "YYYY" (SQLSTATE 22007)
//
// observed on a production rebuild 2026-06-03. The matview must be resilient
// to one poisoned row out of ~474M, not fail the whole build.
func TestExplorerNewContributorsGuardsMalformedAuthorDate(t *testing.T) {
	src := readMatviewsSQLForV0255(t)

	// The regex guard must be present.
	if !strings.Contains(src, `cmt_author_date ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'`) {
		t.Error("explorer_new_contributors commit branch must guard cmt_author_date " +
			"with `~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'` so malformed values (e.g. the " +
			"literal '%aI' from an old git) can't abort the matview build (SQLSTATE 22007).")
	}

	// The guard must sit in the same vicinity as the to_timestamp call it
	// protects — i.e. both reference cmt_author_date. Sanity check that the
	// to_timestamp parse is still present (we guard it, not remove it).
	if !strings.Contains(src, "to_timestamp(co.cmt_author_date") {
		t.Error("explorer_new_contributors must still parse cmt_author_date via to_timestamp")
	}
}
