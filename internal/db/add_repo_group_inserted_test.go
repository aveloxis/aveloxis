// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// TestAddRepoToGroupByIDReportsInsertion pins the 2026-08-18 signature:
// AddRepoToGroupByID returns (inserted bool, err error), derived from the
// command tag's RowsAffected. The INSERT is ON CONFLICT DO NOTHING, so a nil
// error alone cannot distinguish "new link" from "already linked" — which is
// how the org scan logged 9.3 MILLION bogus "new repos" (identical counts for
// the same orgs every 4h pass, e.g. kubernetes new_repos=79 for 8.8 days
// straight) in the Aug 7–16 production run.
func TestAddRepoToGroupByIDReportsInsertion(t *testing.T) {
	body := extractFunctionBody(t, "web_store.go", "AddRepoToGroupByID")

	if !strings.Contains(body, "RowsAffected()") {
		t.Error("AddRepoToGroupByID must derive its inserted-bool from the " +
			"command tag's RowsAffected() — ON CONFLICT DO NOTHING returns a " +
			"nil error whether or not a row was inserted")
	}
	// The signature itself: (inserted/linked bool, err error).
	src := readFileForTest(t, "web_store.go")
	if !strings.Contains(src, "func (s *PostgresStore) AddRepoToGroupByID(ctx context.Context, groupID, repoID int64) (bool, error)") {
		t.Error("AddRepoToGroupByID must return (bool, error) — the bool " +
			"reports whether a NEW user_repos link was inserted")
	}
}
