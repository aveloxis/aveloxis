// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.79: heal migration for the RESOURCE_LIMITS_EXCEEDED activity
// incident (216,000 contributors stamped "checked" with zero data).

package db

import (
	"os"
	"strings"
	"testing"
)

func TestActivityCheckHealMigration(t *testing.T) {
	b, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	label := "v0.27.79 re-null activity-check stamps from the resource-limits incident"
	idx := strings.Index(src, label)
	if idx < 0 {
		t.Fatal("the v0.27.79 activity-check heal migration must exist")
	}
	region := src[idx:min(idx+1200, len(src))]
	for _, needle := range []string{
		"SET gh_activity_checked_at = NULL",
		// Self-disabling time bound — WITHOUT it, legitimately-dataless
		// rows stamped by the FIXED code (deleted users via mark-only)
		// would be re-nulled on every migrate run forever.
		"TIMESTAMPTZ '2026-08-03",
		// Only rows with zero data everywhere — a classified row must
		// never lose its stamp.
		`COALESCE(gh_activity_class, '') = ''`,
		"COALESCE(gh_public_contribs_year, 0) = 0",
		"COALESCE(gh_restricted_contribs_year, 0) = 0",
		"COALESCE(gh_last_contribution_year, 0) = 0",
	} {
		if !strings.Contains(region, needle) {
			t.Errorf("heal migration missing %q", needle)
		}
	}
}
