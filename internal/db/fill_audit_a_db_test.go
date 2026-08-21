// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.103 — Workstream A of the 2026-08-19 fill audit, db-side pins.
package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// A3 — one-shot backfill for the 1,051,111 releases rows whose
// data_source was empty (GitHub ListReleases never set Origin).
// Self-disabling via the data_source = "" predicate.
func TestMigrationBackfillsReleasesDataSource(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	for _, needle := range []string{
		"backfill releases.data_source",
		"UPDATE aveloxis_data.releases",
		"COALESCE(rel.data_source, '') = ''",
	} {
		if !strings.Contains(s, needle) {
			t.Errorf("migrate.go must carry the releases data_source backfill (missing %q)", needle)
		}
	}
}

// A4 — the identity upsert's DO UPDATE refreshed only login/name/email,
// so pre-fix rows with empty node_id/user_type could NEVER heal on
// re-observation even after the mapper fix. Both contributors.go sites
// must refresh them prefer-nonempty (an empty re-observation must not
// clobber a captured value).
func TestIdentityUpsertRefreshesNodeIDAndType(t *testing.T) {
	src, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	nodeRe := regexp.MustCompile(`node_id\s*=\s*COALESCE\(NULLIF\(EXCLUDED\.node_id,\s*''\),\s*contributor_identities\.node_id\)`)
	typeRe := regexp.MustCompile(`user_type\s*=\s*COALESCE\(NULLIF\(EXCLUDED\.user_type,\s*''\),\s*contributor_identities\.user_type\)`)
	if got := len(nodeRe.FindAllString(s, -1)); got < 2 {
		t.Errorf("both contributor_identities upserts in contributors.go must refresh node_id prefer-nonempty (found %d)", got)
	}
	if got := len(typeRe.FindAllString(s, -1)); got < 2 {
		t.Errorf("both contributor_identities upserts in contributors.go must refresh user_type prefer-nonempty (found %d)", got)
	}
}
