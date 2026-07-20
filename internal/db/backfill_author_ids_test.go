// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.25 — pins for the BackfillCommitAuthorIDs fixes, after the
// live 2-day-2-hour orphaned run on aveloxis_large (2026-07-20).

import (
	"os"
	"strings"
	"testing"
)

func backfillSQL(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("commit_resolver_store.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, "func (s *PostgresStore) BackfillCommitAuthorIDs")
	if i < 0 {
		t.Fatal("BackfillCommitAuthorIDs not found")
	}
	body := s[i:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	return body
}

// TestBackfillExcludesEmptyStringsBothSides pins the v0.25.6 lesson
// applied here: Postgres ” = ” is TRUE, and production carries
// 10,636 contributors with gh_login = ”. Without BOTH guards, a repo
// whose unresolved commits include ”-username rows cross-products
// against every one of them inside the join — the shape behind the
// 2-day runtime. IS NOT NULL alone does not exclude ”.
func TestBackfillExcludesEmptyStringsBothSides(t *testing.T) {
	body := backfillSQL(t)
	for _, needle := range []string{
		`c.cmt_author_platform_username != ''`,
		`cn.gh_login != ''`,
	} {
		if !strings.Contains(body, needle) {
			t.Errorf("BackfillCommitAuthorIDs missing %s — '' = '' is TRUE and the empty-string groups cross-product (v0.25.6 class)", needle)
		}
	}
}

// TestBackfillKeepsCaseInsensitiveJoin — regression pin on Fix H
// (v0.20.12): the guards must not have reverted the join to
// case-sensitive equality, which silently drops case-variant logins.
func TestBackfillKeepsCaseInsensitiveJoin(t *testing.T) {
	if !strings.Contains(backfillSQL(t), "LOWER(c.cmt_author_platform_username) = LOWER(cn.gh_login)") {
		t.Error("BackfillCommitAuthorIDs must keep the case-insensitive LOWER = LOWER join (v0.20.12 Fix H)")
	}
}

// TestLowerGhLoginIndexDeclared pins the expression index in BOTH
// homes: schema.sql (fresh installs — plain form; CONCURRENTLY is
// illegal inside the schema exec's transaction) and migrate.go
// (existing fleets — CONCURRENTLY via execCreateIndexConcurrently).
// The v0.19.9 gh_login index CANNOT serve the LOWER() join; without
// this one the backfill seq-scans + sorts 2.3M contributors per repo
// and the planner has no expression statistics.
func TestLowerGhLoginIndexDeclared(t *testing.T) {
	for file, needles := range map[string][]string{
		"schema.sql": {
			"idx_contributors_gh_login_lower",
			"(LOWER(gh_login)) WHERE gh_login != ''",
		},
		"migrate.go": {
			"idx_contributors_gh_login_lower",
			"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_contributors_gh_login_lower",
		},
	} {
		src, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, needle := range needles {
			if !strings.Contains(string(src), needle) {
				t.Errorf("%s missing %q — the LOWER(gh_login) expression index serves BackfillCommitAuthorIDs' case-insensitive join", file, needle)
			}
		}
	}
}
