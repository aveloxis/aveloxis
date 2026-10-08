// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// SR-2, made bidirectional (0.29.77 round 1: the two new covering indexes
// were declared plainly in schema.sql AND built CONCURRENTLY in migrate —
// the base DDL runs first, so an upgraded fleet would have block-built
// them with a SHARE lock on issues and pull_requests, and the CONCURRENTLY
// steps were no-ops; the fixed-name pins could not see it). Every index
// migrate builds CONCURRENTLY is derived from migrate.go itself; one that
// schema.sql also declares is a violation unless it is a legacy shape every
// fleet already had before the rule (the allowlist below, which only
// shrinks). A fresh install gets the CONCURRENTLY build from migrate — an
// instant on an empty table — so schema.sql needs no plain form. Scope
// (stated, round 3): every `CREATE [UNIQUE] INDEX CONCURRENTLY IF NOT
// EXISTS <name>` LITERAL in every non-test file of this package. The three
// helpers that build names through fmt.Sprintf from a table
// (fkExtraIndexes in fk_indexes_extra.go, repo_group_fk_indexes.go,
// email_message_fk_indexes.go) are outside this scan: their v0.20.x names
// are a legacy dual-path set of their own — repo_group_fk_indexes_test.go
// pins the repo-group names OUT of schema.sql, TestSchemaDeclaresExtraFKIndexes
// pins fkExtraIndexes INTO it (the dual path is that helper's pinned
// contract), and the email_message helper has no schema pin — so a new
// entry added to one of those tables AND to schema.sql would escape this
// pin (the 0.29.77 ledger records the decision still to take: keep or end
// the dual path for fkExtraIndexes, and a pin for the email_message helper).
var legacyIndexesInBothPlaces = map[string]bool{
	"idx_clh_cntrb": true, "idx_clh_login": true, "idx_commits_cmt_ght_author_id": true,
	"idx_contributors_activity_checked": true, "idx_contributors_canonical_lookup": true,
	"idx_contributors_email_lookup": true, "idx_contributors_gh_login_lower": true,
	"idx_contributors_history_backfilled": true, "idx_contributors_last_breadth": true,
	"idx_em_proj_pending_keyed": true, "idx_em_proj_pending_threaded": true,
	"idx_email_message_linked_issue": true, "idx_email_message_linked_pr": true,
	"idx_email_message_linked_review": true, "idx_email_message_thread_root": true,
	"idx_lockfile_packages_pkg": true, "idx_messages_node_id": true,
	"idx_pull_request_review_message_ref_msg_id": true, "idx_staging_repo_id": true,
}

func TestConcurrentlyBuiltIndexesAreNotDeclaredInSchemaSQL(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var sources strings.Builder
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		sources.WriteString(readFileForTest(t, f))
		sources.WriteString("\n")
	}
	schema := readFileForTest(t, "schema.sql")
	concurrently := regexp.MustCompile(`CREATE (?:UNIQUE )?INDEX CONCURRENTLY IF NOT EXISTS (\w+)`).FindAllStringSubmatch(sources.String(), -1)
	plain := regexp.MustCompile(`CREATE (?:UNIQUE )?INDEX IF NOT EXISTS (\w+)`).FindAllStringSubmatch(schema, -1)
	declared := map[string]bool{}
	for _, m := range plain {
		declared[m[1]] = true
	}
	if len(concurrently) < 50 {
		t.Fatalf("expected the package's CONCURRENTLY builds to be scanned, found %d", len(concurrently))
	}
	seen := map[string]bool{}
	var both []string
	for _, m := range concurrently {
		name := m[1]
		if declared[name] && !seen[name] {
			seen[name] = true
			both = append(both, name)
		}
	}
	sort.Strings(both)
	for _, name := range both {
		if !legacyIndexesInBothPlaces[name] {
			t.Errorf("%s is built CONCURRENTLY by migrate AND declared in schema.sql: the base DDL runs first, so an existing fleet block-builds it and the CONCURRENTLY step is a no-op (SR-2). Remove the schema.sql declaration; the CONCURRENTLY build covers fresh installs too", name)
		}
	}
	// The allowlist only shrinks: a name no longer in both places leaves it.
	for name := range legacyIndexesInBothPlaces {
		if !seen[name] {
			t.Errorf("legacyIndexesInBothPlaces names %s, which is no longer declared in both places — remove it from the list", name)
		}
	}
}
