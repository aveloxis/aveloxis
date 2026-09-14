// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_schema_test.go — C2/C3 schema: the Jira subsystem's three
// tables + the issues.jira_issue_id column + platform row 4. Pattern:
// the mailing-list subsystem (registration row with claim lock +
// checkpoint; staging with nullable un-FK'd repo_id and a natural-key
// UNIQUE so re-staging is a no-op; identity mapping preserving raw
// identity when no unambiguous match exists — SR-6).
package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestSchemaDeclaresJiraTables(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	for _, needle := range []string{
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.jira_project_serve",
		"CREATE TABLE IF NOT EXISTS aveloxis_ops.jira_staging",
		"CREATE TABLE IF NOT EXISTS aveloxis_data.jira_identities",
		"(4, 'Jira')",
	} {
		if !strings.Contains(schema, needle) {
			t.Errorf("schema.sql missing %q", needle)
		}
	}
	// The staging natural key: one row per (project, issue, updated) —
	// re-staging a replayed sync window is a true no-op.
	if !strings.Contains(schema, "UNIQUE (project_key, issue_key, issue_updated)") {
		t.Error("jira_staging needs UNIQUE (project_key, issue_key, issue_updated)")
	}
	// jira_identities preserves the raw identity; cntrb_id is nullable
	// (SR-6: ambiguous/no-match stays NULL, never a guess).
	block := schema[strings.Index(schema, "CREATE TABLE IF NOT EXISTS aveloxis_data.jira_identities"):]
	block = block[:strings.Index(block, ");")]
	for _, needle := range []string{"jira_name", "jira_user_key", "display_name", "match_method"} {
		if !strings.Contains(block, needle) {
			t.Errorf("jira_identities missing column %q", needle)
		}
	}
	if !strings.Contains(block, "REFERENCES aveloxis_data.contributors(cntrb_id)") {
		t.Error("jira_identities.cntrb_id must FK contributors")
	}
}

func TestSchemaDeclaresJiraIssueIDColumn(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	block := schema[strings.Index(schema, "CREATE TABLE IF NOT EXISTS aveloxis_data.issues "):]
	block = block[:strings.Index(block, ");")]
	if !strings.Contains(block, "jira_issue_id") {
		t.Error("issues must declare jira_issue_id — the real Jira internal id lives in its OWN column; synthetics keep their negative platform_issue_id (never entering the GitHub id space — the review-#2 collision class)")
	}
	src := srctest.Read(t, "internal/db/migrate.go")
	if !strings.Contains(src, `"jira_issue_id"`) {
		t.Error("migrate.go must addColumnIfMissing issues.jira_issue_id")
	}
}

// TestJiraStagingRepoIDNotFKConstrained — the mailing-list precedent:
// issues can stage before the project's repo mapping exists, so
// repo_id is nullable and un-FK'd (the aveloxis_ops.staging
// repo_id-NOT-NULL FK is exactly why a third producer must NOT reuse
// that table).
func TestJiraStagingRepoIDNotFKConstrained(t *testing.T) {
	schema := srctest.Read(t, "internal/db/schema.sql")
	block := schema[strings.Index(schema, "CREATE TABLE IF NOT EXISTS aveloxis_ops.jira_staging"):]
	block = block[:strings.Index(block, ");")]
	for _, ln := range strings.Split(block, "\n") {
		if strings.Contains(ln, "repo_id") && strings.Contains(ln, "REFERENCES") {
			t.Errorf("jira_staging.repo_id must NOT carry an FK: %q", strings.TrimSpace(ln))
		}
	}
}
