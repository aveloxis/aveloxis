package db

import (
	"os"
	"strings"
	"testing"
)

// TestAugurCompatSchema verifies that schema.sql creates the aveloxis_augur_data
// compatibility schema with views that map Augur column names to Aveloxis column names.
// This schema lets 8Knot and other Augur-era tools query using Augur conventions
// via search_path = aveloxis_augur_data, aveloxis_data.

func TestAugurCompatSchema_Exists(t *testing.T) {
	sql := readSchemaSQL(t)
	if !strings.Contains(sql, "CREATE SCHEMA IF NOT EXISTS aveloxis_augur_data") {
		t.Error("schema.sql must create the aveloxis_augur_data schema")
	}
}

func TestAugurCompatSchema_RepoView(t *testing.T) {
	sql := readSchemaSQL(t)
	// Must have a "repo" view (singular) pointing at "repos" (plural)
	// with repo_language alias for primary_language.
	if !strings.Contains(sql, "aveloxis_augur_data.repo") {
		t.Error("missing augur_data.repo compatibility view")
	}
	if !strings.Contains(sql, "repo_language") {
		t.Error("repo view must alias primary_language AS repo_language")
	}
}

func TestAugurCompatSchema_RepoInfoView(t *testing.T) {
	sql := readSchemaSQL(t)
	if !strings.Contains(sql, "aveloxis_augur_data.repo_info") {
		t.Error("missing repo_info compatibility view")
	}
	// Must alias star_count → stars_count and watcher_count → watchers_count.
	if !strings.Contains(sql, "stars_count") {
		t.Error("repo_info view must alias star_count AS stars_count")
	}
	if !strings.Contains(sql, "watchers_count") {
		t.Error("repo_info view must alias watcher_count AS watchers_count")
	}
}

func TestAugurCompatSchema_IssuesView(t *testing.T) {
	sql := readSchemaSQL(t)
	if !strings.Contains(sql, "aveloxis_augur_data.issues") {
		t.Error("missing issues compatibility view")
	}
	if !strings.Contains(sql, "gh_issue_number") {
		t.Error("issues view must alias issue_number AS gh_issue_number")
	}
	if !strings.Contains(sql, "gh_issue_id") {
		t.Error("issues view must alias platform_issue_id AS gh_issue_id")
	}
}

func TestAugurCompatSchema_PullRequestsView(t *testing.T) {
	sql := readSchemaSQL(t)
	if !strings.Contains(sql, "aveloxis_augur_data.pull_requests") {
		t.Error("missing pull_requests compatibility view")
	}
	if !strings.Contains(sql, "pr_src_number") {
		t.Error("pull_requests view must alias pr_number AS pr_src_number")
	}
	if !strings.Contains(sql, "pr_augur_contributor_id") {
		t.Error("pull_requests view must alias author_id AS pr_augur_contributor_id")
	}
	if !strings.Contains(sql, "pr_created_at") {
		t.Error("pull_requests view must alias created_at AS pr_created_at")
	}
	if !strings.Contains(sql, "pr_closed_at") {
		t.Error("pull_requests view must alias closed_at AS pr_closed_at")
	}
	if !strings.Contains(sql, "pr_merged_at") {
		t.Error("pull_requests view must alias merged_at AS pr_merged_at")
	}
}

func TestAugurCompatSchema_ReleasesView(t *testing.T) {
	sql := readSchemaSQL(t)
	if !strings.Contains(sql, "aveloxis_augur_data.releases") {
		t.Error("missing releases compatibility view")
	}
	if !strings.Contains(sql, "release_created_at") {
		t.Error("releases view must alias created_at AS release_created_at")
	}
	if !strings.Contains(sql, "release_published_at") {
		t.Error("releases view must alias published_at AS release_published_at")
	}
}

// Tables with identical columns should NOT have views in aveloxis_augur_data.
// They resolve via the second search_path entry (aveloxis_data) directly.
func TestAugurCompatSchema_NoUnnecessaryViews(t *testing.T) {
	sql := readSchemaSQL(t)
	// commits, contributors, repo_groups have identical columns — no views needed.
	unnecessaryViews := []string{
		"aveloxis_augur_data.commits",
		"aveloxis_augur_data.contributors",
		"aveloxis_augur_data.repo_groups",
		"aveloxis_augur_data.pull_request_events",
		"aveloxis_augur_data.repo_labor",
	}
	for _, v := range unnecessaryViews {
		if strings.Contains(sql, "VIEW "+v) {
			t.Errorf("unnecessary view %s — table has identical columns, should resolve via search_path fallback to aveloxis_data", v)
		}
	}
}

func readSchemaSQL(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("cannot read schema.sql: %v", err)
	}
	return string(data)
}
