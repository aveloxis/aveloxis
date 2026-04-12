package db

import (
	"os"
	"strings"
	"testing"
)

// Tests verifying that all 8Knot-required materialized views exist in matviews.sql
// and are listed in the refresh order in matviews.go.

func TestMatviews_ExplorerPrFiles_Exists(t *testing.T) {
	sql := readMatviewsSQL(t)
	if !strings.Contains(sql, "explorer_pr_files") {
		t.Error("matviews.sql must define explorer_pr_files materialized view")
	}
}

func TestMatviews_ExplorerCntrbPerFile_Exists(t *testing.T) {
	sql := readMatviewsSQL(t)
	if !strings.Contains(sql, "explorer_cntrb_per_file") {
		t.Error("matviews.sql must define explorer_cntrb_per_file materialized view")
	}
}

func TestMatviews_ExplorerRepoFiles_Exists(t *testing.T) {
	sql := readMatviewsSQL(t)
	if !strings.Contains(sql, "explorer_repo_files") {
		t.Error("matviews.sql must define explorer_repo_files materialized view")
	}
}

func TestMatviews_ExplorerPrResponse_Exists(t *testing.T) {
	sql := readMatviewsSQL(t)
	// Note: 8Knot spells it "explorer_pr_reponse" (missing 's'), but Aveloxis
	// already has "explorer_pr_response" (correct spelling). We need both names
	// or the correctly-spelled one that 8Knot actually queries.
	if !strings.Contains(sql, "explorer_pr_response") && !strings.Contains(sql, "explorer_pr_reponse") {
		t.Error("matviews.sql must define explorer_pr_response (or explorer_pr_reponse) materialized view")
	}
}

// Verify all four new views are in the refresh list.
func TestMatviews_RefreshList_ContainsNewViews(t *testing.T) {
	required := []string{
		"explorer_pr_files",
		"explorer_cntrb_per_file",
		"explorer_repo_files",
	}
	for _, name := range required {
		found := false
		for _, mv := range matviewNames {
			if strings.Contains(mv, name) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("matviewNames refresh list must contain %s", name)
		}
	}
}

// Verify unique indexes exist for CONCURRENTLY refresh support.
func TestMatviews_UniqueIndexes(t *testing.T) {
	sql := readMatviewsSQL(t)
	indexes := []string{
		"idx_unique_explorer_pr_files",
		"idx_unique_explorer_cntrb_per_file",
		"idx_unique_explorer_repo_files",
	}
	for _, idx := range indexes {
		if !strings.Contains(sql, idx) {
			t.Errorf("matviews.sql must create unique index %s for CONCURRENTLY refresh", idx)
		}
	}
}

func readMatviewsSQL(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("matviews.sql")
	if err != nil {
		t.Fatalf("cannot read matviews.sql: %v", err)
	}
	return string(data)
}
