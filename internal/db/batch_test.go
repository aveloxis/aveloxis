package db

import (
	"testing"
	"time"
)

// TestLibyearRowFields verifies LibyearRow struct has all fields used by batch insert.
func TestLibyearRowFields(t *testing.T) {
	row := LibyearRow{
		Name:               "express",
		Requirement:        "^4.18.0",
		Type:               "runtime",
		PackageManager:     "npm",
		CurrentVersion:     "4.18.0",
		LatestVersion:      "4.19.0",
		CurrentReleaseDate: "2023-01-01",
		LatestReleaseDate:  "2024-01-01",
		Libyear:            1.0,
		License:            "MIT",
		Purl:               "pkg:npm/express@4.18.0",
	}
	if row.Name != "express" {
		t.Errorf("LibyearRow.Name = %q, want %q", row.Name, "express")
	}
	if row.Libyear != 1.0 {
		t.Errorf("LibyearRow.Libyear = %f, want 1.0", row.Libyear)
	}
}

// TestRepoLaborRowFields verifies RepoLaborRow struct has all fields used by batch insert.
func TestRepoLaborRowFields(t *testing.T) {
	now := time.Now()
	row := RepoLaborRow{
		CloneDate:    now,
		AnalysisDate: now,
		Language:     "Go",
		FilePath:     "internal/db/postgres.go",
		FileName:     "postgres.go",
		TotalLines:   500,
		CodeLines:    400,
		CommentLines: 50,
		BlankLines:   50,
		Complexity:   25,
	}
	if row.Language != "Go" {
		t.Errorf("RepoLaborRow.Language = %q, want %q", row.Language, "Go")
	}
	if row.TotalLines != 500 {
		t.Errorf("RepoLaborRow.TotalLines = %d, want 500", row.TotalLines)
	}
}

// TestContributorRepoRowFields verifies ContributorRepoRow struct used by batch insert.
func TestContributorRepoRowFields(t *testing.T) {
	now := time.Now()
	row := ContributorRepoRow{
		CntrbID:   "01000000-0100-0000-0000-000000000000",
		RepoGit:   "https://github.com/owner/repo",
		RepoName:  "repo",
		GHRepoID:  12345,
		Category:  "PushEvent",
		EventID:   67890,
		CreatedAt: now,
	}
	if row.Category != "PushEvent" {
		t.Errorf("ContributorRepoRow.Category = %q, want %q", row.Category, "PushEvent")
	}
	if row.GHRepoID != 12345 {
		t.Errorf("ContributorRepoRow.GHRepoID = %d, want 12345", row.GHRepoID)
	}
}
