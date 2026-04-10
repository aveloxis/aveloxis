package db

import (
	"testing"
)

// TestRepoStatsStructHasAllFields verifies the RepoStats struct has
// both gathered and metadata fields for PRs, issues, and commits.
func TestRepoStatsStructHasAllFields(t *testing.T) {
	s := RepoStats{
		RepoID:          1,
		GatheredPRs:     100,
		GatheredIssues:  50,
		GatheredCommits: 500,
		MetadataPRs:     110,
		MetadataIssues:  55,
		MetadataCommits: 520,
	}
	if s.GatheredPRs != 100 {
		t.Errorf("GatheredPRs = %d, want 100", s.GatheredPRs)
	}
	if s.MetadataCommits != 520 {
		t.Errorf("MetadataCommits = %d, want 520", s.MetadataCommits)
	}
}
