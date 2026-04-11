package github

import (
	"encoding/json"
	"testing"
)

// TestGHPRBranchRepoDeserialization verifies that the repo object within
// a PR's head/base branch is correctly deserialized from the GitHub API response.
// GitHub returns head.repo and base.repo with fork repo details.
func TestGHPRBranchRepoDeserialization(t *testing.T) {
	data := []byte(`{
		"label": "contributor:feature-branch",
		"ref": "feature-branch",
		"sha": "abc123",
		"user": {"id": 42, "login": "contributor"},
		"repo": {
			"id": 99999,
			"node_id": "MDEwOlJlcG9zaXRvcnk5OTk5OQ==",
			"name": "my-fork",
			"full_name": "contributor/my-fork",
			"private": false,
			"owner": {"id": 42, "login": "contributor"}
		}
	}`)

	var branch ghPRBranch
	if err := json.Unmarshal(data, &branch); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if branch.Repo == nil {
		t.Fatal("branch.Repo should not be nil")
	}
	if branch.Repo.ID != 99999 {
		t.Errorf("Repo.ID = %d, want 99999", branch.Repo.ID)
	}
	if branch.Repo.NodeID != "MDEwOlJlcG9zaXRvcnk5OTk5OQ==" {
		t.Errorf("Repo.NodeID = %q", branch.Repo.NodeID)
	}
	if branch.Repo.Name != "my-fork" {
		t.Errorf("Repo.Name = %q, want %q", branch.Repo.Name, "my-fork")
	}
	if branch.Repo.FullName != "contributor/my-fork" {
		t.Errorf("Repo.FullName = %q, want %q", branch.Repo.FullName, "contributor/my-fork")
	}
	if branch.Repo.Private {
		t.Error("Repo.Private should be false")
	}
	if branch.User.Login != "contributor" {
		t.Errorf("User.Login = %q, want %q", branch.User.Login, "contributor")
	}
}

// TestGHPRBranchNilRepo verifies that a null repo (e.g., deleted fork) is handled.
func TestGHPRBranchNilRepo(t *testing.T) {
	data := []byte(`{
		"label": "main",
		"ref": "main",
		"sha": "def456",
		"user": {"id": 1, "login": "owner"},
		"repo": null
	}`)

	var branch ghPRBranch
	if err := json.Unmarshal(data, &branch); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if branch.Repo != nil {
		t.Error("branch.Repo should be nil for deleted forks")
	}
}
