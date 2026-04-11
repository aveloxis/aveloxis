package collector

import (
	"encoding/json"
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
)

// TestStagedPRRepoEnvelope verifies that PR repo data is correctly
// bundled in the staged PR envelope for both head and base repos.
func TestStagedPRRepoEnvelope(t *testing.T) {
	env := stagedPR{
		PR: model.PullRequest{
			Title: "Add feature",
		},
		MetaHead: &model.PullRequestMeta{
			HeadOrBase: "head",
			Label:      "contributor:feature",
			Ref:        "feature",
			SHA:        "abc123",
		},
		MetaBase: &model.PullRequestMeta{
			HeadOrBase: "base",
			Label:      "owner:main",
			Ref:        "main",
			SHA:        "def456",
		},
		RepoHead: &model.PullRequestRepo{
			HeadOrBase:   "head",
			SrcRepoID:    99999,
			SrcNodeID:    "MDEwOlJlcG9zaXRvcnk5OTk5OQ==",
			RepoName:     "my-fork",
			RepoFullName: "contributor/my-fork",
			Private:      false,
		},
		RepoBase: &model.PullRequestRepo{
			HeadOrBase:   "base",
			SrcRepoID:    11111,
			SrcNodeID:    "MDEwOlJlcG9zaXRvcnkxMTExMQ==",
			RepoName:     "upstream",
			RepoFullName: "owner/upstream",
			Private:      false,
		},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded stagedPR
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.RepoHead == nil {
		t.Fatal("RepoHead should not be nil")
	}
	if decoded.RepoHead.RepoName != "my-fork" {
		t.Errorf("RepoHead.RepoName = %q, want %q", decoded.RepoHead.RepoName, "my-fork")
	}
	if decoded.RepoBase == nil {
		t.Fatal("RepoBase should not be nil")
	}
	if decoded.RepoBase.RepoName != "upstream" {
		t.Errorf("RepoBase.RepoName = %q, want %q", decoded.RepoBase.RepoName, "upstream")
	}
}

// TestPullRequestRepoModel verifies the PullRequestRepo model fields.
func TestPullRequestRepoModel(t *testing.T) {
	repo := model.PullRequestRepo{
		HeadOrBase:   "head",
		SrcRepoID:    42,
		SrcNodeID:    "node123",
		RepoName:     "my-repo",
		RepoFullName: "owner/my-repo",
		Private:      true,
	}

	if repo.HeadOrBase != "head" {
		t.Errorf("HeadOrBase = %q", repo.HeadOrBase)
	}
	if repo.SrcRepoID != 42 {
		t.Errorf("SrcRepoID = %d", repo.SrcRepoID)
	}
	if !repo.Private {
		t.Error("Private should be true")
	}
}
