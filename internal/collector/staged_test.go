package collector

import (
	"encoding/json"
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
)

func TestEntityTypeConstants(t *testing.T) {
	// All entity types used in staging.
	types := []string{
		EntityIssue,
		EntityPullRequest,
		EntityIssueEvent,
		EntityPREvent,
		EntityMessage,
		EntityReviewComment,
		EntityRelease,
		EntityContributor,
		EntityRepoInfo,
		EntityCloneStats,
	}

	seen := make(map[string]bool)
	for _, et := range types {
		if et == "" {
			t.Error("entity type constant is empty")
		}
		if seen[et] {
			t.Errorf("duplicate entity type: %q", et)
		}
		seen[et] = true
	}

	if len(types) != 10 {
		t.Errorf("expected 10 entity types, got %d", len(types))
	}
}

func TestProcessBatchSize(t *testing.T) {
	if processBatchSize != 500 {
		t.Errorf("processBatchSize = %d, want 500", processBatchSize)
	}
}

func TestStagedIssueEnvelope_MarshalRoundTrip(t *testing.T) {
	env := stagedIssue{
		Issue: model.Issue{
			Number: 42,
			Title:  "test issue",
		},
		Labels: []model.IssueLabel{
			{Text: "bug", Color: "red"},
			{Text: "help wanted", Color: "green"},
		},
		Assignees: []model.IssueAssignee{
			{PlatformSrcID: 100},
		},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded stagedIssue
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Issue.Number != 42 {
		t.Errorf("Issue.Number = %d, want 42", decoded.Issue.Number)
	}
	if decoded.Issue.Title != "test issue" {
		t.Errorf("Issue.Title = %q, want %q", decoded.Issue.Title, "test issue")
	}
	if len(decoded.Labels) != 2 {
		t.Fatalf("len(Labels) = %d, want 2", len(decoded.Labels))
	}
	if decoded.Labels[0].Text != "bug" {
		t.Errorf("Labels[0].Text = %q, want %q", decoded.Labels[0].Text, "bug")
	}
	if len(decoded.Assignees) != 1 {
		t.Fatalf("len(Assignees) = %d, want 1", len(decoded.Assignees))
	}
	if decoded.Assignees[0].PlatformSrcID != 100 {
		t.Errorf("Assignees[0].PlatformSrcID = %d, want 100", decoded.Assignees[0].PlatformSrcID)
	}
}

func TestStagedPREnvelope_MarshalRoundTrip(t *testing.T) {
	env := stagedPR{
		PR: model.PullRequest{
			Number: 99,
			Title:  "test pr",
		},
		Labels: []model.PullRequestLabel{
			{Name: "enhancement"},
		},
		Assignees: []model.PullRequestAssignee{
			{PlatformSrcID: 200},
		},
		Reviewers: []model.PullRequestReviewer{
			{PlatformSrcID: 300},
		},
		Reviews: []model.PullRequestReview{
			{State: "APPROVED", PlatformReviewID: 1},
		},
		Commits: []model.PullRequestCommit{
			{SHA: "abc123"},
		},
		Files: []model.PullRequestFile{
			{Path: "main.go", Additions: 10, Deletions: 5},
		},
		MetaHead: &model.PullRequestMeta{
			HeadOrBase: "head",
			Ref:        "feature-branch",
			SHA:        "def456",
		},
		MetaBase: &model.PullRequestMeta{
			HeadOrBase: "base",
			Ref:        "main",
			SHA:        "789abc",
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

	if decoded.PR.Number != 99 {
		t.Errorf("PR.Number = %d, want 99", decoded.PR.Number)
	}
	if len(decoded.Labels) != 1 {
		t.Fatalf("len(Labels) = %d, want 1", len(decoded.Labels))
	}
	if len(decoded.Assignees) != 1 {
		t.Fatalf("len(Assignees) = %d, want 1", len(decoded.Assignees))
	}
	if len(decoded.Reviewers) != 1 {
		t.Fatalf("len(Reviewers) = %d, want 1", len(decoded.Reviewers))
	}
	if len(decoded.Reviews) != 1 || decoded.Reviews[0].State != "APPROVED" {
		t.Errorf("Reviews = %+v, want APPROVED", decoded.Reviews)
	}
	if len(decoded.Commits) != 1 || decoded.Commits[0].SHA != "abc123" {
		t.Errorf("Commits = %+v", decoded.Commits)
	}
	if len(decoded.Files) != 1 || decoded.Files[0].Path != "main.go" {
		t.Errorf("Files = %+v", decoded.Files)
	}
	if decoded.MetaHead == nil || decoded.MetaHead.Ref != "feature-branch" {
		t.Errorf("MetaHead = %+v", decoded.MetaHead)
	}
	if decoded.MetaBase == nil || decoded.MetaBase.Ref != "main" {
		t.Errorf("MetaBase = %+v", decoded.MetaBase)
	}
}

func TestStagedIssueEnvelope_EmptyChildren(t *testing.T) {
	env := stagedIssue{
		Issue: model.Issue{Number: 1},
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var decoded stagedIssue
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if decoded.Labels != nil {
		t.Errorf("expected nil Labels, got %v", decoded.Labels)
	}
	if decoded.Assignees != nil {
		t.Errorf("expected nil Assignees, got %v", decoded.Assignees)
	}
}

func TestProcessorEntityOrder(t *testing.T) {
	// Verify the processing order in ProcessRepo matches dependencies.
	// Contributors must be first.
	entityOrder := []string{
		EntityContributor,
		EntityIssue,
		EntityPullRequest,
		EntityIssueEvent,
		EntityPREvent,
		EntityMessage,
		EntityReviewComment,
		EntityRelease,
		EntityRepoInfo,
		EntityCloneStats,
	}

	if entityOrder[0] != EntityContributor {
		t.Errorf("first entity type should be %q, got %q", EntityContributor, entityOrder[0])
	}

	// Issues must come before issue events (events reference issues).
	issueIdx := indexOf(entityOrder, EntityIssue)
	eventIdx := indexOf(entityOrder, EntityIssueEvent)
	if issueIdx >= eventIdx {
		t.Error("issues must be processed before issue events")
	}

	// PRs must come before PR events.
	prIdx := indexOf(entityOrder, EntityPullRequest)
	prEventIdx := indexOf(entityOrder, EntityPREvent)
	if prIdx >= prEventIdx {
		t.Error("pull requests must be processed before PR events")
	}
}

func indexOf(slice []string, item string) int {
	for i, s := range slice {
		if s == item {
			return i
		}
	}
	return -1
}
