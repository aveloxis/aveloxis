// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"

	"github.com/aveloxis/aveloxis/internal/model"
)

// mr_map.go — one fetched merge request → every model shape derived
// from it (v0.28.15).
//
// GitLab's single MR payload already carries the labels, assignees,
// reviewers, branch/SHA meta, and source/target project ids that the
// per-PR children need. Pre-v0.28.15 the batch composer called
// FetchPRByNumber + ListPRLabels + ListPRAssignees + ListPRReviewers +
// FetchPRMeta + FetchPRRepos — SIX GETs of the identical URL per MR —
// and the HTTP client's ETag cache turned every hit after the first
// into a 304, which none of those single-object readers can use:
// `gitlab pr batch at #1: labels: not modified (304)` aborted every MR
// batch (petsc/petsc on aveloxis_large: 0 of 9,450 MRs ever stored,
// routine collection AND the healer). These mappers are the single
// source for both the public per-child methods (which still GET the
// MR themselves) and the batch (which GETs it once).

// mrToPullRequest maps the fetched MR to the model (FetchPRByNumber's
// mapping, byte-for-byte).
// Provenance labels for pull_requests.data_source. v0.28.18: pre-.18 the
// single mapper stamped "(gap fill)" on every MR — including routine
// collection, which runs through FetchPRBatch — so the whole GitLab PR
// corpus read as gap-filled while GitHub's batch says "(pr batch)".
const (
	prDataSourceBatch   = "GitLab API (mr batch)"
	prDataSourceGapFill = "GitLab API (gap fill)"
)

func mrToPullRequest(raw glMergeRequest, dataSource string) *model.PullRequest {
	state := raw.State
	if state == "opened" {
		state = "open"
	}
	mergeCommit := raw.MergeCommitSHA
	if mergeCommit == "" {
		mergeCommit = raw.SquashCommitSHA
	}
	pr := &model.PullRequest{
		PlatformSrcID:  raw.ID,
		Number:         raw.IID,
		HTMLURL:        raw.WebURL,
		DiffURL:        raw.WebURL + ".diff",
		Title:          raw.Title,
		Body:           raw.Description,
		State:          state,
		Locked:         state == "locked",
		CreatedAt:      raw.CreatedAt,
		UpdatedAt:      raw.UpdatedAt,
		ClosedAt:       raw.ClosedAt,
		MergedAt:       raw.MergedAt,
		MergeCommitSHA: mergeCommit,
		AuthorRef:      glUserToRef(raw.Author),
		Origin: model.DataOrigin{
			ToolSource: "aveloxis",
			DataSource: dataSource,
		},
	}
	return pr
}

func mrLabels(raw glMergeRequest) []model.PullRequestLabel {
	if len(raw.Labels) == 0 {
		return nil // parity with the append-built slices: absent, not empty
	}
	out := make([]model.PullRequestLabel, 0, len(raw.Labels))
	for _, name := range raw.Labels {
		out = append(out, model.PullRequestLabel{Name: name})
	}
	return out
}

func mrAssignees(raw glMergeRequest) []model.PullRequestAssignee {
	if len(raw.Assignees) == 0 {
		return nil // parity with the append-built slices: absent, not empty
	}
	out := make([]model.PullRequestAssignee, 0, len(raw.Assignees))
	for _, a := range raw.Assignees {
		out = append(out, model.PullRequestAssignee{
			PlatformSrcID: a.ID,
			UserRef:       glUserToRef(a),
		})
	}
	return out
}

func mrReviewers(raw glMergeRequest) []model.PullRequestReviewer {
	if len(raw.Reviewers) == 0 {
		return nil // parity with the append-built slices: absent, not empty
	}
	out := make([]model.PullRequestReviewer, 0, len(raw.Reviewers))
	for _, r := range raw.Reviewers {
		out = append(out, model.PullRequestReviewer{
			PlatformSrcID: r.ID,
			UserRef:       glUserToRef(r),
		})
	}
	return out
}

func mrMeta(raw glMergeRequest) (head, base *model.PullRequestMeta) {
	head = &model.PullRequestMeta{
		HeadOrBase: "head",
		Ref:        raw.SourceBranch,
		SHA:        raw.SHA,
	}
	base = &model.PullRequestMeta{
		HeadOrBase: "base",
		Ref:        raw.TargetBranch,
	}
	return head, base
}

// mrRepos resolves the MR's source (head/fork) and target (base)
// projects. These are separate /projects/:id GETs — the only child
// lookups a single MR payload cannot answer.
func (c *Client) mrRepos(ctx context.Context, raw glMergeRequest) (headRepo, baseRepo *model.PullRequestRepo) {
	if raw.SourceProjectID != 0 {
		headRepo = c.fetchGLProjectAsRepo(ctx, raw.SourceProjectID, "head")
	}
	if raw.TargetProjectID != 0 {
		baseRepo = c.fetchGLProjectAsRepo(ctx, raw.TargetProjectID, "base")
	}
	return headRepo, baseRepo
}
