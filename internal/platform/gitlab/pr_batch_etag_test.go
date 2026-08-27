// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The 2026-08-26 aveloxis_large finding: petsc/petsc (the fleet's only
// GitLab repo) had 0 of 9,450 merge requests stored since inception —
// routine collection and heal-collection-gaps both failed every MR
// batch with `gitlab pr batch at #N: labels: not modified (304)`. The
// batch composer GET the same MR URL six times per MR (FetchPRByNumber,
// then ListPRLabels/ListPRAssignees/ListPRReviewers/FetchPRMeta/
// FetchPRRepos); the HTTP client cached the ETag from the first hit and
// sent If-None-Match on the rest, GitLab answered 304, and a
// single-object reader cannot use a 304. This mock honours
// If-None-Match exactly like GitLab does.

const mr1ETag = `W/"mr1-v1"`

const mr1JSON = `{
  "id": 501, "iid": 1, "title": "Fix the thing", "description": "body",
  "state": "opened", "web_url": "https://gitlab.com/owner/repo/-/merge_requests/1",
  "author": {"id": 7, "username": "alice", "name": "Alice"},
  "labels": ["bug", "help wanted"],
  "assignees": [{"id": 8, "username": "bob", "name": "Bob"}],
  "reviewers": [{"id": 9, "username": "carol", "name": "Carol"}],
  "created_at": "2026-01-02T03:04:05Z", "updated_at": "2026-01-03T03:04:05Z",
  "source_branch": "feature", "target_branch": "main",
  "source_project_id": 11, "target_project_id": 12, "sha": "abc123"
}`

// etagHonoringMRRouter serves MR !1 with an ETag and answers 304 to a
// matching If-None-Match; mrBodyHits counts the times the BODY was
// actually served. Every other endpoint the batch composes returns a
// minimal valid payload.
func etagHonoringMRRouter(mrBodyHits *int32) http.Handler {
	projectRe := regexp.MustCompile(`^/projects/\d+$`)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := r.URL.Path // %2F-escaped project path arrives decoded
		switch {
		case strings.HasSuffix(path, "/merge_requests/1"):
			w.Header().Set("ETag", mr1ETag)
			if r.Header.Get("If-None-Match") == mr1ETag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			atomic.AddInt32(mrBodyHits, 1)
			_, _ = w.Write([]byte(mr1JSON))
		case strings.HasSuffix(path, "/approvals"):
			_, _ = w.Write([]byte(`{"approved_by":[]}`))
		case strings.HasSuffix(path, "/commits"), strings.HasSuffix(path, "/diffs"):
			_, _ = w.Write([]byte(`[]`))
		case projectRe.MatchString(path):
			// Echo the requested project id so source (11) and target (12)
			// resolve to distinct projects.
			id := strings.TrimPrefix(path, "/projects/")
			_, _ = w.Write([]byte(`{"id":` + id + `,"name":"repo","path_with_namespace":"owner/repo","visibility":"public","namespace":{"path":"owner","kind":"group"}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"message":"404 Not Found"}`))
		}
	})
}

// TestFetchPRBatchSurvivesWarmETagCache — red before v0.28.15: after any
// single-object GET has cached the MR's ETag (a routine cycle does this
// constantly), the batch's own MR GETs got 304s and the composer aborted.
// Post-fix the batch fetches the body exactly once per MR, ETag-free,
// and every child derives from that one payload.
func TestFetchPRBatchSurvivesWarmETagCache(t *testing.T) {
	var hits int32
	client, _ := newTestClientWithCapture(t, etagHonoringMRRouter(&hits))
	ctx := context.Background()

	// Warm the client's ETag cache the way routine collection does.
	if _, err := client.FetchPRByNumber(ctx, "owner", "repo", 1); err != nil {
		t.Fatalf("warm-up FetchPRByNumber: %v", err)
	}

	batch, err := client.FetchPRBatch(ctx, "owner", "repo", []int{1})
	if err != nil {
		t.Fatalf("FetchPRBatch with a warm ETag cache: %v (pre-v0.28.15: `labels: not modified (304)`)", err)
	}
	if len(batch) != 1 {
		t.Fatalf("staged %d MRs, want 1", len(batch))
	}
	got := batch[0]
	if got.PR.Number != 1 || got.PR.Title != "Fix the thing" {
		t.Errorf("PR = %+v, want number 1 / title from the payload", got.PR)
	}
	if len(got.Labels) != 2 || got.Labels[0].Name != "bug" || got.Labels[1].Name != "help wanted" {
		t.Errorf("labels = %+v, want [bug, help wanted]", got.Labels)
	}
	if len(got.Assignees) != 1 || got.Assignees[0].PlatformSrcID != 8 {
		t.Errorf("assignees = %+v, want one with PlatformSrcID 8", got.Assignees)
	}
	if len(got.Reviewers) != 1 || got.Reviewers[0].PlatformSrcID != 9 {
		t.Errorf("reviewers = %+v, want one with PlatformSrcID 9", got.Reviewers)
	}
	if got.MetaHead == nil || got.MetaHead.Ref != "feature" || got.MetaHead.SHA != "abc123" {
		t.Errorf("MetaHead = %+v, want ref feature / sha abc123", got.MetaHead)
	}
	if got.MetaBase == nil || got.MetaBase.Ref != "main" {
		t.Errorf("MetaBase = %+v, want ref main", got.MetaBase)
	}
	if got.RepoHead == nil || got.RepoHead.SrcRepoID != 11 || got.RepoBase == nil || got.RepoBase.SrcRepoID != 12 {
		t.Errorf("RepoHead/RepoBase = %+v / %+v, want project ids 11 / 12", got.RepoHead, got.RepoBase)
	}
	// One body fetch for the warm-up + ONE for the whole batched MR
	// (six before v0.28.15).
	if h := atomic.LoadInt32(&hits); h != 2 {
		t.Errorf("MR body served %d times, want 2 (warm-up + one per batched MR)", h)
	}
	// The next cycle's batch in the same process also gets the body —
	// never a 304 that would silently empty the children (v0.26.3 class).
	if _, err := client.FetchPRBatch(ctx, "owner", "repo", []int{1}); err != nil {
		t.Fatalf("second FetchPRBatch in the same process: %v", err)
	}
	if h := atomic.LoadInt32(&hits); h != 3 {
		t.Errorf("MR body served %d times after the second batch, want 3", h)
	}
}

// The public per-child readers keep working on their own (they still
// GET the MR themselves and now share the mappers with the batch).
func TestListPRLabelsStandaloneStillReadsTheMergeRequest(t *testing.T) {
	var hits int32
	client, _ := newTestClientWithCapture(t, etagHonoringMRRouter(&hits))
	var names []string
	for l, err := range client.ListPRLabels(context.Background(), "owner", "repo", 1) {
		if err != nil {
			t.Fatalf("ListPRLabels: %v", err)
		}
		names = append(names, l.Name)
	}
	if strings.Join(names, ",") != "bug,help wanted" {
		t.Errorf("labels = %v, want [bug help wanted]", names)
	}
}

// Structural pin (comment-stripped): the batch derives every MR-payload
// child from its ONE fetch and runs ETag-free. A future "reuse the
// public method" refactor re-creates the six-GET / 304 shape.
func TestPRBatchDoesNotReGetTheMergeRequest(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/platform/gitlab/pr_batch.go"))
	if !strings.Contains(src, "platform.WithoutETag(ctx)") {
		t.Error("pr_batch.go must run the batch under platform.WithoutETag — a cached ETag turns the single-object MR GET into an unusable 304")
	}
	for _, banned := range []string{"c.FetchPRByNumber(", "c.ListPRLabels(", "c.ListPRAssignees(", "c.ListPRReviewers(", "c.FetchPRMeta(", "c.FetchPRRepos("} {
		if strings.Contains(src, banned) {
			t.Errorf("pr_batch.go calls %s — that re-GETs the merge request URL the batch already fetched; derive from the payload via mr_map.go instead", banned)
		}
	}
	for _, required := range []string{"mrToPullRequest(raw)", "mrLabels(raw)", "mrAssignees(raw)", "mrReviewers(raw)", "mrMeta(raw)", "c.mrRepos(ctx, raw)"} {
		if !strings.Contains(src, required) {
			t.Errorf("pr_batch.go must derive children via %s", required)
		}
	}
}
