package github

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

)

// TestListIssuesAndPRs_EmptyRepo — a repo with no issues and no PRs must
// return an empty batch without error. Important for brand-new repos
// that make it into the queue before anyone opens an issue.
func TestListIssuesAndPRs_EmptyRepo(t *testing.T) {
	server := httptest.NewServer(graphqlFixture(t, []string{
		// issues page 1 — empty
		`{"data":{"repository":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
		// pullRequests page 1 — empty
		`{"data":{"repository":{"pullRequests":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`,
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	batch, err := client.ListIssuesAndPRs(context.Background(), "o", "r", time.Time{})
	if err != nil {
		t.Fatalf("ListIssuesAndPRs: %v", err)
	}
	if batch == nil {
		t.Fatal("batch is nil")
	}
	if len(batch.Issues) != 0 {
		t.Errorf("len(Issues) = %d, want 0", len(batch.Issues))
	}
	if len(batch.PullRequests) != 0 {
		t.Errorf("len(PullRequests) = %d, want 0", len(batch.PullRequests))
	}
}

// TestListIssuesAndPRs_HappyPath — single-page response for each connection.
// Asserts field-for-field mapping from GraphQL node to model.Issue /
// model.PullRequest matches what REST populates today, so phase 2 is a
// drop-in replacement for listing.
func TestListIssuesAndPRs_HappyPath(t *testing.T) {
	issuesResp := `{
		"data": {"repository": {"issues": {
			"nodes": [
				{
					"databaseId": 100, "number": 1, "id": "I_1",
					"title": "Test issue", "body": "Body text",
					"state": "OPEN",
					"url": "https://github.com/o/r/issues/1",
					"createdAt": "2026-04-01T12:00:00Z",
					"updatedAt": "2026-04-02T12:00:00Z",
					"closedAt": null,
					"comments": {"totalCount": 3},
					"author": {"__typename": "User", "login": "alice", "databaseId": 5001, "avatarUrl": "https://a.com/alice", "url": "https://github.com/alice"}
				}
			],
			"pageInfo": {"hasNextPage": false, "endCursor": null}
		}}}
	}`
	prsResp := `{
		"data": {"repository": {"pullRequests": {
			"nodes": [
				{
					"databaseId": 200, "number": 42, "id": "PR_42",
					"title": "Add x", "body": "",
					"state": "OPEN", "locked": false,
					"url": "https://github.com/o/r/pull/42",
					"createdAt": "2026-04-01T12:00:00Z",
					"updatedAt": "2026-04-03T12:00:00Z",
					"closedAt": null, "mergedAt": null,
					"mergeCommit": null,
					"authorAssociation": "CONTRIBUTOR",
					"author": {"__typename": "User", "login": "bob", "databaseId": 5002, "avatarUrl": "https://a.com/bob", "url": "https://github.com/bob"}
				}
			],
			"pageInfo": {"hasNextPage": false, "endCursor": null}
		}}}
	}`
	server := httptest.NewServer(graphqlFixture(t, []string{issuesResp, prsResp}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	batch, err := client.ListIssuesAndPRs(context.Background(), "o", "r", time.Time{})
	if err != nil {
		t.Fatalf("ListIssuesAndPRs: %v", err)
	}
	if len(batch.Issues) != 1 {
		t.Fatalf("len(Issues) = %d, want 1", len(batch.Issues))
	}
	issue := batch.Issues[0]
	if issue.Number != 1 {
		t.Errorf("Issue.Number = %d, want 1", issue.Number)
	}
	if issue.PlatformID != 100 {
		t.Errorf("Issue.PlatformID = %d, want 100 (databaseId)", issue.PlatformID)
	}
	if issue.NodeID != "I_1" {
		t.Errorf("Issue.NodeID = %q, want I_1", issue.NodeID)
	}
	if issue.State != "open" {
		t.Errorf("Issue.State = %q, want lowercase 'open' (matches REST convention)", issue.State)
	}
	if issue.CommentCount != 3 {
		t.Errorf("Issue.CommentCount = %d, want 3", issue.CommentCount)
	}
	if issue.ReporterRef.Login != "alice" {
		t.Errorf("Issue.ReporterRef.Login = %q, want alice", issue.ReporterRef.Login)
	}
	if issue.ReporterRef.PlatformID != 5001 {
		t.Errorf("Issue.ReporterRef.PlatformID = %d, want 5001", issue.ReporterRef.PlatformID)
	}

	if len(batch.PullRequests) != 1 {
		t.Fatalf("len(PullRequests) = %d, want 1", len(batch.PullRequests))
	}
	pr := batch.PullRequests[0]
	if pr.Number != 42 {
		t.Errorf("PR.Number = %d, want 42", pr.Number)
	}
	if pr.PlatformSrcID != 200 {
		t.Errorf("PR.PlatformSrcID = %d, want 200", pr.PlatformSrcID)
	}
	if pr.State != "open" {
		t.Errorf("PR.State = %q, want lowercase 'open'", pr.State)
	}
	if pr.AuthorAssociation != "CONTRIBUTOR" {
		t.Errorf("PR.AuthorAssociation = %q, want CONTRIBUTOR", pr.AuthorAssociation)
	}
	if pr.AuthorRef.Login != "bob" {
		t.Errorf("PR.AuthorRef.Login = %q, want bob", pr.AuthorRef.Login)
	}
}

// TestListIssuesAndPRs_MergedPRStateMapping — MERGED PRs must surface as
// State="merged", same rule as FetchPRBatch. A REST path called it
// "merged" when mergedAt was set; GraphQL returns enum MERGED directly.
// Both must map to lowercase "merged" in the model.
func TestListIssuesAndPRs_MergedPRStateMapping(t *testing.T) {
	issuesResp := `{"data":{"repository":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`
	prsResp := `{
		"data": {"repository": {"pullRequests": {
			"nodes": [
				{
					"databaseId": 300, "number": 7, "id": "PR_7",
					"title": "Merged PR", "body": "",
					"state": "MERGED", "locked": false,
					"url": "https://github.com/o/r/pull/7",
					"createdAt": "2026-03-01T12:00:00Z",
					"updatedAt": "2026-03-05T12:00:00Z",
					"closedAt": "2026-03-05T12:00:00Z",
					"mergedAt": "2026-03-05T12:00:00Z",
					"mergeCommit": {"oid": "cafebabe"},
					"authorAssociation": "MEMBER",
					"author": null
				}
			],
			"pageInfo": {"hasNextPage": false, "endCursor": null}
		}}}
	}`
	server := httptest.NewServer(graphqlFixture(t, []string{issuesResp, prsResp}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	batch, err := client.ListIssuesAndPRs(context.Background(), "o", "r", time.Time{})
	if err != nil {
		t.Fatalf("ListIssuesAndPRs: %v", err)
	}
	if len(batch.PullRequests) != 1 {
		t.Fatalf("len(PullRequests) = %d", len(batch.PullRequests))
	}
	pr := batch.PullRequests[0]
	if pr.State != "merged" {
		t.Errorf("PR.State = %q, want merged (state=MERGED and mergedAt set)", pr.State)
	}
	if pr.MergeCommitSHA != "cafebabe" {
		t.Errorf("PR.MergeCommitSHA = %q, want cafebabe", pr.MergeCommitSHA)
	}
}

// TestListIssuesAndPRs_PaginatesIssues — cursor pagination across multiple
// pages of the issues connection. Verifies every page is collected, in
// order, and the cursor is honored.
func TestListIssuesAndPRs_PaginatesIssues(t *testing.T) {
	issuesPage1 := `{
		"data": {"repository": {"issues": {
			"nodes": [
				{"databaseId": 1, "number": 1, "id": "I_1", "title": "t1", "body": "", "state": "OPEN", "url": "", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "closedAt": null, "comments": {"totalCount": 0}, "author": null}
			],
			"pageInfo": {"hasNextPage": true, "endCursor": "CUR1"}
		}}}
	}`
	issuesPage2 := `{
		"data": {"repository": {"issues": {
			"nodes": [
				{"databaseId": 2, "number": 2, "id": "I_2", "title": "t2", "body": "", "state": "CLOSED", "url": "", "createdAt": "2026-01-02T00:00:00Z", "updatedAt": "2026-01-02T00:00:00Z", "closedAt": "2026-01-03T00:00:00Z", "comments": {"totalCount": 0}, "author": null}
			],
			"pageInfo": {"hasNextPage": false, "endCursor": null}
		}}}
	}`
	prsResp := `{"data":{"repository":{"pullRequests":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`

	// Verify page 2's request carried the cursor from page 1.
	var p2Cursor atomic.Value
	var call atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := call.Add(1)
		body, _ := io.ReadAll(r.Body)
		var parsed struct {
			Variables map[string]any `json:"variables"`
		}
		_ = json.Unmarshal(body, &parsed)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		switch n {
		case 1:
			_, _ = w.Write([]byte(issuesPage1))
		case 2:
			// Record cursor used for issues page 2
			if c, ok := parsed.Variables["cursor"]; ok {
				p2Cursor.Store(c)
			}
			_, _ = w.Write([]byte(issuesPage2))
		default:
			_, _ = w.Write([]byte(prsResp))
		}
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	batch, err := client.ListIssuesAndPRs(context.Background(), "o", "r", time.Time{})
	if err != nil {
		t.Fatalf("ListIssuesAndPRs: %v", err)
	}
	if len(batch.Issues) != 2 {
		t.Errorf("len(Issues) = %d, want 2 (paginated)", len(batch.Issues))
	}
	if batch.Issues[0].Number != 1 || batch.Issues[1].Number != 2 {
		t.Errorf("paginated issues out of order: %+v", []int{batch.Issues[0].Number, batch.Issues[1].Number})
	}
	if c := p2Cursor.Load(); c != "CUR1" {
		t.Errorf("page 2 cursor = %v, want CUR1 (page 1's endCursor)", c)
	}
}

// TestListIssuesAndPRs_SinceFilterBreaksPRPagination — GraphQL's
// pullRequests connection has no native `since` filter. The implementation
// must order DESC by UPDATED_AT and stop paginating when it sees a PR
// whose updatedAt predates the since cutoff (same behavior REST had).
// Without that early-break, incremental collection re-fetches every
// PR every time.
func TestListIssuesAndPRs_SinceFilterBreaksPRPagination(t *testing.T) {
	issuesResp := `{"data":{"repository":{"issues":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}`
	// Page 1: one PR newer than since, one older. Implementation sees
	// "older" and stops — should NOT request page 2.
	prsPage1 := `{
		"data": {"repository": {"pullRequests": {
			"nodes": [
				{"databaseId": 10, "number": 10, "id": "PR_10", "title": "new", "body": "", "state": "OPEN", "locked": false, "url": "", "createdAt": "2026-04-01T00:00:00Z", "updatedAt": "2026-04-05T00:00:00Z", "closedAt": null, "mergedAt": null, "mergeCommit": null, "authorAssociation": "NONE", "author": null},
				{"databaseId": 9, "number": 9, "id": "PR_9", "title": "old", "body": "", "state": "OPEN", "locked": false, "url": "", "createdAt": "2026-01-01T00:00:00Z", "updatedAt": "2026-01-01T00:00:00Z", "closedAt": null, "mergedAt": null, "mergeCommit": null, "authorAssociation": "NONE", "author": null}
			],
			"pageInfo": {"hasNextPage": true, "endCursor": "SHOULD_NOT_BE_USED"}
		}}}
	}`

	var prPageCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var parsed struct {
			Query string `json:"query"`
		}
		_ = json.Unmarshal(body, &parsed)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		if strings.Contains(parsed.Query, "pullRequests") {
			prPageCalls.Add(1)
			_, _ = w.Write([]byte(prsPage1))
		} else {
			_, _ = w.Write([]byte(issuesResp))
		}
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	since := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	batch, err := client.ListIssuesAndPRs(context.Background(), "o", "r", since)
	if err != nil {
		t.Fatalf("ListIssuesAndPRs: %v", err)
	}
	// Only the PR newer than since should come back.
	if len(batch.PullRequests) != 1 {
		t.Errorf("len(PullRequests) = %d, want 1 (only the one newer than since)", len(batch.PullRequests))
	}
	// And the pullRequests query should have been called exactly ONCE —
	// the break-on-old-PR logic should have prevented a page 2 fetch.
	if prPageCalls.Load() != 1 {
		t.Errorf("pullRequests query called %d times, want 1 (must stop paginating once a PR older than since is seen)", prPageCalls.Load())
	}
}

// TestListIssuesAndPRs_QueryMentionsSinceFilterForIssues — source-level
// pin: the issues connection query must use `filterBy.since` so the
// server only returns issues updated after the cutoff. Without this,
// incremental collection pays to transfer every historical issue every
// cycle. (The PR side doesn't have a `since` filter — confirmed by the
// break-on-old-PR test above.)
func TestListIssuesAndPRs_QueryMentionsSinceFilterForIssues(t *testing.T) {
	data, err := readPhase2Source(t)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(data, "filterBy") {
		t.Error("phase 2 listing query must use issues(..., filterBy: {since: $since}) " +
			"to let the server apply the since filter — otherwise every incremental " +
			"collection re-fetches the entire issue history")
	}
	if !strings.Contains(data, "since") {
		t.Error("phase 2 listing must reference a 'since' variable")
	}
}

// TestListIssuesAndPRs_QueryOrdersPRsByUpdatedAtDesc — the PR connection
// uses orderBy UPDATED_AT DESC so the break-on-old logic finds the
// cutoff quickly. Without the ordering, the implementation would have
// to fetch every PR before finding the cutoff.
func TestListIssuesAndPRs_QueryOrdersPRsByUpdatedAtDesc(t *testing.T) {
	data, err := readPhase2Source(t)
	if err != nil {
		t.Fatal(err)
	}
	// Must order by UPDATED_AT on both connections.
	if !strings.Contains(data, "UPDATED_AT") {
		t.Error("listing queries must use orderBy: {field: UPDATED_AT, ...} so the " +
			"since-cutoff break logic can terminate early")
	}
	if !strings.Contains(data, "DESC") {
		t.Error("listing queries must order DESC so since-cutoff breaks at first old item")
	}
}

// --- helpers ----------------------------------------------------------

func readPhase2Source(t *testing.T) (string, error) {
	t.Helper()
	b, err := os.ReadFile("graphql_listing.go")
	if err != nil {
		return "", err
	}
	return string(b), nil
}
