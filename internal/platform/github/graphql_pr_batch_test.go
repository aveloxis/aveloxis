package github

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// graphqlFixture is a handler that serves canned GraphQL responses in the
// same format GitHub does: HTTP 200, JSON body with {"data": ..., "errors": [...]}.
// Tests wire this into httptest.NewServer and use the resulting URL as the
// baseURL for a github.Client, so FetchPRBatch exercises the real client
// code path without hitting the network.
func graphqlFixture(t *testing.T, responses []string) http.Handler {
	t.Helper()
	var idx atomic.Int32
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Sanity: FetchPRBatch must POST to /graphql (relative to baseURL).
		if r.Method != "POST" {
			http.Error(w, "expected POST", http.StatusBadRequest)
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/graphql") {
			http.Error(w, "expected /graphql path, got "+r.URL.Path, http.StatusNotFound)
			return
		}
		n := int(idx.Add(1)) - 1
		if n >= len(responses) {
			http.Error(w, "out of canned responses", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(responses[n]))
	})
}

// TestFetchPRBatch_EmptyInput verifies the zero-effort path: passing an
// empty slice returns an empty result and makes NO network request.
// Callers that accumulate PR numbers in a slice may hand over an empty
// batch at the end of iteration; the implementation must not burn a
// GraphQL point on an empty query.
func TestFetchPRBatch_EmptyInput(t *testing.T) {
	var hit atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	out, err := client.FetchPRBatch(context.Background(), "o", "r", nil)
	if err != nil {
		t.Fatalf("empty input should be a no-op, got err=%v", err)
	}
	if len(out) != 0 {
		t.Errorf("expected empty slice, got %d elements", len(out))
	}
	if hit.Load() {
		t.Error("empty input must not make an HTTP request")
	}

	// Explicitly empty slice, same contract.
	out, err = client.FetchPRBatch(context.Background(), "o", "r", []int{})
	if err != nil || len(out) != 0 {
		t.Errorf("empty slice: err=%v len=%d", err, len(out))
	}
}

// TestFetchPRBatch_HappyPath verifies a 3-PR batch with all children under
// the 100-item limit returns a fully-populated []stagedPR with correct
// model mappings. This is the core "does the GraphQL path produce
// equivalent data to REST?" test at the unit level.
func TestFetchPRBatch_HappyPath(t *testing.T) {
	// Two-PR canned response covers: PR core fields, labels, assignees,
	// requested reviewers, reviews, commits, files, head/base refs and
	// repositories. Intentionally small counts so no pagination kicks in.
	resp := `{
		"data": {
			"repository": {
				"pr0": {
					"databaseId": 1001,
					"id": "PR_kwDO1",
					"number": 42,
					"title": "Add feature X",
					"body": "implements X per discussion",
					"state": "OPEN",
					"locked": false,
					"createdAt": "2026-04-01T12:00:00Z",
					"updatedAt": "2026-04-02T12:00:00Z",
					"closedAt": null,
					"mergedAt": null,
					"mergeCommit": null,
					"url": "https://github.com/o/r/pull/42",
					"authorAssociation": "CONTRIBUTOR",
					"author": {"login": "alice", "__typename": "User", "databaseId": 5001, "avatarUrl": "https://a.com/alice", "url": "https://github.com/alice"},
					"labels": {"nodes": [{"id": "LA_1", "name": "enhancement", "color": "a2eeef", "description": "New feature or request", "isDefault": false}], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"assignees": {"nodes": [{"databaseId": 5001, "login": "alice"}], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"reviewRequests": {"nodes": [{"requestedReviewer": {"__typename": "User", "databaseId": 5002, "login": "bob"}}], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"reviews": {"nodes": [{"databaseId": 7000, "id": "R_1", "state": "APPROVED", "body": "lgtm", "submittedAt": "2026-04-02T11:00:00Z", "authorAssociation": "MEMBER", "url": "https://github.com/o/r/pull/42#pullrequestreview-7000", "author": {"login": "bob", "databaseId": 5002, "__typename": "User"}, "commit": {"oid": "abc123"}}], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"commits": {"nodes": [{"commit": {"oid": "abc123", "message": "feat: x", "committedDate": "2026-04-01T12:00:00Z", "author": {"email": "alice@example.com", "name": "Alice", "date": "2026-04-01T12:00:00Z", "user": {"databaseId": 5001, "login": "alice"}}}}], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"files": {"nodes": [{"path": "src/x.go", "additions": 42, "deletions": 3}], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"headRefName": "feature-x", "headRefOid": "abc123",
					"baseRefName": "main", "baseRefOid": "000111",
					"headRepository": {"databaseId": 1234, "id": "R_1", "nameWithOwner": "alice/r", "name": "r", "isPrivate": false, "owner": {"login": "alice", "__typename": "User", "databaseId": 5001}},
					"baseRepository": {"databaseId": 5678, "id": "R_2", "nameWithOwner": "o/r", "name": "r", "isPrivate": false, "owner": {"login": "o", "__typename": "Organization", "databaseId": 9000}}
				},
				"pr1": {
					"databaseId": 1002,
					"id": "PR_kwDO2",
					"number": 43,
					"title": "Fix bug Y",
					"body": "",
					"state": "MERGED",
					"locked": false,
					"createdAt": "2026-04-03T12:00:00Z",
					"updatedAt": "2026-04-04T12:00:00Z",
					"closedAt": "2026-04-04T11:00:00Z",
					"mergedAt": "2026-04-04T11:00:00Z",
					"mergeCommit": {"oid": "deadbeef"},
					"url": "https://github.com/o/r/pull/43",
					"authorAssociation": "MEMBER",
					"author": {"login": "carol", "__typename": "User", "databaseId": 5003, "avatarUrl": "https://a.com/carol", "url": "https://github.com/carol"},
					"labels": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"assignees": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"reviewRequests": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"reviews": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"commits": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"files": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
					"headRefName": "", "headRefOid": "",
					"baseRefName": "main", "baseRefOid": "000112",
					"headRepository": null,
					"baseRepository": {"databaseId": 5678, "id": "R_2", "nameWithOwner": "o/r", "name": "r", "isPrivate": false, "owner": {"login": "o", "__typename": "Organization", "databaseId": 9000}}
				}
			},
			"rateLimit": {"limit": 5000, "remaining": 4967, "resetAt": "2026-04-18T15:00:00Z", "cost": 33}
		}
	}`

	server := httptest.NewServer(graphqlFixture(t, []string{resp}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	out, err := client.FetchPRBatch(context.Background(), "o", "r", []int{42, 43})
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 PRs, got %d", len(out))
	}

	// Verify PR 42 core fields map correctly.
	pr42 := findPRByNumber(out, 42)
	if pr42 == nil {
		t.Fatal("PR 42 missing from results")
	}
	if pr42.PR.Number != 42 {
		t.Errorf("PR.Number = %d, want 42", pr42.PR.Number)
	}
	if pr42.PR.Title != "Add feature X" {
		t.Errorf("PR.Title = %q, want 'Add feature X'", pr42.PR.Title)
	}
	if pr42.PR.State != "open" {
		// GraphQL returns state enum uppercase; the mapping must lowercase
		// to match the REST-populated column values.
		t.Errorf("PR.State = %q, want lowercase 'open'", pr42.PR.State)
	}
	if pr42.PR.PlatformSrcID != 1001 {
		t.Errorf("PR.PlatformSrcID = %d, want 1001 (from databaseId)", pr42.PR.PlatformSrcID)
	}
	if pr42.PR.NodeID != "PR_kwDO1" {
		t.Errorf("PR.NodeID = %q, want PR_kwDO1", pr42.PR.NodeID)
	}
	if pr42.PR.AuthorRef.Login != "alice" {
		t.Errorf("PR.AuthorRef.Login = %q, want alice", pr42.PR.AuthorRef.Login)
	}
	if pr42.PR.AuthorRef.PlatformID != 5001 {
		t.Errorf("PR.AuthorRef.PlatformID = %d, want 5001", pr42.PR.AuthorRef.PlatformID)
	}
	if pr42.PR.AuthorAssociation != "CONTRIBUTOR" {
		t.Errorf("PR.AuthorAssociation = %q, want CONTRIBUTOR", pr42.PR.AuthorAssociation)
	}

	// Labels.
	if len(pr42.Labels) != 1 {
		t.Errorf("len(Labels) = %d, want 1", len(pr42.Labels))
	} else {
		lab := pr42.Labels[0]
		if lab.Name != "enhancement" {
			t.Errorf("Labels[0].Name = %q, want enhancement", lab.Name)
		}
		if lab.Color != "a2eeef" {
			t.Errorf("Labels[0].Color = %q, want a2eeef", lab.Color)
		}
		// PlatformID is intentionally 0 on the GraphQL path — GitHub's
		// GraphQL Label type has no databaseId. Pin this so a future
		// refactor can't silently re-introduce a phantom field that
		// breaks against the live schema (the v0.18.1 regression).
		if lab.PlatformID != 0 {
			t.Errorf("Labels[0].PlatformID = %d, want 0 (GraphQL doesn't expose Label.databaseId; documented parity gap)", lab.PlatformID)
		}
		if lab.NodeID != "LA_1" {
			t.Errorf("Labels[0].NodeID = %q, want LA_1", lab.NodeID)
		}
	}

	// Assignees.
	if len(pr42.Assignees) != 1 {
		t.Errorf("len(Assignees) = %d, want 1", len(pr42.Assignees))
	} else if pr42.Assignees[0].PlatformSrcID != 5001 {
		t.Errorf("Assignees[0].PlatformSrcID = %d, want 5001", pr42.Assignees[0].PlatformSrcID)
	}

	// Requested reviewers.
	if len(pr42.Reviewers) != 1 {
		t.Errorf("len(Reviewers) = %d, want 1", len(pr42.Reviewers))
	} else if pr42.Reviewers[0].PlatformSrcID != 5002 {
		t.Errorf("Reviewers[0].PlatformSrcID = %d, want 5002", pr42.Reviewers[0].PlatformSrcID)
	}

	// Reviews.
	if len(pr42.Reviews) != 1 {
		t.Errorf("len(Reviews) = %d, want 1", len(pr42.Reviews))
	} else {
		rv := pr42.Reviews[0]
		if rv.State != "APPROVED" {
			t.Errorf("Reviews[0].State = %q, want APPROVED", rv.State)
		}
		if rv.CommitID != "abc123" {
			t.Errorf("Reviews[0].CommitID = %q, want abc123", rv.CommitID)
		}
		if rv.PlatformReviewID != 7000 {
			t.Errorf("Reviews[0].PlatformReviewID = %d, want 7000", rv.PlatformReviewID)
		}
	}

	// Commits — GraphQL gives us author.user.databaseId inline, a key win
	// over REST which requires a separate GET /users/{login}.
	if len(pr42.Commits) != 1 {
		t.Errorf("len(Commits) = %d, want 1", len(pr42.Commits))
	} else {
		cm := pr42.Commits[0]
		if cm.SHA != "abc123" {
			t.Errorf("Commits[0].SHA = %q, want abc123", cm.SHA)
		}
		if cm.AuthorEmail != "alice@example.com" {
			t.Errorf("Commits[0].AuthorEmail = %q, want alice@example.com", cm.AuthorEmail)
		}
		if cm.AuthorRef.PlatformID != 5001 {
			t.Errorf("Commits[0].AuthorRef.PlatformID = %d, want 5001 — GraphQL wins over REST here by returning author.user.databaseId inline",
				cm.AuthorRef.PlatformID)
		}
	}

	// Files.
	if len(pr42.Files) != 1 {
		t.Errorf("len(Files) = %d, want 1", len(pr42.Files))
	} else {
		f := pr42.Files[0]
		if f.Path != "src/x.go" || f.Additions != 42 || f.Deletions != 3 {
			t.Errorf("Files[0] = %+v, want src/x.go +42 -3", f)
		}
	}

	// Head/base meta.
	if pr42.MetaHead == nil {
		t.Error("MetaHead should be populated from headRef")
	} else {
		if pr42.MetaHead.Ref != "feature-x" {
			t.Errorf("MetaHead.Ref = %q, want feature-x", pr42.MetaHead.Ref)
		}
		if pr42.MetaHead.SHA != "abc123" {
			t.Errorf("MetaHead.SHA = %q, want abc123", pr42.MetaHead.SHA)
		}
	}
	if pr42.MetaBase == nil {
		t.Error("MetaBase should be populated from baseRef")
	}

	// Head/base repo (the fork tracking).
	if pr42.RepoHead == nil {
		t.Error("RepoHead should be populated from headRepository")
	} else {
		if pr42.RepoHead.RepoFullName != "alice/r" {
			t.Errorf("RepoHead.RepoFullName = %q, want alice/r", pr42.RepoHead.RepoFullName)
		}
		if pr42.RepoHead.SrcRepoID != 1234 {
			t.Errorf("RepoHead.SrcRepoID = %d, want 1234", pr42.RepoHead.SrcRepoID)
		}
	}

	// PR 43 — the merged one, different state handling.
	pr43 := findPRByNumber(out, 43)
	if pr43 == nil {
		t.Fatal("PR 43 missing from results")
	}
	// A PR with mergedAt != null must surface state="merged", not "closed".
	// This matches the REST path's behavior in github/client.go.
	if pr43.PR.State != "merged" {
		t.Errorf("PR 43 State = %q, want 'merged' (mergedAt != null)", pr43.PR.State)
	}
	if pr43.PR.MergeCommitSHA != "deadbeef" {
		t.Errorf("PR 43 MergeCommitSHA = %q, want 'deadbeef'", pr43.PR.MergeCommitSHA)
	}
	// Defensive check: fixture has empty headRefName AND empty headRefOid
	// on the merged PR. Real GitHub always populates these (persistent
	// String! scalars), so in practice MetaHead is always non-nil — but
	// if for some reason both come back empty, the mapper must handle
	// that gracefully and skip the meta row instead of inserting a
	// zero-value.
	if pr43.MetaHead != nil {
		t.Errorf("PR 43 MetaHead = %+v, want nil (both headRefName and headRefOid were empty)", pr43.MetaHead)
	}
	if pr43.RepoHead != nil {
		t.Errorf("PR 43 RepoHead = %+v, want nil (headRepository was null — fork deleted)", pr43.RepoHead)
	}
}

// TestFetchPRBatch_PaginatesOversizedCommits verifies that when a PR's
// commits connection has hasNextPage=true, the implementation follows the
// cursor until hasNextPage=false and returns the full set. This is the
// correctness guarantee for repos where PRs can have 100+ commits.
func TestFetchPRBatch_PaginatesOversizedCommits(t *testing.T) {
	// Page 1 of commits: 100 items, hasNextPage=true.
	var p1Commits strings.Builder
	p1Commits.WriteString(`{"nodes":[`)
	for i := 0; i < 100; i++ {
		if i > 0 {
			p1Commits.WriteString(",")
		}
		p1Commits.WriteString(`{"commit":{"oid":"sha`)
		p1Commits.WriteString(sprintInt(i))
		p1Commits.WriteString(`","message":"c`)
		p1Commits.WriteString(sprintInt(i))
		p1Commits.WriteString(`","committedDate":"2026-04-01T12:00:00Z","author":{"email":"a@example.com","name":"a","date":"2026-04-01T12:00:00Z","user":null}}}`)
	}
	p1Commits.WriteString(`],"pageInfo":{"hasNextPage":true,"endCursor":"cursor1"}}`)

	// Page 2: 50 more items, hasNextPage=false.
	var p2Commits strings.Builder
	p2Commits.WriteString(`{"nodes":[`)
	for i := 100; i < 150; i++ {
		if i > 100 {
			p2Commits.WriteString(",")
		}
		p2Commits.WriteString(`{"commit":{"oid":"sha`)
		p2Commits.WriteString(sprintInt(i))
		p2Commits.WriteString(`","message":"c`)
		p2Commits.WriteString(sprintInt(i))
		p2Commits.WriteString(`","committedDate":"2026-04-01T12:00:00Z","author":{"email":"a@example.com","name":"a","date":"2026-04-01T12:00:00Z","user":null}}}`)
	}
	p2Commits.WriteString(`],"pageInfo":{"hasNextPage":false,"endCursor":null}}`)

	// Main response: PR with oversized commits connection.
	main := `{"data":{"repository":{"pr0":{
		"databaseId": 1, "id": "PR_1", "number": 1, "title": "big", "body": "", "state": "OPEN",
		"locked": false, "createdAt": "2026-04-01T12:00:00Z", "updatedAt": "2026-04-01T12:00:00Z",
		"closedAt": null, "mergedAt": null, "mergeCommit": null, "url": "",
		"authorAssociation": "NONE", "author": null,
		"labels":         {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
		"assignees":      {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
		"reviewRequests": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
		"reviews":        {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
		"commits":        ` + p1Commits.String() + `,
		"files":          {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
		"headRef": null, "baseRef": null, "headRepository": null, "baseRepository": null
	}}}}`

	// Pagination follow-up response: contains commits-only for the one PR.
	followup := `{"data":{"repository":{"pullRequest":{"commits": ` + p2Commits.String() + `}}}}`

	server := httptest.NewServer(graphqlFixture(t, []string{main, followup}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	out, err := client.FetchPRBatch(context.Background(), "o", "r", []int{1})
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 PR, got %d", len(out))
	}
	if len(out[0].Commits) != 150 {
		t.Errorf("expected 150 paginated commits, got %d — pagination did not follow hasNextPage", len(out[0].Commits))
	}
	// Sanity: SHAs are in order and unique (pagination must concatenate,
	// not replace or skip).
	for i, c := range out[0].Commits {
		expected := "sha" + sprintInt(i)
		if c.SHA != expected {
			t.Errorf("Commits[%d].SHA = %q, want %q", i, c.SHA, expected)
			break
		}
	}
}

// TestFetchPRBatch_BatchSize — callers may pass more PR numbers than the
// per-query batch size; the implementation must split them into multiple
// queries transparently. Verify exactly the expected number of HTTP
// requests for a batch of 30 PRs (should be 2 at batch-size=25).
func TestFetchPRBatch_BatchSize(t *testing.T) {
	var queries atomic.Int32
	// Canned response for a batch — always returns an empty PR for each
	// aliased field, because we're counting requests, not validating content.
	var canned []string
	for batch := 0; batch < 2; batch++ {
		var sb strings.Builder
		sb.WriteString(`{"data":{"repository":{`)
		start := batch * 25
		end := start + 25
		if end > 30 {
			end = 30
		}
		for i := start; i < end; i++ {
			if i > start {
				sb.WriteString(",")
			}
			sb.WriteString(`"pr`)
			sb.WriteString(sprintInt(i - start))
			sb.WriteString(`":{"databaseId":`)
			sb.WriteString(sprintInt(i + 1))
			sb.WriteString(`,"id":"PR_`)
			sb.WriteString(sprintInt(i + 1))
			sb.WriteString(`","number":`)
			sb.WriteString(sprintInt(i + 1))
			sb.WriteString(`,"title":"","body":"","state":"OPEN","locked":false,"createdAt":"2026-04-01T12:00:00Z","updatedAt":"2026-04-01T12:00:00Z","closedAt":null,"mergedAt":null,"mergeCommit":null,"url":"","authorAssociation":"NONE","author":null,"labels":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"assignees":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"reviewRequests":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"reviews":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"commits":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"files":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},"headRef":null,"baseRef":null,"headRepository":null,"baseRepository":null}`)
		}
		sb.WriteString(`}}}`)
		canned = append(canned, sb.String())
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := int(queries.Add(1)) - 1
		if n >= len(canned) {
			http.Error(w, "too many queries", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(canned[n]))
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	numbers := make([]int, 30)
	for i := range numbers {
		numbers[i] = i + 1
	}
	out, err := client.FetchPRBatch(context.Background(), "o", "r", numbers)
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}
	if len(out) != 30 {
		t.Errorf("expected 30 PRs, got %d", len(out))
	}
	// Exactly 2 queries: 25 + 5 at batchSize=25.
	if queries.Load() != 2 {
		t.Errorf("expected 2 HTTP requests (batchSize=25, numbers=30), got %d", queries.Load())
	}
}

// TestFetchPRBatch_NullPR — a PR was deleted between the enumeration
// query and this batch query. GitHub returns the aliased field as null.
// The mapper must skip it, not panic and not inflate the result with a
// zero-valued PR.
func TestFetchPRBatch_NullPR(t *testing.T) {
	resp := `{
		"data": {"repository": {
			"pr0": null,
			"pr1": {
				"databaseId": 2, "id": "PR_2", "number": 2, "title": "ok", "body": "", "state": "OPEN",
				"locked": false, "createdAt": "2026-04-01T12:00:00Z", "updatedAt": "2026-04-01T12:00:00Z",
				"closedAt": null, "mergedAt": null, "mergeCommit": null, "url": "",
				"authorAssociation": "NONE", "author": null,
				"labels":         {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
				"assignees":      {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
				"reviewRequests": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
				"reviews":        {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
				"commits":        {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
				"files":          {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": null}},
				"headRef": null, "baseRef": null, "headRepository": null, "baseRepository": null
			}
		}}
	}`

	server := httptest.NewServer(graphqlFixture(t, []string{resp}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	out, err := client.FetchPRBatch(context.Background(), "o", "r", []int{1, 2})
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}
	if len(out) != 1 {
		t.Errorf("null PR must be skipped, got %d results (want 1)", len(out))
	}
	if len(out) == 1 && out[0].PR.Number != 2 {
		t.Errorf("surviving PR = %d, want 2", out[0].PR.Number)
	}
}

// TestFetchPRBatch_GraphQLErrorPropagates — when the GraphQL response has
// an errors array, FetchPRBatch must surface that error via
// platform.ClassifyError so callers apply the right retry/skip/abort policy.
func TestFetchPRBatch_GraphQLErrorPropagates(t *testing.T) {
	resp := `{"data":null,"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`
	server := httptest.NewServer(graphqlFixture(t, []string{resp}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	_, err := client.FetchPRBatch(context.Background(), "o", "r", []int{1})
	if err == nil {
		t.Fatal("GraphQL errors field must surface as a Go error")
	}
	if got := platform.ClassifyError(err); got != platform.ClassRateLimit {
		t.Errorf("expected ClassRateLimit, got %v (err=%v)", got, err)
	}
}

// TestFetchPRBatch_QueryContainsAllChildren — a source-level guard so a
// future refactor can't silently drop a child connection from the query.
// If it drops, that column stops populating in the data tables — a silent
// completeness regression. The query string must contain every field
// name we need.
func TestFetchPRBatch_QueryContainsAllChildren(t *testing.T) {
	src, err := os.ReadFile("graphql_pr_batch.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)
	for _, needed := range []string{
		"labels",
		"assignees",
		"reviewRequests",
		"reviews",
		"commits",
		"files",
		// Persistent scalar fields (not headRef/baseRef pointers) —
		// see prNodeFragment comment for why these specifically.
		"headRefName",
		"headRefOid",
		"baseRefName",
		"baseRefOid",
		"headRepository",
		"baseRepository",
		"authorAssociation",
		"pageInfo",
		"endCursor",
		"hasNextPage",
		"databaseId", // every entity that has one needs the numeric platform ID
	} {
		if !strings.Contains(code, needed) {
			t.Errorf("GraphQL query missing required field %q — dropping it would cause a silent column regression vs REST", needed)
		}
	}
}

// --- helpers -----------------------------------------------------------

func newTestGraphQLClient(t *testing.T, baseURL string) *Client {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	httpClient := platform.NewHTTPClient(baseURL, keys, logger, platform.AuthGitHub)
	return &Client{http: httpClient, logger: logger}
}

func findPRByNumber(prs []StagedPR, number int) *StagedPR {
	for i := range prs {
		if prs[i].PR.Number == number {
			return &prs[i]
		}
	}
	return nil
}

// sprintInt formats an int without importing strconv in the test file.
// Used for building canned fixture JSON.
func sprintInt(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
