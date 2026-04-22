package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
)

// Fix A (v0.18.21): shrink the per-PR child connection page size from
// first: 100 → first: 50 in prNodeFragment. The paginateOversizedChildren
// handler already catches overflow via hasNextPage cursors, so data
// completeness is preserved — we just halve the worst-case response size
// per batch query. This directly addresses the HTTP/2 stream-CANCEL and
// "Timeout on validation of query" errors we saw in production against
// large repos (apache/spark, grpc/grpc) where 25 aliased PRs × 8 children
// × 100 items could produce 20,000-record responses that overran GitHub's
// server-side timeouts.

// TestPRNodeFragmentUsesFirst50 is a source-contract test that pins the
// Fix A invariant: every child connection selected inside the batched PR
// fragment uses `first: 50`, never `first: 100`. If someone later raises
// it back to 100 "for fewer paginations" they'll lose the error-rate
// reduction and this test catches it before ship.
//
// The follow-up cursor queries (paginatePRCommits et al.) are scoped to
// ONE PR at a time, not a 25-PR batch, so their `first:` size doesn't
// have the multiplicative explosion. Fix A leaves them unchanged; this
// test only regresses the batch-fragment value.
func TestPRNodeFragmentUsesFirst50(t *testing.T) {
	children := []string{
		"labels",
		"assignees",
		"reviewRequests",
		"reviews",
		"comments",
		"commits",
		"files",
	}
	for _, child := range children {
		re := regexp.MustCompile(child + `\(first:\s*(\d+)`)
		m := re.FindStringSubmatch(prNodeFragment)
		if m == nil {
			t.Errorf("prNodeFragment missing `%s(first: N)` clause — did a refactor drop this connection?", child)
			continue
		}
		if m[1] != "50" {
			t.Errorf("prNodeFragment: %s uses first: %s, want first: 50 (Fix A — v0.18.21)", child, m[1])
		}
	}
	// Belt and braces: prNodeFragment as a whole must not contain the old
	// `first: 100` literal anywhere. If a NEW connection is added at a
	// later date it must also respect the 50 cap.
	if strings.Contains(prNodeFragment, "first: 100") {
		t.Error("prNodeFragment contains `first: 100` — Fix A requires all batch-fragment connections to use first: 50 so the 25-PR-aliased response stays under GitHub's server-side limits")
	}
}

// TestFetchPRBatch_PaginatesAt51Items verifies the runtime invariant that
// comes with Fix A: any connection returning 50 items + hasNextPage=true
// triggers the existing pagination path and fetches the remainder. The
// pre-Fix-A test TestFetchPRBatch_PaginatesOversizedCommits covered the
// 100+50 case; this one covers the new 50+25 case to prove data
// completeness was preserved when we halved the page size.
func TestFetchPRBatch_PaginatesAt51Items(t *testing.T) {
	// Page 1: exactly 50 commits, hasNextPage=true.
	var p1 strings.Builder
	p1.WriteString(`{"nodes":[`)
	for i := 0; i < 50; i++ {
		if i > 0 {
			p1.WriteString(",")
		}
		p1.WriteString(`{"commit":{"oid":"sha`)
		p1.WriteString(sprintInt(i))
		p1.WriteString(`","message":"c`)
		p1.WriteString(sprintInt(i))
		p1.WriteString(`","committedDate":"2026-04-01T12:00:00Z","author":{"email":"a@example.com","name":"a","date":"2026-04-01T12:00:00Z","user":null}}}`)
	}
	p1.WriteString(`],"pageInfo":{"hasNextPage":true,"endCursor":"cur"}}`)

	// Page 2: 25 more commits, hasNextPage=false. Total 75.
	var p2 strings.Builder
	p2.WriteString(`{"nodes":[`)
	for i := 50; i < 75; i++ {
		if i > 50 {
			p2.WriteString(",")
		}
		p2.WriteString(`{"commit":{"oid":"sha`)
		p2.WriteString(sprintInt(i))
		p2.WriteString(`","message":"c`)
		p2.WriteString(sprintInt(i))
		p2.WriteString(`","committedDate":"2026-04-01T12:00:00Z","author":{"email":"a@example.com","name":"a","date":"2026-04-01T12:00:00Z","user":null}}}`)
	}
	p2.WriteString(`],"pageInfo":{"hasNextPage":false,"endCursor":null}}`)

	main := `{"data":{"repository":{"pr0":{
		"databaseId":1,"id":"PR_1","number":1,"title":"x","body":"","state":"OPEN",
		"locked":false,"createdAt":"2026-04-01T12:00:00Z","updatedAt":"2026-04-01T12:00:00Z",
		"closedAt":null,"mergedAt":null,"mergeCommit":null,"url":"",
		"authorAssociation":"NONE","author":null,
		"labels":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"assignees":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"reviewRequests":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"reviews":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"comments":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"commits":` + p1.String() + `,
		"files":{"nodes":[],"pageInfo":{"hasNextPage":false,"endCursor":null}},
		"headRef":null,"baseRef":null,"headRepository":null,"baseRepository":null
	}}}}`
	followup := `{"data":{"repository":{"pullRequest":{"commits":` + p2.String() + `}}}}`

	var queries atomic.Int32
	server := httptest.NewServer(graphqlFixture(t, []string{main, followup}))
	defer server.Close()
	// Intercept to count; graphqlFixture doesn't expose a counter.
	counting := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		// Re-issue to the canned fixture.
		server.Config.Handler.ServeHTTP(w, r)
	}))
	defer counting.Close()

	client := newTestGraphQLClient(t, counting.URL)
	out, err := client.FetchPRBatch(context.Background(), "o", "r", []int{1})
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("expected 1 PR, got %d", len(out))
	}
	if len(out[0].Commits) != 75 {
		t.Errorf("expected 75 commits (50 initial + 25 paginated), got %d — pagination is broken at the new 50-item page size", len(out[0].Commits))
	}
	if queries.Load() != 2 {
		t.Errorf("expected exactly 2 queries (initial batch + 1 pagination follow-up), got %d", queries.Load())
	}
	// Sanity: SHAs are unique and in order across the page boundary.
	for i, c := range out[0].Commits {
		want := "sha" + sprintInt(i)
		if c.SHA != want {
			t.Errorf("Commits[%d].SHA = %q, want %q (pagination must concatenate in order)", i, c.SHA, want)
			break
		}
	}
}
