package github

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// Fix B (v0.18.22): drop prBatchSize from 25 → 10 to proportionally reduce
// per-query response size and validation cost. The existing batch-split
// logic in FetchPRBatch is unchanged; only the ceiling is lower, so we see
// 2.5× more queries but each is 2.5× cheaper. Point budget is not a concern
// (each batch query is ~2–10 points; 5000/hour per key stays well-fed).

// TestPRBatchSizeIs10 is a source-contract test that pins Fix B. A
// regression that raises the constant back to 25 (or higher) would
// re-introduce the same query-cost pressure that was producing
// stream-CANCEL and "Timeout on validation of query" errors in
// production on large Apache / grpc-scale repos.
func TestPRBatchSizeIs10(t *testing.T) {
	if prBatchSize != 10 {
		t.Errorf("prBatchSize = %d, want 10 (Fix B — v0.18.22)", prBatchSize)
	}
}

// TestFetchPRBatch_NewBatchSplit verifies the runtime contract: with
// prBatchSize=10, a 25-PR input fans out to exactly 3 queries (10+10+5).
// Supersedes the older TestFetchPRBatch_BatchSize which pinned the
// 25-at-a-time behavior — that one is updated to match the new ceiling
// in the same PR. Keeping this as a separate new file makes the Fix B
// contract visible in isolation.
func TestFetchPRBatch_NewBatchSplit(t *testing.T) {
	var queries atomic.Int32

	// The test uses a handler that counts queries and replies with the
	// minimum valid batch response for whatever aliases the caller asked
	// for. The payload isn't validated here (the happy-path tests cover
	// that); what matters is the query COUNT is the expected 3.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		// Determine how many aliases are in this query by counting "pr<i>:"
		// occurrences in the POST body. Crude but sufficient — the handler
		// just needs to respond with a matching number of null PRs so the
		// collector sees a well-formed response.
		buf := make([]byte, 65536)
		n, _ := r.Body.Read(buf)
		body := string(buf[:n])
		aliasCount := strings.Count(body, "pullRequest(number: $n")

		var sb strings.Builder
		sb.WriteString(`{"data":{"repository":{`)
		for i := 0; i < aliasCount; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(`"pr`)
			sb.WriteString(sprintInt(i))
			sb.WriteString(`":null`)
		}
		sb.WriteString(`}}}`)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sb.String()))
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	numbers := make([]int, 25)
	for i := range numbers {
		numbers[i] = i + 1
	}
	_, err := client.FetchPRBatch(context.Background(), "o", "r", numbers)
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}

	// prBatchSize=10, numbers=25 → ceil(25/10)=3 batches (10, 10, 5).
	if got := queries.Load(); got != 3 {
		t.Errorf("expected 3 queries (prBatchSize=10, numbers=25), got %d — Fix B batch split is wrong", got)
	}
}

// TestFetchPRBatch_SingleBatchUnderCeiling — passing exactly prBatchSize
// PRs must use ONE query, not two. Guards against off-by-one regressions
// in the loop bound.
func TestFetchPRBatch_SingleBatchUnderCeiling(t *testing.T) {
	var queries atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries.Add(1)
		buf := make([]byte, 65536)
		n, _ := r.Body.Read(buf)
		body := string(buf[:n])
		aliasCount := strings.Count(body, "pullRequest(number: $n")

		var sb strings.Builder
		sb.WriteString(`{"data":{"repository":{`)
		for i := 0; i < aliasCount; i++ {
			if i > 0 {
				sb.WriteString(",")
			}
			sb.WriteString(`"pr`)
			sb.WriteString(sprintInt(i))
			sb.WriteString(`":null`)
		}
		sb.WriteString(`}}}`)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(sb.String()))
	}))
	defer server.Close()
	client := newTestGraphQLClient(t, server.URL)

	numbers := make([]int, prBatchSize)
	for i := range numbers {
		numbers[i] = i + 1
	}
	_, err := client.FetchPRBatch(context.Background(), "o", "r", numbers)
	if err != nil {
		t.Fatalf("FetchPRBatch: %v", err)
	}
	if got := queries.Load(); got != 1 {
		t.Errorf("exactly prBatchSize PRs should be 1 query, got %d", got)
	}
}
