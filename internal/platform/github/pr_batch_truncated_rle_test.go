// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// TestFetchPRBatchSubdividesOnTruncatedResourceLimit (PR #218 review E1): a
// truncated 200 body carrying RESOURCE_LIMITS_EXCEEDED is GitHub refusing a
// too-expensive query. It is exempt from the read retry, and it used to reach
// the caller as a ClassFatal decode error, so fetchPRBatchWithSubdivide
// (which halves only on ClassTransient / ClassRateLimit) failed the whole PR
// batch. It must classify like the complete-body answer and subdivide.
func TestFetchPRBatchSubdividesOnTruncatedResourceLimit(t *testing.T) {
	var calls, refused atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw, _ := io.ReadAll(r.Body)
		body := string(raw)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		// A query with a third alias (n2) is "too expensive": GitHub's
		// truncated refusal. Halves of two or fewer succeed.
		if strings.Contains(body, `"n2"`) {
			refused.Add(1)
			_, _ = w.Write([]byte(`{"data":{"repository":null},"errors":[{"type":"RESOURCE_LIMITS_EXCEEDED","path":["repository","pr0",`))
			return
		}
		_, _ = w.Write([]byte(`{"data":{"repository":null}}`))
	}))
	defer server.Close()

	client := newTestGraphQLClient(t, server.URL)
	if _, err := client.fetchPRBatchWithSubdivide(context.Background(), "x", "y", []int{1, 2, 3, 4}); err != nil {
		t.Fatalf("a truncated RESOURCE_LIMITS_EXCEEDED answer failed the batch (%v); it must subdivide", err)
	}
	if refused.Load() != 1 || calls.Load() != 3 {
		t.Errorf("calls=%d refused=%d; want one refused 4-alias query then two 2-alias halves (3 calls)", calls.Load(), refused.Load())
	}
}
