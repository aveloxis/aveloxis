// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// v0.29.58 (2026-09-22 log review, finding 3): GitHub answers every
// endpoint of a DMCA-blocked repository with 451 ("Repository access
// blocked", block.reason "dmca"). The client had no arm for it, so the
// status fell into the generic retry: ten backoff attempts per endpoint,
// eleven minutes per job, and prelim never sidelined the repository — it
// re-ran every cycle for ten months.

// TestIsRepoGoneStatus pins the ONE rule every repository-level probe
// shares (prelim, the gone recheck, reconcile-repos, mark-gone-repos):
// 404, 410 and 451 are definitive "this repository is not available".
func TestIsRepoGoneStatus(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusGone, http.StatusUnavailableForLegalReasons} {
		if !IsRepoGoneStatus(code) {
			t.Errorf("IsRepoGoneStatus(%d) = false, want true", code)
		}
	}
	for _, code := range []int{200, 301, 403, 429, 500, 502, 503} {
		if IsRepoGoneStatus(code) {
			t.Errorf("IsRepoGoneStatus(%d) = true, want false (not a definitive answer)", code)
		}
	}
}

// TestGet451ReturnsErrGoneWithoutRetry pins the client arm: one request,
// no retry, an error that is both ErrGone (so every caller skips it) and
// ErrLegallyBlocked (so the reason survives), classified as a skip.
func TestGet451ReturnsErrGoneWithoutRetry(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusUnavailableForLegalReasons)
		_, _ = w.Write([]byte(`{"message":"Repository access blocked","block":{"reason":"dmca","created_at":"2025-11-14T19:11:43Z","html_url":"https://github.com/github/dmca/blob/master/2025/11/2025-11-14-example.md"}}`))
	}))
	defer server.Close()

	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	_, err := client.Get(context.Background(), "/repos/hakyimlab/ptrs-ukb")
	if err == nil {
		t.Fatal("expected an error for 451")
	}
	if !errors.Is(err, ErrGone) {
		t.Errorf("err = %v, want errors.Is(err, ErrGone) so per-resource callers skip it", err)
	}
	if !errors.Is(err, ErrLegallyBlocked) {
		t.Errorf("err = %v, want errors.Is(err, ErrLegallyBlocked) so the reason survives", err)
	}
	if ClassifyError(err) != ClassSkip {
		t.Errorf("ClassifyError = %v, want ClassSkip", ClassifyError(err))
	}
	if h := atomic.LoadInt32(&hits); h != 1 {
		t.Errorf("server hit %d times, want exactly 1 — a legal block is definitive and must never be retried", h)
	}
	if got := err.Error(); !strings.Contains(got, "dmca") {
		t.Errorf("error text %q should carry the block reason from the body", got)
	}
}
