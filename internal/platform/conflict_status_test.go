// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// TestGet409IsADefinitiveAnswer pins worklist item 23: a 409 on a read
// ("Git Repository is empty" — the Git Database and Commits endpoints'
// documented answer for an empty or unavailable repository) fell into the
// "unexpected status" arm — about 110 s of
// retries, then ErrTransient, which the distribution scanner counts as a
// non-answer and strikes the repository toward the sideline. A conflict on
// a GET is the forge's answer about the resource: ErrConflict, ClassSkip,
// never retried. (451 was the same class, fixed in v0.29.58.)
func TestGet409IsADefinitiveAnswer(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"message":"Git Repository is empty."}`))
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL, NewKeyPool([]string{"tok"}, silentLogger()), silentLogger(), AuthGitHub)
	_, err := client.Get(context.Background(), "/repos/o/empty/contents")
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("err = %v; want errors.Is(err, ErrConflict)", err)
	}
	if ClassifyError(err) != ClassSkip || !IsDefinitiveAnswer(err) {
		t.Errorf("ClassifyError = %v, definitive = %v; want ClassSkip and definitive (an answer callers may record)", ClassifyError(err), IsDefinitiveAnswer(err))
	}
	if h := atomic.LoadInt32(&hits); h != 1 {
		t.Errorf("server hit %d times, want exactly 1 — a conflict on a read is never retried", h)
	}
}
