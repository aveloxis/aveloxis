// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// Copilot reviews 5236927242 / 5237013602 on PR #209 (v0.29.55).

// gitlabRefusalServer answers the first key with a 403 spelled the GitLab way
// — RateLimit-Remaining: 0 and RateLimit-Reset, no X-RateLimit-* and no
// Retry-After — and every other key with a 200. auth extracts the key.
func gitlabRefusalServer(t *testing.T, refused string, auth func(*http.Request) string, okBody string) (*httptest.Server, func() map[string]int) {
	t.Helper()
	var mu sync.Mutex
	hits := map[string]int{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k := auth(r)
		mu.Lock()
		hits[k]++
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		if k == refused {
			w.Header().Set("RateLimit-Remaining", "0")
			w.Header().Set("RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
			return
		}
		_, _ = w.Write([]byte(okBody))
	}))
	t.Cleanup(srv.Close)
	return srv, func() map[string]int {
		mu.Lock()
		defer mu.Unlock()
		out := map[string]int{}
		for k, v := range hits {
			out[k] = v
		}
		return out
	}
}

// TestGitLabSpelledRefusalRotatesOnREST: isPrimaryRefusal (the pool's
// predicate) reads GitLab's RateLimit-Remaining, so the key was benched — but
// the REST 403 arm tested only X-RateLimit-Remaining and returned
// ErrForbidden, so the request failed instead of rotating to the healthy key.
func TestGitLabSpelledRefusalRotatesOnREST(t *testing.T) {
	srv, hits := gitlabRefusalServer(t, "a", func(r *http.Request) string { return r.Header.Get("PRIVATE-TOKEN") }, `{}`)
	keys := NewKeyPool([]string{"a", "b"}, testLogger())
	c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitLab)
	resp, err := c.Get(context.Background(), "/projects/1")
	if err != nil {
		t.Fatalf("Get: %v (hits=%v) — a GitLab-spelled refusal must rotate to the other key, not surface as forbidden", err, hits())
	}
	resp.Body.Close()
	if h := hits(); h["a"] != 1 || h["b"] != 1 {
		t.Fatalf("hits = %v, want one refused request on a and one on b", h)
	}
}

// TestGitLabSpelledRefusalRotatesOnGraphQL: the same inconsistency in the
// GraphQL client's 403 arm (the suppressed comment in review 5237013602).
func TestGitLabSpelledRefusalRotatesOnGraphQL(t *testing.T) {
	restore := SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	srv, hits := gitlabRefusalServer(t, "bearer a", func(r *http.Request) string { return r.Header.Get("Authorization") }, `{"data":{"hello":"world"}}`)
	keys := NewKeyPool([]string{"a", "b"}, testLogger())
	c := NewHTTPClient(srv.URL, keys, testLogger(), AuthGitLab)
	var got struct {
		Hello string `json:"hello"`
	}
	if err := c.GraphQL(context.Background(), "{ hello }", nil, &got); err != nil {
		t.Fatalf("GraphQL: %v (hits=%v) — a GitLab-spelled refusal must rotate, not surface as forbidden", err, hits())
	}
	if h := hits(); h["bearer a"] != 1 || h["bearer b"] != 1 {
		t.Fatalf("hits = %v, want one refused request on a and one on b", h)
	}
}
