// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// v0.27.40 (summary/18 Phase 3): race-detector coverage for this
// package's shared state. Its mutexes had never been observed by
// -race under contention — the TestKeyPoolConcurrentAccess house
// template applied here.

package api

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

type raceFakeStore struct{}

func (raceFakeStore) ValidateSessionToken(ctx context.Context, token string) (int, error) {
	if token == "bad" {
		return 0, fmt.Errorf("invalid")
	}
	return len(token), nil
}
func (raceFakeStore) IsUserAdmin(ctx context.Context, userID int) (bool, error) {
	return userID%2 == 0, nil
}
func (raceFakeStore) GetUserRepoScope(ctx context.Context, userID int) ([]int64, error) {
	return []int64{1, 2, 3}, nil
}

func TestAuthenticatorConcurrentAccess(t *testing.T) {
	a := newAuthenticator(raceFakeStore{}, true)
	ctx := context.Background()
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				tok := fmt.Sprintf("tok-%d", (g+i)%8)
				a.resolveToken(ctx, tok)
				if i%17 == 0 {
					a.invalidateAll()
				}
				a.resolveToken(ctx, "bad")
			}
		}(g)
	}
	wg.Wait()
}

func TestRateLimiterConcurrentAccess(t *testing.T) {
	rl, err := newRateLimiter(Options{
		RateLimitRPS: 100, RateLimitBurst: 5, RateLimitDaily: 100000,
	})
	if err != nil {
		t.Fatal(err)
	}
	h := rl.middleware(nil)
	_ = h
	// Exercise through the exported surface the middleware uses.
	handler := rl.middleware(httpNoop{})
	var wg sync.WaitGroup
	for g := range 32 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := range 200 {
				r := httptest.NewRequest("GET", "/api/v1/repos/1/stats", nil)
				// Distinct PUBLIC IPs stress bucket creation + the
				// eviction path (private ranges are exempt).
				r.RemoteAddr = fmt.Sprintf("52.0.%d.%d:1234", g, i%251)
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
			}
		}(g)
	}
	wg.Wait()
}

type httpNoop struct{}

func (httpNoop) ServeHTTP(w http.ResponseWriter, r *http.Request) {}
