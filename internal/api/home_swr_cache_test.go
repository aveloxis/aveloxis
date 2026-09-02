// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.29.0 stale-while-revalidate on /home/repos: with the 5-minute TTL
// nearly every human visit was a cold cache entry, and the cold query
// measured mean 8.1s / max 48.2s on the production fleet. The cache now
// serves an EXPIRED entry immediately and refreshes it in the
// background (single-flight per user); only a genuinely-empty cache
// blocks — and the v0.29.0 queue-cached ranking makes that fast too.

// swrTestServer builds a bare Server with the home loader seam pointed
// at a fake, an auth stub that admits user 7, and the home route wired.
func swrTestServer(t *testing.T, load func(ctx context.Context, userID, limit int) ([]db.HomeRepo, error)) *Server {
	t.Helper()
	s := &Server{
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		mux:        http.NewServeMux(),
		homeLoader: load,
	}
	s.mux.HandleFunc("GET /api/v1/home/repos", func(w http.ResponseWriter, r *http.Request) {
		s.serveHomeRepos(w, r, 7, 50)
	})
	return s
}

func swrGet(t *testing.T, s *Server) (*httptest.ResponseRecorder, string) {
	t.Helper()
	req := httptest.NewRequest("GET", "/api/v1/home/repos", nil)
	rec := httptest.NewRecorder()
	s.mux.ServeHTTP(rec, req)
	return rec, rec.Header().Get("X-Cache")
}

func TestHomeCacheMissBlocksThenCaches(t *testing.T) {
	var calls atomic.Int32
	s := swrTestServer(t, func(context.Context, int, int) ([]db.HomeRepo, error) {
		calls.Add(1)
		return []db.HomeRepo{{RepoID: 1, Owner: "o", Name: "n"}}, nil
	})
	rec, xc := swrGet(t, s)
	if rec.Code != 200 || xc != "miss" {
		t.Fatalf("first request must be a blocking miss (X-Cache: miss — the documented third value), code=%d X-Cache=%q", rec.Code, xc)
	}
	var payload struct {
		Repos []db.HomeRepo `json:"repos"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil || len(payload.Repos) != 1 {
		t.Fatalf("payload: %v %s", err, rec.Body.String())
	}
	if _, xc := swrGet(t, s); xc != "hit" {
		t.Fatalf("second request within TTL must be a fresh hit, X-Cache=%q", xc)
	}
	if calls.Load() != 1 {
		t.Fatalf("loader calls = %d, want 1", calls.Load())
	}
}

func TestHomeCacheServesStaleAndRefreshesInBackground(t *testing.T) {
	refreshed := make(chan struct{}, 4)
	var calls atomic.Int32
	s := swrTestServer(t, func(context.Context, int, int) ([]db.HomeRepo, error) {
		n := calls.Add(1)
		if n > 1 {
			refreshed <- struct{}{}
		}
		return []db.HomeRepo{{RepoID: int64(n), Owner: "o", Name: "n"}}, nil
	})
	swrGet(t, s) // prime
	// Expire the entry in place.
	s.homeCache.mu.Lock()
	e := s.homeCache.entries[homeCacheKey{7, 50}]
	e.expires = time.Now().Add(-time.Minute)
	s.homeCache.entries[homeCacheKey{7, 50}] = e
	s.homeCache.mu.Unlock()

	rec, xc := swrGet(t, s)
	if rec.Code != 200 || xc != "stale" {
		t.Fatalf("expired entry must serve immediately as stale, code=%d X-Cache=%q", rec.Code, xc)
	}
	if body := rec.Body.String(); !jsonHasRepoID(t, body, 1) {
		t.Fatalf("stale response must carry the CACHED body, got %s", body)
	}
	select {
	case <-refreshed:
	case <-time.After(5 * time.Second):
		t.Fatal("a stale serve must trigger a background refresh")
	}
	// The refreshed body becomes the next hit.
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec, xc := swrGet(t, s)
		if xc == "hit" && jsonHasRepoID(t, rec.Body.String(), 2) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("refreshed body never landed, last X-Cache=%q body=%s", xc, rec.Body.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHomeCacheRefreshIsSingleFlight(t *testing.T) {
	block := make(chan struct{})
	var calls atomic.Int32
	s := swrTestServer(t, func(context.Context, int, int) ([]db.HomeRepo, error) {
		if calls.Add(1) > 1 {
			<-block // background refreshes park here
		}
		return []db.HomeRepo{{RepoID: 9, Owner: "o", Name: "n"}}, nil
	})
	swrGet(t, s) // prime (call 1)
	s.homeCache.mu.Lock()
	e := s.homeCache.entries[homeCacheKey{7, 50}]
	e.expires = time.Now().Add(-time.Minute)
	s.homeCache.entries[homeCacheKey{7, 50}] = e
	s.homeCache.mu.Unlock()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); swrGet(t, s) }()
	}
	wg.Wait()
	time.Sleep(100 * time.Millisecond) // let any extra refreshes launch
	if got := calls.Load(); got != 2 {
		t.Fatalf("loader calls = %d, want 2 (prime + ONE single-flight refresh for 8 stale hits)", got)
	}
	close(block)
}

func jsonHasRepoID(t *testing.T, body string, id int64) bool {
	t.Helper()
	var payload struct {
		Repos []db.HomeRepo `json:"repos"`
	}
	if err := json.Unmarshal([]byte(body), &payload); err != nil {
		t.Fatalf("bad payload %q: %v", body, err)
	}
	for _, r := range payload.Repos {
		if r.RepoID == id {
			return true
		}
	}
	return false
}

// TestHomeCacheInvalidateDefeatsInflightRefresh (review 2026-08-31 #1):
// a star toggle invalidates mid-refresh; the refresh's conditional set
// must LOSE — resurrecting the pre-star body would mask the star for a
// full TTL while api.md promises "the next request reorders
// immediately".
func TestHomeCacheInvalidateDefeatsInflightRefresh(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int32
	s := swrTestServer(t, func(context.Context, int, int) ([]db.HomeRepo, error) {
		n := calls.Add(1)
		if n == 2 {
			<-release // the background refresh parks here
		}
		return []db.HomeRepo{{RepoID: int64(n), Owner: "o", Name: "n"}}, nil
	})
	swrGet(t, s) // prime (body RepoID=1)
	s.homeCache.mu.Lock()
	e := s.homeCache.entries[homeCacheKey{7, 50}]
	e.expires = time.Now().Add(-time.Minute)
	s.homeCache.entries[homeCacheKey{7, 50}] = e
	s.homeCache.mu.Unlock()

	if _, xc := swrGet(t, s); xc != "stale" {
		t.Fatalf("expected stale serve, got %q", xc)
	}
	s.homeCache.invalidate(7) // the star toggle lands while the refresh is parked
	close(release)
	// Wait for the refresh goroutine to finish.
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.homeCache.mu.Lock()
		inflight := s.homeCache.refreshing[homeCacheKey{7, 50}]
		_, resurrected := s.homeCache.entries[homeCacheKey{7, 50}]
		s.homeCache.mu.Unlock()
		if !inflight {
			if resurrected {
				t.Fatal("the in-flight refresh resurrected a pre-invalidate body — the generation guard is gone")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh never finished")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// The next request must be a blocking MISS with fresh data.
	rec, xc := swrGet(t, s)
	if xc != "miss" || !jsonHasRepoID(t, rec.Body.String(), 3) {
		t.Fatalf("post-invalidate request must reload fresh, X-Cache=%q body=%s", xc, rec.Body.String())
	}
}

// TestHomeCacheLimitScopesTheEntry (review 2026-08-31 #2): a body built
// for one effective limit must never serve a request for another — and
// a small-limit refresh must not clobber the default-limit page's
// cache.
func TestHomeCacheLimitScopesTheEntry(t *testing.T) {
	var gotLimits []int
	var mu sync.Mutex
	s := swrTestServer(t, func(_ context.Context, _, limit int) ([]db.HomeRepo, error) {
		mu.Lock()
		gotLimits = append(gotLimits, limit)
		mu.Unlock()
		return []db.HomeRepo{{RepoID: int64(limit), Owner: "o", Name: "n"}}, nil
	})
	// Route that honors ?limit like the real handler.
	s.mux.HandleFunc("GET /api/v1/home/repos2", func(w http.ResponseWriter, r *http.Request) {
		limit := 0
		if v := r.URL.Query().Get("limit"); v != "" {
			limit, _ = strconvAtoi(v)
		}
		s.serveHomeRepos(w, r, 7, limit)
	})
	get := func(q string) (string, string) {
		req := httptest.NewRequest("GET", "/api/v1/home/repos2"+q, nil)
		rec := httptest.NewRecorder()
		s.mux.ServeHTTP(rec, req)
		return rec.Body.String(), rec.Header().Get("X-Cache")
	}
	if _, xc := get(""); xc != "miss" {
		t.Fatalf("first default-limit request: %q", xc)
	}
	body, xc := get("?limit=5")
	if xc != "miss" || !jsonHasRepoID(t, body, 5) {
		t.Fatalf("a different limit must MISS and load its own body, X-Cache=%q body=%s", xc, body)
	}
	// Default limit again: the 5-row entry must not serve it — AND the
	// default entry must still be CACHED (Copilot round 13: keying by
	// user alone made this a blocking miss, so alternating limits
	// defeated the SWR cache entirely while the correctness half held).
	body, xc = get("")
	if !jsonHasRepoID(t, body, 50) {
		t.Fatalf("default-limit request served a foreign-limit body: %s", body)
	}
	if xc != "hit" {
		t.Fatalf("default-limit entry was clobbered by the ?limit=5 store: X-Cache=%q, want a fresh hit", xc)
	}
	// And the small-limit entry coexists.
	if _, xc = get("?limit=5"); xc != "hit" {
		t.Fatalf("limit=5 entry not retained beside the default: X-Cache=%q", xc)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(gotLimits) < 2 || gotLimits[0] != 50 || gotLimits[1] != 5 {
		t.Fatalf("loader limits = %v (effective default must be 50)", gotLimits)
	}
}

// TestHomeCacheStaleAgeIsBounded (review 2026-08-31 #6): an entry past
// TTL + homeReposStaleMax is a MISS — a returning user after hours away
// gets fresh data, not an arbitrarily old body.
func TestHomeCacheStaleAgeIsBounded(t *testing.T) {
	var calls atomic.Int32
	s := swrTestServer(t, func(context.Context, int, int) ([]db.HomeRepo, error) {
		return []db.HomeRepo{{RepoID: int64(calls.Add(1)), Owner: "o", Name: "n"}}, nil
	})
	swrGet(t, s)
	s.homeCache.mu.Lock()
	e := s.homeCache.entries[homeCacheKey{7, 50}]
	e.expires = time.Now().Add(-homeReposStaleMax - time.Minute)
	s.homeCache.entries[homeCacheKey{7, 50}] = e
	s.homeCache.mu.Unlock()
	rec, xc := swrGet(t, s)
	if xc != "miss" || !jsonHasRepoID(t, rec.Body.String(), 2) {
		t.Fatalf("beyond the stale bound the cache must MISS and block-load, X-Cache=%q", xc)
	}
}

// TestServeHomeReposNilLoaderFailsClosed (review 2026-08-31 #7): the
// seam matches the sharedWithMeStore precedent's fail-closed half.
func TestServeHomeReposNilLoaderFailsClosed(t *testing.T) {
	s := &Server{logger: slog.New(slog.NewTextHandler(io.Discard, nil)), mux: http.NewServeMux()}
	s.mux.HandleFunc("GET /api/v1/home/repos", func(w http.ResponseWriter, r *http.Request) {
		s.serveHomeRepos(w, r, 7, 50)
	})
	rec, _ := swrGet(t, s)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("nil homeLoader must fail closed with 500, got %d", rec.Code)
	}
}

func strconvAtoi(v string) (int, error) { return strconv.Atoi(v) }
