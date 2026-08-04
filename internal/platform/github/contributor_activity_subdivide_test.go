// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// contributor_activity_subdivide_test.go — TDD suite for the
// v0.27.81 RLE subdivision in FetchContributorActivity.
//
// Background (2026-08-04 production wedge): v0.27.79 cut the alias
// batch to 25 after a live probe showed the RESOURCE_LIMITS_EXCEEDED
// edge at 35-40 aliases — but the edge MOVES (query cost depends on
// how dense the specific accounts are, not just alias count), and on
// 2026-08-04 production RLE'd at 25. Because the fetcher had no
// subdivision and the claim is deterministic NULL-first, the same
// 2,500-contributor batch failed identically every 15-minute tick:
// zero classifications, forever. These tests pin the fix: halve on
// RLE down to size 1; an account that RLEs ALONE is individually
// pathological (actions-user / ghost class) and is skipped with a
// WARN so the scheduler's existing absent→mark-only path retires it;
// a chunk where EVERY account skips at size 1 is the systemic
// incident shape and must still FAIL the fetch (the v0.27.79 216K-row
// lesson: never convert a resource-limit condition into an
// empty-but-successful result).

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

var aliasRe = regexp.MustCompile(`(u\d+): user\(login: "([^"]+)"\)`)

// rleResponse builds the production RLE shape: per-path
// RESOURCE_LIMITS_EXCEEDED on every alias plus null data nodes (the
// whole-query condition GitHub reports per-path — the 2026-07-30/31
// incident shape).
func rleResponse(aliases [][]string) string {
	var data, errs []string
	for _, m := range aliases {
		data = append(data, fmt.Sprintf(`%q:null`, m[1]))
		errs = append(errs, fmt.Sprintf(
			`{"type":"RESOURCE_LIMITS_EXCEEDED","path":[%q,"contributionsCollection"],"message":"Resource limits for this query exceeded."}`, m[1]))
	}
	return `{"data":{` + strings.Join(data, ",") + `},"errors":[` + strings.Join(errs, ",") + `]}`
}

func okResponse(aliases [][]string) string {
	var data []string
	for _, m := range aliases {
		data = append(data, fmt.Sprintf(
			`%q:{"login":%q,"contributionsCollection":{"restrictedContributionsCount":0,"contributionYears":[2026],"contributionCalendar":{"totalContributions":42}}}`,
			m[1], m[2]))
	}
	return `{"data":{` + strings.Join(data, ",") + `}}`
}

// TestFetchContributorActivitySubdividesAroundExpensiveAccount is the
// 2026-08-04 wedge fix: one pathologically dense account in a 25-alias
// batch trips RLE for the whole query. The fetcher must halve down to
// size 1, isolate the poison account, skip it (absent from the result
// map → the scheduler mark-only stamps it, retiring it from the claim
// head), and still deliver every other account's data.
func TestFetchContributorActivitySubdividesAroundExpensiveAccount(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const poison = "ghost-monster"

	var mu sync.Mutex
	var queries, poisonSoloQueries int
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		hasPoison := false
		for _, m := range aliases {
			if m[2] == poison {
				hasPoison = true
			}
		}
		mu.Lock()
		queries++
		if hasPoison && len(aliases) == 1 {
			poisonSoloQueries++
		}
		mu.Unlock()
		if hasPoison {
			fmt.Fprint(w, rleResponse(aliases))
			return
		}
		fmt.Fprint(w, okResponse(aliases))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	logins := make([]string, contributorActivityBatchSize)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	logins[7] = poison // mid-batch so both halves get exercised

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, err := client.FetchContributorActivity(t.Context(), logins)
	if err != nil {
		t.Fatalf("one expensive account must not fail the batch (the 2026-08-04 wedge): %v", err)
	}
	for _, l := range logins {
		if l == poison {
			continue
		}
		if _, ok := got[l]; !ok {
			t.Errorf("%s must be present — subdivision must recover every non-poison account", l)
		}
	}
	if _, ok := got[poison]; ok {
		t.Error("the poison account must be ABSENT (mark-only path), never classified from an errored fetch")
	}
	if poisonSoloQueries == 0 {
		t.Error("subdivision must isolate the poison account to a size-1 query before giving up on it")
	}
	if queries < 3 {
		t.Errorf("expected subdivision (multiple queries), got %d", queries)
	}
}

// TestFetchContributorActivityAllExpensiveFailsSystemic preserves the
// v0.27.79 incident contract under subdivision: when EVERY account in
// a chunk skips at size 1 (GitHub rejecting everything — a systemic
// condition, not an account property), the fetch must FAIL so the
// scheduler marks nothing. Subdivision must never degrade the
// incident shape into an empty-but-successful result.
func TestFetchContributorActivityAllExpensiveFailsSystemic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		fmt.Fprint(w, rleResponse(aliasRe.FindAllStringSubmatch(req.Query, -1)))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	logins := make([]string, contributorActivityBatchSize)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("a chunk where every account RLEs at size 1 is the systemic incident shape and must FAIL")
	}
	if platform.ClassifyError(err) != platform.ClassTransient {
		t.Errorf("systemic RLE must stay ClassTransient (retry next tick), got %v for %v",
			platform.ClassifyError(err), err)
	}
	if !errors.Is(err, platform.ErrResourceLimits) {
		t.Errorf("systemic RLE error must wrap platform.ErrResourceLimits, got %v", err)
	}
}

// TestFetchContributorActivityServerErrorFastFailAndSkip pins the
// SECOND failure shape the 2026-08-04 pilot surfaced: a dense
// account's query draws deterministic HTTP 500s (GitHub's resolver
// timing out server-side — the actions-user shape) rather than
// in-body RLE. The retry-exhaustion error wraps ErrTransient →
// ClassTransient, so subdivision must engage for it too. This test
// also pins the WithGraphQLFastFail budget: each failing query must
// burn exactly graphqlFastFailRetries attempts, not the full
// 10-retry backoff chain (the pilot measured ~7 minutes for ONE
// query under the full budget). Runtime note: this test sleeps
// through real backoff (~10-20s) — that's the price of exercising
// the genuine retry loop.
func TestFetchContributorActivityServerErrorFastFailAndSkip(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const poison = "dense-500-account"

	var mu sync.Mutex
	var poisonRequests int
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		for _, m := range aliases {
			if m[2] == poison {
				mu.Lock()
				poisonRequests++
				mu.Unlock()
				w.WriteHeader(http.StatusBadGateway)
				return
			}
		}
		fmt.Fprint(w, okResponse(aliases))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, err := client.FetchContributorActivity(t.Context(), []string{poison, "healthy-user"})
	if err != nil {
		t.Fatalf("a single 500-drawing account must not fail the batch: %v", err)
	}
	if _, ok := got["healthy-user"]; !ok {
		t.Error("healthy-user must be recovered by subdivision")
	}
	if _, ok := got[poison]; ok {
		t.Error("the 500-drawing account must be absent (mark-only path)")
	}
	mu.Lock()
	defer mu.Unlock()
	// Two failing queries hit the poison account: the size-2 batch and
	// its size-1 isolation — each capped at the fast-fail budget.
	if want := 2 * 3; poisonRequests != want {
		t.Errorf("fast-fail budget: want %d poison requests (2 queries x graphqlFastFailRetries), got %d — "+
			"if this is ~2x10 the WithGraphQLFastFail cap is not wired", want, poisonRequests)
	}
}

// TestFetchContributorActivityRateLimitedBubblesWithoutSubdivision:
// RATE_LIMITED is temporal, not an account property — halving the
// query doesn't help and skipping accounts at size 1 would mark-only
// innocent contributors. It must bubble immediately (one query, no
// subdivision) so the batch retries next tick against a recovered
// pool.
func TestFetchContributorActivityRateLimitedBubblesWithoutSubdivision(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var mu sync.Mutex
	var queries int
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries++
		mu.Unlock()
		fmt.Fprint(w, `{"errors":[{"type":"RATE_LIMITED","message":"API rate limit exceeded"}]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, err := client.FetchContributorActivity(t.Context(), []string{"alice", "bob"})
	if err == nil {
		t.Fatal("RATE_LIMITED must fail the fetch")
	}
	if platform.ClassifyError(err) != platform.ClassRateLimit {
		t.Errorf("want ClassRateLimit, got %v for %v", platform.ClassifyError(err), err)
	}
	mu.Lock()
	defer mu.Unlock()
	if queries != 1 {
		t.Errorf("RATE_LIMITED must not trigger subdivision: want 1 query, got %d", queries)
	}
}
