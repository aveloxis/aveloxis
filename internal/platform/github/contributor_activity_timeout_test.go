// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// contributor_activity_timeout_test.go — v0.29.56. GitHub's in-body
// execution timeout ("Something went wrong while executing your query")
// failed 7 of 7 activity-classification ticks on 2026-09-17 because it
// classified fatal, so the chunk neither retried nor subdivided and the
// whole 2,500-login batch was discarded every tick. A live probe of the
// same batch succeeded 100/100, so the failure is intermittent.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

const activityExecutionTimeoutBody = `{"data":null,"errors":[{"message":"Something went wrong while executing your query on 2026-09-17T15:51:52Z. Please include ` +
	"`BC62:2B6E7D:4C218F4:FDB59B2:6AAC0C8E`" + ` when reporting this issue."}]}`

// TestFetchContributorActivityRecoversFromIntermittentTimeout: the first
// query times out once, the retry succeeds, and every login is delivered
// with no subdivision.
func TestFetchContributorActivityRecoversFromIntermittentTimeout(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var mu sync.Mutex
	queries := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		queries++
		n := queries
		mu.Unlock()
		if n == 1 {
			fmt.Fprint(w, activityExecutionTimeoutBody)
			return
		}
		fmt.Fprint(w, okResponse(aliasRe.FindAllStringSubmatch(req.Query, -1)))
	}))
	defer srv.Close()

	logins := make([]string, contributorActivityBatchSize)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, _, err := client.FetchContributorActivity(t.Context(), logins)
	if err != nil {
		t.Fatalf("one intermittent execution timeout must not fail the sweep: %v", err)
	}
	if len(got) != len(logins) {
		t.Errorf("got %d of %d logins", len(got), len(logins))
	}
	mu.Lock()
	defer mu.Unlock()
	if queries != 2 {
		t.Errorf("queries = %d, want 2 (one timeout, one retry, no subdivision)", queries)
	}
}

// TestFetchContributorActivityIsolatesPersistentTimeout: an account whose
// query times out on every attempt is isolated by subdivision and left out
// (the existing size-1 skip), and every other account is still delivered.
func TestFetchContributorActivityIsolatesPersistentTimeout(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	const poison = "times-out-every-time"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		for _, m := range aliases {
			if m[2] == poison {
				fmt.Fprint(w, activityExecutionTimeoutBody)
				return
			}
		}
		fmt.Fprint(w, okResponse(aliases))
	}))
	defer srv.Close()

	logins := make([]string, contributorActivityBatchSize)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	logins[11] = poison
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, _, err := client.FetchContributorActivity(t.Context(), logins)
	if err != nil {
		t.Fatalf("one persistently timing-out account must not fail the batch: %v", err)
	}
	if _, ok := got[poison]; ok {
		t.Error("the timing-out account must be absent")
	}
	if len(got) != len(logins)-1 {
		t.Errorf("got %d logins, want %d", len(got), len(logins)-1)
	}
}

// TestFetchContributorActivityContinuesPastAFailedChunk: a chunk that fails
// as a whole must not stop the ones after it. The claim is deterministic
// (oldest-checked first, no lease), so once the chunks AHEAD of a
// persistently failing one are stamped, that chunk becomes the first one —
// and stopping there would mean the sweep never progresses again, which is
// the v0.27.81 wedge and the 2026-09-17 incident this release exists to fix.
func TestFetchContributorActivityContinuesPastAFailedChunk(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Every login in the first chunk is individually unresolvable — the
	// "systemic" shape that fails the chunk rather than mark-stamping it.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		for _, m := range aliases {
			if strings.HasPrefix(m[2], "doomed") {
				fmt.Fprint(w, activityExecutionTimeoutBody)
				return
			}
		}
		fmt.Fprint(w, okResponse(aliases))
	}))
	defer srv.Close()

	logins := make([]string, 0, contributorActivityBatchSize*2)
	for i := 0; i < contributorActivityBatchSize; i++ {
		logins = append(logins, fmt.Sprintf("doomed%d", i))
	}
	for i := 0; i < contributorActivityBatchSize; i++ {
		logins = append(logins, fmt.Sprintf("healthy%d", i))
	}

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, _, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("a chunk that fails as a whole must still be reported")
	}
	for i := 0; i < contributorActivityBatchSize; i++ {
		if _, ok := got[fmt.Sprintf("healthy%d", i)]; !ok {
			t.Fatalf("healthy%d was never fetched: the sweep stopped at the failing chunk and would wedge at the claim head", i)
		}
	}
	for i := 0; i < contributorActivityBatchSize; i++ {
		if _, ok := got[fmt.Sprintf("doomed%d", i)]; ok {
			t.Errorf("doomed%d must be absent", i)
		}
	}
}

// TestFetchContributorActivityGivesUpAfterConsecutiveChunkFailures: keeping
// going past a failed chunk must not mean attempting all 100 chunks of a
// tick during a GitHub-wide outage. One fully-failing chunk costs ~147
// requests (49 subdivision nodes × the fast-fail budget) plus backoff, so a
// whole tick would be ~14,700 requests and hours of wall clock. A run of
// consecutive failures is the outage signature (2026-07-30/31); the sweep
// stops and reports, and the unstamped logins retry next tick anyway.
func TestFetchContributorActivityGivesUpAfterConsecutiveChunkFailures(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	var mu sync.Mutex
	queries := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries++
		mu.Unlock()
		fmt.Fprint(w, activityExecutionTimeoutBody)
	}))
	defer srv.Close()

	// Ten chunks, all failing: the sweep must stop after the give-up limit.
	logins := make([]string, 0, contributorActivityBatchSize*10)
	for i := 0; i < contributorActivityBatchSize*10; i++ {
		logins = append(logins, fmt.Sprintf("user%d", i))
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, unfetched, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("a run of failing chunks must be reported")
	}
	if len(unfetched) != len(logins) {
		t.Errorf("unfetched = %d, want all %d (nothing was fetched, and the chunks never attempted are unfetched too)", len(unfetched), len(logins))
	}
	mu.Lock()
	defer mu.Unlock()
	// Per fully-failing chunk: 49 subdivision nodes × 3 fast-fail attempts.
	const perChunk = 49 * 3
	if max := activityChunkFailureLimit * perChunk; queries > max {
		t.Errorf("sent %d queries, want at most %d (%d chunks × %d) — the sweep must stop once chunks fail in a row",
			queries, max, activityChunkFailureLimit, perChunk)
	}
	// The error text must stay readable: it names the count, not every failure.
	if n := strings.Count(err.Error(), "unresolvable at size 1"); n > activityChunkFailureLimit {
		t.Errorf("the error joins %d chunk failures; it must be bounded", n)
	}
}

// TestFetchContributorActivityReportsUnattemptedOnShutdown: `unfetched` is
// the caller's licence to stamp everyone else, so it must be complete on
// EVERY exit — including a `stop serve` landing mid-sweep. Before v0.29.56
// it listed only the chunks that had run, so a caller acting on it would
// stamp ~200 never-attempted contributors "checked" and burn a cooldown for
// them.
func TestFetchContributorActivityReportsUnattemptedOnShutdown(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	queries := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		queries++
		n := queries
		mu.Unlock()
		if n >= 2 {
			cancel() // shutdown lands during the second chunk
		}
		fmt.Fprint(w, okResponse(aliasRe.FindAllStringSubmatch(req.Query, -1)))
	}))
	defer srv.Close()

	logins := make([]string, 0, contributorActivityBatchSize*10)
	for i := 0; i < contributorActivityBatchSize*10; i++ {
		logins = append(logins, fmt.Sprintf("user%d", i))
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, unfetched, err := client.FetchContributorActivity(ctx, logins)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a context.Canceled the caller can classify as shutdown", err)
	}
	answered := map[string]bool{}
	for l := range got {
		answered[l] = true
	}
	for _, l := range unfetched {
		answered[l] = true
	}
	for _, l := range logins {
		if !answered[l] {
			t.Fatalf("%s is neither fetched nor reported unfetched: a caller would read its absence as 'deleted' and stamp it", l)
		}
	}
}

// TestFetchContributorActivityGiveUpRequiresConsecutiveFailures pins what
// makes the give-up counter CONSECUTIVE, and the bound on how many chunk
// errors the joined message carries. Both were unpinned: deleting
// `failedRun = 0` made the sweep stop at the third CUMULATIVE failure,
// silently skipping healthy chunks every tick while the message still said
// "in a row"; and the existing assertion on the joined count could never
// fire in a scenario where every chunk fails, because the give-up stops at
// the limit anyway — the two guards masked each other.
func TestFetchContributorActivityGiveUpRequiresConsecutiveFailures(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Seven chunks; chunks 0, 2 and 4 fail — four failures in total once
	// chunk 6 is added below, never three in a row.
	const chunks = 7
	failing := map[int]bool{0: true, 2: true, 4: true, 6: true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		for _, m := range aliases {
			var chunk int
			if _, err := fmt.Sscanf(m[2], "user%d", &chunk); err == nil && failing[chunk/contributorActivityBatchSize] {
				fmt.Fprint(w, activityExecutionTimeoutBody)
				return
			}
		}
		fmt.Fprint(w, okResponse(aliases))
	}))
	defer srv.Close()

	logins := make([]string, 0, contributorActivityBatchSize*chunks)
	for i := 0; i < contributorActivityBatchSize*chunks; i++ {
		logins = append(logins, fmt.Sprintf("user%d", i))
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, unfetched, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("four failing chunks must be reported")
	}

	// Every chunk was attempted: three healthy ones after the first failure
	// must not be skipped.
	wantFetched := contributorActivityBatchSize * 3
	if len(got) != wantFetched {
		t.Errorf("fetched %d logins, want %d — the sweep stopped early on non-consecutive failures", len(got), wantFetched)
	}
	if want := contributorActivityBatchSize * 4; len(unfetched) != want {
		t.Errorf("unfetched = %d, want %d (the four failed chunks only)", len(unfetched), want)
	}
	// Not a give-up: the message must not claim a run.
	if strings.Contains(err.Error(), "in a row") {
		t.Errorf("err = %v claims a consecutive run that never happened", err)
	}
	if !strings.Contains(err.Error(), "4 of 7 chunks failed") {
		t.Errorf("err = %v, want the honest 4-of-7 count", err)
	}
	// The joined list stays bounded even though four chunks failed.
	if n := strings.Count(err.Error(), "unresolvable at size 1"); n > activityChunkFailureLimit {
		t.Errorf("the error joins %d chunk failures, want at most %d", n, activityChunkFailureLimit)
	}
}

// TestFetchContributorActivityShutdownIsNotReportedAsAnOutage: a `stop
// serve` mid-sweep must surface as a shutdown, not as "chunks failed".
// The existing assertion only checked errors.Is(context.Canceled), which
// errors.Join satisfies even when the shutdown is wrapped in the outage
// message — so the short-circuit that distinguishes them was unpinned.
func TestFetchContributorActivityShutdownIsNotReportedAsAnOutage(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var mu sync.Mutex
	queries := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		queries++
		n := queries
		mu.Unlock()
		if n == 1 {
			cancel() // shutdown during the first chunk
		}
		fmt.Fprint(w, okResponse(aliasRe.FindAllStringSubmatch(req.Query, -1)))
	}))
	defer srv.Close()

	logins := make([]string, 0, contributorActivityBatchSize*5)
	for i := 0; i < contributorActivityBatchSize*5; i++ {
		logins = append(logins, fmt.Sprintf("user%d", i))
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, _, err := client.FetchContributorActivity(ctx, logins)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want a context.Canceled the caller classifies as shutdown", err)
	}
	if strings.Contains(err.Error(), "chunks failed") {
		t.Errorf("a shutdown was reported as a chunk outage: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if queries > 2 {
		t.Errorf("sent %d queries after cancellation — the sweep must stop at once", queries)
	}
}

// TestFetchContributorActivityGiveUpMessageSeparatesTheCounts: the tick's
// TOTAL failures and the RUN that triggered the give-up are different
// numbers, and the scheduler logs this text verbatim. Interpolating the
// total into "in a row" reported a GitHub outage bigger than the one that
// happened. Here chunk 0 fails alone, then 2-3-4 fail consecutively: four
// failures, a run of three.
func TestFetchContributorActivityGiveUpMessageSeparatesTheCounts(t *testing.T) {
	restore := platform.SetGraphQLSleepForTest(func(context.Context, time.Duration) error { return nil })
	defer restore()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	failing := map[int]bool{0: true, 2: true, 3: true, 4: true}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := aliasRe.FindAllStringSubmatch(req.Query, -1)
		for _, m := range aliases {
			var n int
			if _, err := fmt.Sscanf(m[2], "user%d", &n); err == nil && failing[n/contributorActivityBatchSize] {
				fmt.Fprint(w, activityExecutionTimeoutBody)
				return
			}
		}
		fmt.Fprint(w, okResponse(aliases))
	}))
	defer srv.Close()

	logins := make([]string, 0, contributorActivityBatchSize*6)
	for i := 0; i < contributorActivityBatchSize*6; i++ {
		logins = append(logins, fmt.Sprintf("user%d", i))
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, _, err := client.FetchContributorActivity(t.Context(), logins)
	if err == nil {
		t.Fatal("a run of three failing chunks must give up and report")
	}
	if !strings.Contains(err.Error(), "4 of 5 chunks failed") {
		t.Errorf("err = %v, want the tick's total (4 of 5 chunks attempted)", err)
	}
	if !strings.Contains(err.Error(), fmt.Sprintf("(%d in a row", activityChunkFailureLimit)) {
		t.Errorf("err = %v, want the RUN length (%d) in the give-up clause, not the total", err, activityChunkFailureLimit)
	}
}
