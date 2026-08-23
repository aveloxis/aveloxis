// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// contributor_history_test.go — TDD suite for the v0.27.58 daily
// contributor-history fetcher. Contracts:
//   - HistoryWindows tiles [account start → now] in configurable spans
//     (config → behavior end-to-end per the config-knobs rule), skipping
//     windows with no overlap with any contribution year;
//   - one GraphQL query per (user, window); day rows merge the four
//     contribution types per (day, repo); calendar day totals ride the
//     same query;
//   - hitting the API's caps (100 repositories per type, or a
//     contribution page with hasNextPage) SUBDIVIDES the window in half
//     rather than accepting loss, logging at INFO (operator ask:
//     "log on INFO whenever a user hits the activity cap so we can
//     keep track of loss rate");
//   - at the 1-day minimum window a cap is unrecoverable: log the loss
//     explicitly and keep what was returned.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/platform"
)

func day(s string) time.Time {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestHistoryWindowsTilesAndSkipsInactiveYears(t *testing.T) {
	created := day("2015-06-01")
	years := []int{2026, 2025, 2016} // 2017-2024 inactive, 2015 pre-first-contribution
	now := day("2026-07-30")
	wins := HistoryWindows(created, years, now, 180)
	if len(wins) == 0 {
		t.Fatal("expected windows")
	}
	// Starts no earlier than Jan 1 of the first contribution year
	// (2016) — 2015 has no contributions, and account creation caps it.
	if wins[0].From.Before(day("2016-01-01")) {
		t.Errorf("first window starts %s — must not precede the first contribution year", wins[0].From)
	}
	for _, w := range wins {
		if !w.To.After(w.From) {
			t.Fatalf("degenerate window %+v", w)
		}
		if w.To.Sub(w.From) > 180*24*time.Hour {
			t.Errorf("window %s→%s exceeds the 180-day span", w.From, w.To)
		}
		// No window may lie entirely inside the inactive 2017-2024 gap.
		if w.From.Year() >= 2017 && w.To.Year() <= 2024 {
			t.Errorf("window %s→%s has no overlap with any contribution year and must be skipped", w.From, w.To)
		}
	}
	last := wins[len(wins)-1]
	if last.To.Before(now.Add(-24 * time.Hour)) {
		t.Errorf("final window must reach now, ends %s", last.To)
	}
	if len(HistoryWindows(created, nil, now, 180)) != 0 {
		t.Error("no contribution years → no windows (nothing to fetch)")
	}
}

// The config→behavior path (config-knobs-end-to-end rule): the JSON
// value flows through the accessor into the spans actually generated.
func TestHistoryWindowsHonorConfiguredSpanEndToEnd(t *testing.T) {
	var c config.CollectionConfig
	if err := json.Unmarshal([]byte(`{"activity_history_window_days": 90}`), &c); err != nil {
		t.Fatal(err)
	}
	wins := HistoryWindows(day("2025-01-01"), []int{2025, 2026}, day("2026-01-01"), c.ActivityHistoryWindowDaysOrDefault())
	if len(wins) < 4 {
		t.Fatalf("a 1-year span at 90-day windows must yield >= 4 windows, got %d", len(wins))
	}
	for _, w := range wins {
		if w.To.Sub(w.From) > 90*24*time.Hour {
			t.Errorf("window %s→%s exceeds the configured 90-day span", w.From, w.To)
		}
	}
}

// historyResponse builds a window response. capRepos=true emits exactly
// 100 commit-repo entries (the repo cap); overflow=true marks the first
// repo's contributions page hasNextPage.
func historyResponse(capRepos, overflow bool) string {
	repoEntry := func(name string, dayISO string, n int, hasNext bool) string {
		return fmt.Sprintf(`{"repository":{"nameWithOwner":%q},"contributions":{"totalCount":%d,"pageInfo":{"hasNextPage":%v},"nodes":[{"occurredAt":"%sT07:00:00Z","commitCount":%d}]}}`,
			name, n, hasNext, dayISO, n)
	}
	var commitRepos []string
	if capRepos {
		for i := 0; i < 100; i++ {
			commitRepos = append(commitRepos, repoEntry(fmt.Sprintf("o/r%d", i), "2026-03-05", 1, false))
		}
	} else {
		commitRepos = append(commitRepos, repoEntry("aveloxis/aveloxis", "2026-03-05", 4, overflow))
	}
	return fmt.Sprintf(`{"data":{"user":{"contributionsCollection":{
		"contributionCalendar":{"weeks":[{"contributionDays":[{"date":"2026-03-05","contributionCount":9}]}]},
		"commitContributionsByRepository":[%s],
		"issueContributionsByRepository":[{"repository":{"nameWithOwner":"aveloxis/aveloxis"},"contributions":{"totalCount":1,"pageInfo":{"hasNextPage":false},"nodes":[{"occurredAt":"2026-03-05T07:00:00Z"}]}}],
		"pullRequestContributionsByRepository":[],
		"pullRequestReviewContributionsByRepository":[]
	}}}}`, strings.Join(commitRepos, ","))
}

func newHistoryTestClient(t *testing.T, handler http.HandlerFunc) (*Client, *bytes.Buffer) {
	t.Helper()
	var logBuf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger), &logBuf
}

func TestFetchContributorDailyHistoryMergesTypes(t *testing.T) {
	var queries int
	client, _ := newHistoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		queries++
		fmt.Fprint(w, historyResponse(false, false))
	})
	wins := []HistoryWindow{
		{From: day("2026-01-01"), To: day("2026-06-30")},
		{From: day("2026-06-30"), To: day("2026-07-30")},
	}
	days, totals, err := client.FetchContributorDailyHistory(t.Context(), "sgoggins", wins)
	if err != nil {
		t.Fatalf("FetchContributorDailyHistory: %v", err)
	}
	if queries != 2 {
		t.Errorf("2 uncapped windows must take exactly 2 queries, got %d", queries)
	}
	// Same (day, repo) from commit AND issue types merges into ONE row.
	var found bool
	for _, d := range days {
		if d.Day == "2026-03-05" && d.RepoFullName == "aveloxis/aveloxis" {
			found = true
			if d.Commits != 4 || d.Issues != 1 {
				t.Errorf("merged row wrong: %+v (want commits=4 issues=1)", d)
			}
		}
	}
	if !found {
		t.Fatal("expected a merged (2026-03-05, aveloxis/aveloxis) day row")
	}
	var totalFound bool
	for _, dt := range totals {
		if dt.Day == "2026-03-05" && dt.Total == 9 {
			totalFound = true
		}
	}
	if !totalFound {
		t.Error("calendar day totals must ride the same fetch (2026-03-05 → 9)")
	}
}

func TestFetchContributorDailyHistorySubdividesOnCapAndLogs(t *testing.T) {
	var queries int
	client, logBuf := newHistoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		queries++
		if queries == 1 {
			fmt.Fprint(w, historyResponse(true, false)) // 100 repos → cap
			return
		}
		fmt.Fprint(w, historyResponse(false, false))
	})
	wins := []HistoryWindow{{From: day("2026-01-01"), To: day("2026-06-30")}}
	_, _, err := client.FetchContributorDailyHistory(t.Context(), "busy-user", wins)
	if err != nil {
		t.Fatalf("FetchContributorDailyHistory: %v", err)
	}
	if queries != 3 {
		t.Errorf("capped window must subdivide into two halves (1 + 2 queries), got %d", queries)
	}
	log := logBuf.String()
	if !strings.Contains(log, "activity cap hit") {
		t.Errorf("cap hits must log at INFO with 'activity cap hit' (loss-rate tracking), got: %s", log)
	}
	if !strings.Contains(log, "busy-user") {
		t.Error("the cap log must identify the user")
	}
}

func TestFetchContributorDailyHistoryMinWindowLossIsLogged(t *testing.T) {
	var queries int
	client, logBuf := newHistoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		queries++
		fmt.Fprint(w, historyResponse(true, false)) // ALWAYS capped
	})
	// A 1-day window cannot subdivide: exactly one query, loss logged.
	wins := []HistoryWindow{{From: day("2026-03-05"), To: day("2026-03-06")}}
	days, _, err := client.FetchContributorDailyHistory(t.Context(), "hyper-user", wins)
	if err != nil {
		t.Fatalf("FetchContributorDailyHistory: %v", err)
	}
	if queries != 1 {
		t.Errorf("minimum-width window must not recurse (infinite subdivision guard), got %d queries", queries)
	}
	if len(days) == 0 {
		t.Error("capped minimum window must still keep the data that WAS returned")
	}
	if !strings.Contains(logBuf.String(), "data lost") {
		t.Errorf("unrecoverable cap at minimum window must log 'data lost': %s", logBuf.String())
	}
}

func TestFetchContributorHistoryMeta(t *testing.T) {
	client, _ := newHistoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"user":{"createdAt":"2010-08-29T16:25:48Z","contributionsCollection":{"contributionYears":[2026,2025,2010]}}}}`)
	})
	created, years, err := client.FetchContributorHistoryMeta(t.Context(), "sgoggins")
	if err != nil {
		t.Fatalf("FetchContributorHistoryMeta: %v", err)
	}
	if created.Year() != 2010 || len(years) != 3 || years[0] != 2026 {
		t.Errorf("meta decoded wrong: created=%s years=%v", created, years)
	}
}

// v0.28.3 — window-level concurrency. The serial chain was the
// sweep's per-contributor floor (~20 sequential RTTs on a 10-year
// account). The barrier handler releases only once BOTH requests are
// in flight: a serialized implementation times out at max=1 and
// fails. Run under -race: the shared accumulator's merge is the
// hazard the mutex exists for.
func TestFetchContributorDailyHistoryWindowsConcurrently(t *testing.T) {
	var mu sync.Mutex
	cur, peak := 0, 0
	release := make(chan struct{})
	var once sync.Once
	client, _ := newHistoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		cur++
		if cur > peak {
			peak = cur
		}
		if cur == 2 {
			once.Do(func() { close(release) })
		}
		mu.Unlock()
		select {
		case <-release:
		case <-time.After(5 * time.Second): // serialized — peak stays 1
		}
		fmt.Fprint(w, historyResponse(false, false))
		mu.Lock()
		cur--
		mu.Unlock()
	})
	client.SetHistoryWindowConcurrency(2)
	wins := []HistoryWindow{
		{From: day("2026-01-01"), To: day("2026-06-30")},
		{From: day("2026-06-30"), To: day("2026-07-30")},
	}
	if _, _, err := client.FetchContributorDailyHistory(t.Context(), "sgoggins", wins); err != nil {
		t.Fatalf("FetchContributorDailyHistory: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if peak != 2 {
		t.Errorf("windows must fetch concurrently (peak in-flight = %d, want 2)", peak)
	}
}

// Unset concurrency = the legacy serial shape — standalone callers
// and every pre-v0.28.3 test are behavior-identical until wired.
func TestHistoryWindowConcurrencyDefaultsSerial(t *testing.T) {
	c := &Client{}
	if got := c.historyWindowConc.Load(); got != 0 {
		t.Errorf("unset concurrency raw value = %d, want 0 (treated as 1)", got)
	}
	c.SetHistoryWindowConcurrency(0)
	if got := c.historyWindowConc.Load(); got != 1 {
		t.Errorf("SetHistoryWindowConcurrency(0) must clamp to 1, got %d", got)
	}
}

// v0.28.5 (Copilot round): a canceled PARENT context must surface as
// an ERROR, never as a partial-success — workers that exit at the
// semaphore/entry checks record no fetch error, and pre-fix the
// function returned the partial accumulator with err == nil, letting
// the scheduler store truncated history and stamp the contributor
// backfilled for a full cooldown.
func TestFetchContributorDailyHistoryCanceledCtxIsError(t *testing.T) {
	client, hits := newHistoryTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, historyResponse(false, false))
	})
	client.SetHistoryWindowConcurrency(4)
	wins := []HistoryWindow{
		{From: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)},
		{From: time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC), To: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)},
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled: workers drain without recording a fetch error
	_, _, err := client.FetchContributorDailyHistory(ctx, "sgoggins", wins)
	if err == nil {
		t.Fatal("canceled parent ctx returned success — partial/empty history would be stored and the contributor stamped backfilled")
	}
	_ = hits
}
