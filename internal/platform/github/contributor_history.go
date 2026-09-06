// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// SetHistoryWindowConcurrency bounds how many history windows fetch
// concurrently per contributor (v0.28.3; wired each sweep tick from
// collection.activity_history_window_concurrency — the platform
// package stays config-free). Unset/zero = 1, the legacy serial
// shape, so standalone callers and tests are behavior-identical
// until wired. Atomic: the sweep sets it while pool workers may be
// mid-fetch.
func (c *Client) SetHistoryWindowConcurrency(n int) {
	if n < 1 {
		n = 1
	}
	c.historyWindowConc.Store(int32(n))
}

// contributor_history.go — v0.27.58 daily contributor-history fetch.
// One GraphQL query per (user, window) returns, at rate-limit cost 1
// (live-verified 2026-07-30): the daily contribution calendar
// (includes disclosed private) plus the four by-repository connections
// whose dated nodes give per-(day, repo) public activity. Windows are
// operator-configurable (activity_history_window_days, default 180;
// GitHub hard-caps any window at one year).
//
// Cap handling (operator contract): when a window hits the API's
// 100-repositories-per-type cap or a contributions page overflows
// (pageInfo.hasNextPage), the window is SUBDIVIDED in half and
// re-fetched — cost stays proportional to real activity density —
// with an INFO log per hit so loss rate is trackable. At the 1-day
// minimum width a cap is unrecoverable: the loss is logged explicitly
// and whatever WAS returned is kept.

// HistoryWindow is one from/to span for a contributionsCollection
// query. To is exclusive-ish (GitHub treats the range inclusively at
// day granularity; adjacent windows share their boundary instant).
type HistoryWindow struct {
	From time.Time
	To   time.Time
}

// historyMinWindow is the subdivision floor: below 48h a window's
// halves would be sub-day, and the by-repository nodes are per-day
// rollups — nothing finer exists to recover.
const historyMinWindow = 48 * time.Hour

// HistoryWindows tiles [account start → now] into windowDays-wide
// spans, where the start is the later of account creation and Jan 1 of
// the first contribution year. Windows with no overlap with any
// contribution year are skipped (contributionYears is the API's own
// index of non-empty years — querying the gaps would waste the whole
// sweep's budget on empty windows). No years → nothing to fetch.
func HistoryWindows(createdAt time.Time, years []int, now time.Time, windowDays int) []HistoryWindow {
	if len(years) == 0 || windowDays <= 0 || !now.After(createdAt) {
		return nil
	}
	active := make(map[int]bool, len(years))
	minYear := years[0]
	for _, y := range years {
		active[y] = true
		if y < minYear {
			minYear = y
		}
	}
	start := createdAt
	if firstJan := time.Date(minYear, 1, 1, 0, 0, 0, 0, time.UTC); firstJan.After(start) {
		start = firstJan
	}
	span := time.Duration(windowDays) * 24 * time.Hour
	var out []HistoryWindow
	for from := start; from.Before(now); from = from.Add(span) {
		to := from.Add(span)
		if to.After(now) {
			to = now
		}
		overlaps := false
		for y := from.Year(); y <= to.Year(); y++ {
			if active[y] {
				overlaps = true
				break
			}
		}
		if overlaps {
			out = append(out, HistoryWindow{From: from, To: to})
		}
	}
	return out
}

// FetchContributorHistoryMeta returns the account creation time and
// contribution years — the inputs HistoryWindows needs. Cost: 1 point.
func (c *Client) FetchContributorHistoryMeta(ctx context.Context, login string) (time.Time, []int, error) {
	// Background sweep: leave GraphQLBackgroundReserve headroom per key for
	// foreground collection (the 2026-09-01 pytorch diagnostic — the history
	// sweep's sustained load kept keys graphql-dry under multi-day jobs).
	ctx = platform.WithGraphQLBackgroundBudget(ctx)
	query := fmt.Sprintf(`query { user(login: %q) { createdAt contributionsCollection { contributionYears } } }`, login)
	var resp struct {
		User struct {
			CreatedAt               time.Time `json:"createdAt"`
			ContributionsCollection struct {
				ContributionYears []int `json:"contributionYears"`
			} `json:"contributionsCollection"`
		} `json:"user"`
	}
	if err := c.http.GraphQL(ctx, query, nil, &resp); err != nil {
		return time.Time{}, nil, fmt.Errorf("contributor history meta %q: %w", login, err)
	}
	return resp.User.CreatedAt, resp.User.ContributionsCollection.ContributionYears, nil
}

// historyAccumulator merges per-window results across windows and
// subdivision levels. v0.28.3: windows fetch CONCURRENTLY, so the
// merge sections lock mu — safe because the merge uses ASSIGNMENT
// semantics (order-independent; boundary days arrive identical from
// both neighboring windows).
type historyAccumulator struct {
	mu     sync.Mutex
	days   map[string]*model.ContributorDayActivity // key: day + "\x00" + repo
	totals map[string]int                           // day → calendar total
}

func newHistoryAccumulator() *historyAccumulator {
	return &historyAccumulator{days: map[string]*model.ContributorDayActivity{}, totals: map[string]int{}}
}

func (a *historyAccumulator) row(dayISO, repo string) *model.ContributorDayActivity {
	key := dayISO + "\x00" + repo
	if r, ok := a.days[key]; ok {
		return r
	}
	r := &model.ContributorDayActivity{Day: dayISO, RepoFullName: repo}
	a.days[key] = r
	return r
}

// historyWindowResp mirrors the per-window GraphQL response.
type historyWindowResp struct {
	User struct {
		ContributionsCollection historyCollection `json:"contributionsCollection"`
	} `json:"user"`
}

type historyCollection struct {
	ContributionCalendar struct {
		Weeks []struct {
			ContributionDays []struct {
				Date              string `json:"date"`
				ContributionCount int    `json:"contributionCount"`
			} `json:"contributionDays"`
		} `json:"weeks"`
	} `json:"contributionCalendar"`
	Commits []historyRepoEntry `json:"commitContributionsByRepository"`
	Issues  []historyRepoEntry `json:"issueContributionsByRepository"`
	PRs     []historyRepoEntry `json:"pullRequestContributionsByRepository"`
	Reviews []historyRepoEntry `json:"pullRequestReviewContributionsByRepository"`
}

type historyRepoEntry struct {
	Repository struct {
		NameWithOwner string `json:"nameWithOwner"`
	} `json:"repository"`
	Contributions struct {
		TotalCount int `json:"totalCount"`
		PageInfo   struct {
			HasNextPage bool `json:"hasNextPage"`
		} `json:"pageInfo"`
		Nodes []struct {
			OccurredAt  time.Time `json:"occurredAt"`
			CommitCount int       `json:"commitCount"`
		} `json:"nodes"`
	} `json:"contributions"`
}

// FetchContributorDailyHistory fetches and merges every window,
// subdividing on cap hits. Errors abort (nothing is fabricated); the
// caller retries the contributor on its next claim.
//
// v0.28.3: windows fetch CONCURRENTLY (bounded by
// SetHistoryWindowConcurrency; default 1 = the legacy serial shape).
// The serial chain was the sweep's throughput floor — a 10-year
// account is ~20 windows ≈ 20 sequential round-trips. Windows are
// independent spans and the accumulator's assignment-semantics merge
// is order-independent, so concurrency needs only the accumulator
// mutex. First error cancels the remaining windows and aborts the
// contributor — unchanged failure semantics, reached faster.
func (c *Client) FetchContributorDailyHistory(ctx context.Context, login string, windows []HistoryWindow) ([]model.ContributorDayActivity, []model.ContributorDayTotal, error) {
	// Background sweep — same reserve rationale as FetchContributorHistoryMeta.
	ctx = platform.WithGraphQLBackgroundBudget(ctx)
	acc := newHistoryAccumulator()
	conc := int(c.historyWindowConc.Load())
	if conc < 1 {
		conc = 1
	}
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, conc)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	for _, w := range windows {
		wg.Add(1)
		go func(w HistoryWindow) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-wctx.Done():
				return
			}
			defer func() { <-sem }()
			if wctx.Err() != nil {
				return
			}
			if err := c.fetchHistoryWindow(wctx, login, w, acc); err != nil {
				errMu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				errMu.Unlock()
				cancel()
			}
		}(w)
	}
	wg.Wait()
	if firstErr != nil {
		return nil, nil, firstErr
	}
	// v0.28.5 (Copilot round): a canceled PARENT context can drain
	// every worker at the semaphore/entry checks without any fetch
	// recording an error — returning the partial accumulator as
	// success would let the scheduler store it AND stamp the
	// contributor backfilled for a full cooldown period, silently
	// truncating their history. Cancellation is a failed fetch.
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	days := make([]model.ContributorDayActivity, 0, len(acc.days))
	for _, r := range acc.days {
		days = append(days, *r)
	}
	totals := make([]model.ContributorDayTotal, 0, len(acc.totals))
	for d, n := range acc.totals {
		totals = append(totals, model.ContributorDayTotal{Day: d, Total: n})
	}
	return days, totals, nil
}

func (c *Client) fetchHistoryWindow(ctx context.Context, login string, w HistoryWindow, acc *historyAccumulator) error {
	query := fmt.Sprintf(`query { user(login: %q) { contributionsCollection(from: %q, to: %q) {
  contributionCalendar { weeks { contributionDays { date contributionCount } } }
  commitContributionsByRepository(maxRepositories: 100) { repository { nameWithOwner } contributions(first: 100) { totalCount pageInfo { hasNextPage } nodes { occurredAt commitCount } } }
  issueContributionsByRepository(maxRepositories: 100) { repository { nameWithOwner } contributions(first: 100) { totalCount pageInfo { hasNextPage } nodes { occurredAt } } }
  pullRequestContributionsByRepository(maxRepositories: 100) { repository { nameWithOwner } contributions(first: 100) { totalCount pageInfo { hasNextPage } nodes { occurredAt } } }
  pullRequestReviewContributionsByRepository(maxRepositories: 100) { repository { nameWithOwner } contributions(first: 100) { totalCount pageInfo { hasNextPage } nodes { occurredAt } } }
} } }`, login, w.From.UTC().Format(time.RFC3339), w.To.UTC().Format(time.RFC3339))

	var resp historyWindowResp
	if err := c.http.GraphQL(ctx, query, nil, &resp); err != nil {
		// chaoss.tv log (2026-09-03..05: 11,003 stuck history fetches):
		// GitHub rejects a too-expensive window with
		// RESOURCE_LIMITS_EXCEEDED — the same overload signal as a cap
		// hit, but delivered as an ERROR (no data) rather than truncated
		// data. Failing the contributor re-fetches the SAME expensive
		// window on every next claim, so a prolific contributor's history
		// sweep never advances. Subdivide instead (a shorter span
		// aggregates fewer contributions and is cheap enough to compute),
		// down to the 48h floor where the span is skipped rather than
		// failing the whole contributor — mirroring the cap-hit path and
		// the classification sweep's fetchActivityWithSubdivide (v0.27.81).
		// chaoss.tv log (2026-09-05): the too-expensive signal arrives in
		// THREE shapes, all "subdivide" — a global RESOURCE_LIMITS_EXCEEDED
		// (ErrResourceLimits → ClassTransient), a retry-budget exhaustion
		// on repeated 500s (ErrTransient → ClassTransient), and a TRUNCATED
		// body whose partial JSON carries RESOURCE_LIMITS_EXCEEDED (a decode
		// error the classifier cannot see). ClassTransient covers the first
		// two; the string check covers the truncated-body case. Same policy
		// as the classification sweep's fetchActivityWithSubdivide (v0.27.81).
		// The explicit RESOURCE_LIMITS_EXCEEDED marker (global message OR a
		// truncated body carrying it) is the ONLY reliable "too expensive"
		// signal. ClassTransient is broader (it also covers outages, DNS
		// failures, request deadlines — platform/errors.go).
		tooExpensive := strings.Contains(err.Error(), "RESOURCE_LIMITS_EXCEEDED")
		// Subdivide a too-expensive OR transiently-failing window above the
		// floor: a shorter span aggregates fewer contributions (recovers the
		// too-expensive-as-500 case) and can survive a mid-window blip.
		// Copilot round 29 (PR #193) DECLINED narrowing this gate to the
		// explicit marker only: the chaoss.tv log shows too-expensive
		// windows presenting as PERSISTENT 500s with no in-body marker
		// (QCADevProd/robinmordasiewicz — "exhausted 10 retries: transient"),
		// and v0.27.81's fetchActivityWithSubdivide subdivides on
		// ClassTransient for exactly that measured shape. Gating on the
		// marker alone would re-strand those windows forever. The
		// reviewer's real hazard — a generic outage recursing to the floor
		// and being SKIPPED as lossy — is closed below: only the explicit
		// marker is lossy at the floor; a generic transient BUBBLES, so an
		// outage costs at most a bounded chain of subdivision attempts
		// (~log2(span/48h) levels) before the contributor fails and
		// retries. Nothing incomplete is ever stamped.
		if w.To.Sub(w.From) >= historyMinWindow &&
			(tooExpensive || platform.ClassifyError(err) == platform.ClassTransient) {
			c.logger.Info("activity history: query too expensive or transient — subdividing window",
				"login", login,
				"from", w.From.Format("2006-01-02"), "to", w.To.Format("2006-01-02"))
			mid := w.From.Add(w.To.Sub(w.From) / 2)
			if serr := c.fetchHistoryWindow(ctx, login, HistoryWindow{From: w.From, To: mid}, acc); serr != nil {
				return serr
			}
			return c.fetchHistoryWindow(ctx, login, HistoryWindow{From: mid, To: w.To}, acc)
		}
		// At the 48h floor. Copilot round 28 (PR #193): ONLY an explicit
		// RESOURCE_LIMITS_EXCEEDED is unrecoverable-and-lossy here — a 48h
		// span GitHub still refuses is genuinely too big, so skip it (data
		// lost) rather than stranding the contributor. A GENERIC transient
		// at the floor (outage/DNS/deadline) is NOT proof the query is too
		// expensive: BUBBLE it so the whole contributor fails and is retried
		// on the next claim, never persisted with silently-incomplete history.
		if tooExpensive {
			c.logger.Info("activity history: query too expensive at minimum window — span skipped",
				"login", login,
				"from", w.From.Format("2006-01-02"), "to", w.To.Format("2006-01-02"))
			return nil
		}
		return fmt.Errorf("contributor history window %q %s→%s: %w", login, w.From.Format("2006-01-02"), w.To.Format("2006-01-02"), err)
	}
	cc := resp.User.ContributionsCollection
	capped := historyWindowCapped(cc)
	if capped != "" {
		if w.To.Sub(w.From) >= historyMinWindow {
			// Subdivide: the halves re-fetch EVERYTHING in this span, so
			// the capped (incomplete) result is discarded, not merged.
			c.logger.Info("activity cap hit — subdividing window",
				"login", login, "cap", capped,
				"from", w.From.Format("2006-01-02"), "to", w.To.Format("2006-01-02"))
			mid := w.From.Add(w.To.Sub(w.From) / 2)
			if err := c.fetchHistoryWindow(ctx, login, HistoryWindow{From: w.From, To: mid}, acc); err != nil {
				return err
			}
			return c.fetchHistoryWindow(ctx, login, HistoryWindow{From: mid, To: w.To}, acc)
		}
		c.logger.Info("activity cap hit at minimum window — data lost",
			"login", login, "cap", capped,
			"from", w.From.Format("2006-01-02"), "to", w.To.Format("2006-01-02"))
		// Fall through: keep what WAS returned — partial beats nothing.
	}

	// v0.28.3: the merge region locks — windows merge concurrently.
	acc.mu.Lock()
	defer acc.mu.Unlock()
	for _, week := range cc.ContributionCalendar.Weeks {
		for _, d := range week.ContributionDays {
			if d.ContributionCount > 0 {
				acc.totals[d.Date] = d.ContributionCount
			}
		}
	}
	// Merge with ASSIGNMENT semantics, not accumulation: adjacent
	// windows share a boundary instant and the API is day-bucketed, so
	// a boundary day can legitimately arrive from BOTH windows with
	// identical values — assignment makes that (and any re-fetch)
	// idempotent, where += would double-count. Within one window, the
	// per-item types are counted locally first, then assigned.
	localCount := func(entries []historyRepoEntry) map[[2]string]int {
		counts := map[[2]string]int{}
		for _, e := range entries {
			for _, n := range e.Contributions.Nodes {
				key := [2]string{n.OccurredAt.Format("2006-01-02"), e.Repository.NameWithOwner}
				if n.CommitCount > 0 {
					counts[key] = n.CommitCount // per-(day,repo) rollup node
				} else {
					counts[key]++ // per-item node (issues/PRs/reviews)
				}
			}
		}
		return counts
	}
	for key, n := range localCount(cc.Commits) {
		acc.row(key[0], key[1]).Commits = n
	}
	for key, n := range localCount(cc.Issues) {
		acc.row(key[0], key[1]).Issues = n
	}
	for key, n := range localCount(cc.PRs) {
		acc.row(key[0], key[1]).PRs = n
	}
	for key, n := range localCount(cc.Reviews) {
		acc.row(key[0], key[1]).Reviews = n
	}
	return nil
}

// historyWindowCapped reports which cap (if any) a window response hit:
// the 100-repositories-per-type list cap, or an overflowing
// contributions page. Empty string = complete.
func historyWindowCapped(cc historyCollection) string {
	for name, entries := range map[string][]historyRepoEntry{
		"commit": cc.Commits, "issue": cc.Issues, "pull_request": cc.PRs, "review": cc.Reviews,
	} {
		if len(entries) >= 100 {
			return name + "-repo-cap"
		}
		for _, e := range entries {
			if e.Contributions.PageInfo.HasNextPage {
				return name + "-page-overflow"
			}
		}
	}
	return ""
}
