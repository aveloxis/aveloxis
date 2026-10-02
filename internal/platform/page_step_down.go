// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Page-size step-down (v0.29.72). A forge serves each REST request within a
// fixed time budget (GitHub's is ~10 s), and a page's cost is the sum of its
// items' render cost. The 2026-10-02 log review traced five of the fleet's
// largest repositories failing every cycle to one listing: GitHub's
// repo-wide /pulls/comments answers 502 after ~10-11 s on every attempt for
// pages of OLD review comments at per_page=100 (direction=asc page=1 fails
// too, so it is not offset depth), while the same offset at per_page=50 or
// 25 answers 200 in 3-8 s. A retry of the identical URL can never succeed;
// a smaller page at the same item offset does. Probes and the log evidence:
// summary/changelog/v0.29.md, the v0.29.72 entry.
//
// A listing opts in with WithPageSizeStepDown, naming the forge's request
// budget. Under it, Get gives up on a URL after stepDownAfterGatewayErrors
// 502/504 answers that each took at least that budget (ErrPageTooSlow)
// instead of spending the whole retry budget, and paginate re-requests the
// SAME item offset at the next smaller size on pageSizeLadder, keeping that
// size until it can step back up.
//
// A slow 502 is not proof that the page is too big: a degraded forge whose
// backends hang answers EVERY request after its timer (review round 2). So
// the step is not permanent: after a step the walk probes the next larger
// size at the first offset that size divides, and each probe that is still
// over budget doubles the pages the walk waits before probing that size
// again. The wait is kept per size: a successful probe to one size resets
// only that size's wait (review round 3). An incident that passed is
// climbed out of at once; for each size, a region where it is really over
// budget costs failed probes that grow with the log of the region's length.
// Only the probes are logarithmic: in a region of mixed density each window
// too dense for the current size still costs one step-down (two over-budget
// answers) when the walk reaches it (review round 4).
// And a page still over budget at the floor gets the ordinary retry budget
// before the listing fails, so a long incident cannot fail the walk sooner
// than it did before the step-down existed.

// ErrPageTooSlow marks a page the forge answered with 502/504, each after
// running out its request budget, on stepDownAfterGatewayErrors attempts
// under
// WithPageSizeStepDown: the page costs more than the forge's request
// budget. paginate steps the page size down on it; at the floor of
// pageSizeLadder it reaches the caller wrapped together with ErrTransient,
// so it classifies like any exhausted retry.
var ErrPageTooSlow = errors.New("page exceeded the forge's request time budget")

// stepDownAfterGatewayErrors is how many 502/504 answers to one page Get
// accepts before it reports ErrPageTooSlow. The first cannot tell a
// passing gateway blip from a page over budget; a second on the same URL is
// the evidence, because an over-budget page fails on every attempt (the
// 2026-10-02 probes: ten of ten, across runs a day apart). One retry keeps a
// blip from shrinking the walk; more would only delay the step, each one a
// full ~10 s forge timeout plus backoff.
const stepDownAfterGatewayErrors = 2

// pageSizeLadder is the sequence of page sizes a stepping walk descends.
// Each size divides the one before it, so the item offset of a failed page,
// (page-1)*per_page, is always a whole page at the next size: the walk
// resumes on exactly the item it failed on, with no gap and no duplicate.
// 100 is GitHub's and GitLab's maximum page size; 10 is skipped because it
// does not divide 25; 1 is the floor, below which a page cannot shrink.
var pageSizeLadder = []int{100, 50, 25, 5, 1}

// GitHubRequestBudget is GitHub's documented REST processing limit: "If
// GitHub takes more than 10 seconds to process an API request, GitHub will
// terminate the request and you will receive a timeout response" (REST API
// best practices, "Timeouts"). The 2026-10-02 over-budget pages answered 502
// after 10-11 s, as the limit predicts.
const GitHubRequestBudget = 10 * time.Second

type ctxKeyPageStepDown struct{}

// WithPageSizeStepDown returns a derived context under which paginate steps
// the page size down when a page exceeds the forge's request budget: a
// 502/504 that arrived after at least budget, on
// stepDownAfterGatewayErrors attempts. The duration rules out the fast
// failures. A page over budget fails only once the forge's timer runs out,
// and the client's measured time includes the server's, so it is never
// shorter than the budget. A hard outage or a gateway blip answers fast and
// keeps the plain retry budget at the page's own size (review round 1 on
// v0.29.72: on status alone a burst of fast 502s walked the ladder in ~7 s
// and finished the walk at 1/20-1/100 of its page size). A degraded forge
// answers slowly and does step the walk down; the step-up and the floor's
// ordinary retry budget (above) bound what that costs. Opt in per
// listing whose items can be expensive for the forge to render; the review
// comment listings are the measured case (GitHubRequestBudget). A
// non-positive budget disables the step-down.
func WithPageSizeStepDown(ctx context.Context, budget time.Duration) context.Context {
	return context.WithValue(ctx, ctxKeyPageStepDown{}, budget)
}

// pageStepDownBudget returns the request budget set by WithPageSizeStepDown
// and whether the step-down is on.
func pageStepDownBudget(ctx context.Context) (time.Duration, bool) {
	v, _ := ctx.Value(ctxKeyPageStepDown{}).(time.Duration)
	return v, v > 0
}

// isRequestBudgetStatus reports whether a status is the forge's answer to a
// request that ran out of time: 502 (GitHub's usual answer for its 10 s
// budget) or 504 (seen on the same URLs). 500 and 503 are an error and an
// outage, not a page that is too big, and keep the full retry budget.
func isRequestBudgetStatus(code int) bool {
	return code == http.StatusBadGateway || code == http.StatusGatewayTimeout
}

// parsePage reads a listing path's per_page and page (page defaults to 1).
// ok is false when per_page is missing or either value is unusable.
func parsePage(path string) (perPage, page int, ok bool) {
	_, rawQuery, _ := strings.Cut(path, "?")
	q, err := url.ParseQuery(rawQuery)
	if err != nil {
		return 0, 0, false
	}
	perPage, err = strconv.Atoi(q.Get("per_page"))
	if err != nil || perPage < 1 {
		return 0, 0, false
	}
	page = 1
	if p := q.Get("page"); p != "" {
		page, err = strconv.Atoi(p)
		if err != nil || page < 1 {
			return 0, 0, false
		}
	}
	return perPage, page, true
}

// resizePage rewrites a listing path to request the same item offset at
// size. ok is false when the path has no usable per_page/page or size does
// not divide the offset (the page would not start on the same item).
func resizePage(path string, size int) (newPath string, offset int, ok bool) {
	perPage, page, ok := parsePage(path)
	if !ok || size < 1 {
		return "", 0, false
	}
	offset = (page - 1) * perPage
	if offset%size != 0 {
		return "", offset, false
	}
	newPath = setQueryParam(path, "per_page", strconv.Itoa(size))
	newPath = setQueryParam(newPath, "page", strconv.Itoa(offset/size+1))
	return newPath, offset, true
}

// stepUpSize is the next size a stepped-down walk probes: the smallest
// ladder size above cur that is not above the walk's starting size, or 0
// when there is none (the walk is back at its size, or its start is off the
// ladder and cur is the largest ladder size under it).
func stepUpSize(cur, start int) int {
	for i := len(pageSizeLadder) - 1; i >= 0; i-- {
		if size := pageSizeLadder[i]; size > cur && size <= start {
			return size
		}
	}
	return 0
}

// stepDownPage rewrites a listing path to request the same item offset at
// the next smaller page size. It returns the new path, the old and new
// sizes and the item offset, or ok=false: atFloor when the page is already
// at the smallest size, otherwise the path has no usable per_page/page to
// step from (paginate logs the two differently — review round 1). The new
// size is the largest ladder size below the current one that divides the
// offset, so a per_page off the ladder still resumes exactly.
func stepDownPage(path string) (newPath string, from, to, offset int, ok, atFloor bool) {
	from, page, parsed := parsePage(path)
	if !parsed {
		return "", 0, 0, 0, false, false
	}
	offset = (page - 1) * from
	for _, size := range pageSizeLadder {
		if size < from && offset%size == 0 {
			to = size
			break
		}
	}
	if to == 0 {
		return "", from, 0, offset, false, true
	}
	newPath, _, _ = resizePage(path, to) // to divides offset by construction
	return newPath, from, to, offset, true, false
}

// serverErrorRetrySleep is Get's backoff wait after a 5xx answer. A
// package var so tests can drive retry sequences without real waits;
// production value only.
var serverErrorRetrySleep = func(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

// SetServerErrorSleepForTest swaps Get's 5xx backoff wait and returns the
// restore function, for t.Cleanup. Tests only.
func SetServerErrorSleepForTest(f func(ctx context.Context, d time.Duration) error) (restore func()) {
	old := serverErrorRetrySleep
	serverErrorRetrySleep = f
	return func() { serverErrorRetrySleep = old }
}
