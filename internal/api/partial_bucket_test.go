// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// summary/18 Phase 2 (v0.27.39): the compare window must end at the
// last COMPLETE bucket. Pre-fix, the in-progress week/month was served
// as a full data point: the last point of every active repo's series
// drooped, the GUI's OLS fit consumed the partial bucket as real data
// (biasing slope negative), and the ±2σ tube painted a phantom red
// anomaly dot on "today" for healthy repos.

package api

import (
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestCompareWindowTruncatesPartialBucket(t *testing.T) {
	// Default until (= now): the window must end at the CURRENT
	// bucket's start, so the last emitted bucket is the previous,
	// complete one.
	r := httptest.NewRequest("GET", "/api/v1/compare?metric=commits&entities=repo:1", nil)
	_, until, bucket, err := compareWindow(r)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := until, truncBucket(time.Now().UTC(), bucket); !got.Equal(want) {
		t.Errorf("default until must truncate to the current bucket start %v, got %v — serving the in-progress bucket as a full point is the trend-droop artifact", want, got)
	}

	// Explicit mid-bucket until: same rule — the bucket containing it
	// would only be partially covered by the query window.
	r = httptest.NewRequest("GET", "/api/v1/compare?until=2026-07-09&bucket=week", nil) // a Thursday
	_, until, _, err = compareWindow(r)
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 7, 6, 0, 0, 0, 0, time.UTC) // that week's Monday
	if !until.Equal(want) {
		t.Errorf("explicit mid-week until must truncate to its bucket start %v, got %v", want, until)
	}

	// Month buckets truncate to the 1st.
	r = httptest.NewRequest("GET", "/api/v1/compare?until=2026-07-09&bucket=month", nil)
	_, until, _, err = compareWindow(r)
	if err != nil {
		t.Fatal(err)
	}
	want = time.Date(2026, 7, 1, 0, 0, 0, 0, time.UTC)
	if !until.Equal(want) {
		t.Errorf("month bucket until must truncate to the 1st, got %v", until)
	}
}

// TestCompareWindowDefaultSinceIsBucketAligned — v0.29.71 review round 1
// (F1): the default since was now minus three years, a new instant every
// second, and the per-entity series cache keys on since; the GUI omits
// since at its default window, so every compare and repo-page series
// request missed and each miss filled the shared cache. Round 2 (R2-4):
// three years before a Monday is mid-week, so the first point was a
// partial bucket. The default is the bucket start three years before the
// truncated until: aligned, and stable for a whole bucket.
func TestCompareWindowDefaultSinceIsBucketAligned(t *testing.T) {
	for _, q := range []string{"bucket=week", "bucket=month", "until=2026-07-09&bucket=week"} {
		r := httptest.NewRequest("GET", "/api/v1/compare?"+q, nil)
		since, until, bucket, err := compareWindow(r)
		if err != nil {
			t.Fatal(err)
		}
		if want := truncBucket(until.AddDate(-3, 0, 0), bucket); !since.Equal(want) {
			t.Errorf("%s: default since = %v, want the bucket start three years before the truncated until (%v)", q, since, want)
		}
	}
	// An explicit since is kept as given.
	r := httptest.NewRequest("GET", "/api/v1/compare?since=2024-01-03&bucket=week", nil)
	if since, _, _, err := compareWindow(r); err != nil || !since.Equal(time.Date(2024, 1, 3, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("explicit since = %v, %v", since, err)
	}
}

// 2026-10-07: the compare page's explicit until is bounded by the latest
// plausible commit time too — the one other caller-supplied commit window.
func TestCompareWindowUntilIsBounded(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/compare?metric=commits&entities=repo:1&until=2081-01-01", nil)
	_, until, bucket, err := compareWindow(r)
	if err != nil {
		t.Fatal(err)
	}
	bound := db.LatestPlausibleCommitTime(time.Now())
	if until.After(bound) {
		t.Fatalf("until=2081 must be clamped to the plausible bound %v (then bucket-truncated), got %v (%s)", bound, until, bucket)
	}
}

func TestCompareWindowSinceIsBounded(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/compare?metric=commits&entities=repo:1&since=0001-01-01", nil)
	since, _, _, err := compareWindow(r)
	if err != nil {
		t.Fatal(err)
	}
	if since.Before(db.EarliestPlausibleCommitTime()) {
		t.Fatalf("since=0001 must be clamped to the plausible floor, got %v", since)
	}
}

// Review round 3 F1: the DEFAULT since (three years before an early until)
// is clamped too, rounded up to a whole bucket.
func TestCompareWindowDefaultSinceIsBounded(t *testing.T) {
	r := httptest.NewRequest("GET", "/api/v1/compare?metric=commits&entities=repo:1&until=1972-06-01", nil)
	since, _, bucket, err := compareWindow(r)
	if err != nil {
		t.Fatal(err)
	}
	floor := db.EarliestPlausibleCommitTime()
	if since.Before(floor) {
		t.Fatalf("the default since derived from until=1972 must not fall below the floor %v, got %v", floor, since)
	}
	if !since.Equal(truncBucket(since, bucket)) {
		t.Fatalf("the clamped default since must be a whole bucket start, got %v (%s)", since, bucket)
	}
}
