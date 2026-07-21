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
