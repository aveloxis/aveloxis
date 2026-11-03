// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// trafficDay is midnight UTC of a day in October 2026, the month of the
// traffic shapes these tests are built from.
func trafficDay(day int) time.Time {
	return time.Date(2026, time.October, day, 0, 0, 0, 0, time.UTC)
}

// trafficViewDays returns n consecutive daily views buckets starting at
// first, with non-zero counts.
func trafficViewDays(first time.Time, n int) []model.RepoView {
	views := make([]model.RepoView, n)
	for i := range views {
		views[i] = model.RepoView{
			Timestamp:   first.AddDate(0, 0, i),
			TotalViews:  10 + i,
			UniqueViews: 2 + i%4,
		}
	}
	return views
}

// trafficCloneDays returns n consecutive daily clones buckets starting
// at first, with non-zero counts.
func trafficCloneDays(first time.Time, n int) []model.RepoClone {
	clones := make([]model.RepoClone, n)
	for i := range clones {
		uniques := 1 + i%3
		clones[i] = model.RepoClone{
			Timestamp:    first.AddDate(0, 0, i),
			TotalClones:  3 + i,
			UniqueClones: &uniques,
		}
	}
	return clones
}

// trafficWeekBuckets returns one weekly bucket of kind per start, with
// the uniques given in the same order.
func trafficWeekBuckets(kind model.TrafficKind, starts []time.Time, uniques []int) []model.RepoTrafficWeek {
	weeks := make([]model.RepoTrafficWeek, len(starts))
	for i, s := range starts {
		weeks[i] = model.RepoTrafficWeek{Kind: kind, WeekStart: s, Uniques: uniques[i]}
	}
	return weeks
}

// zeroTrafficFixture builds the inputs of a read of the repository
// without traffic recorded in internal/platform/github/testdata: the
// daily views and the 14-day views uniques from views?per=day, the
// weekly buckets from views?per=week and clones?per=week, the 14-day
// clones uniques from clones?per=week, and the daily clones from
// clones?per=day. The fixture holds real GitHub responses read on
// Monday 2026-10-19 (UTC): 14 zero-filled daily buckets from 2026-10-06
// to 2026-10-19, and weekly buckets labelled 2026-10-06 (the window
// start, a Tuesday), 2026-10-12 and 2026-10-19.
func zeroTrafficFixture(t *testing.T) (*model.RepoTraffic, []model.RepoClone) {
	t.Helper()
	type bucket struct {
		Timestamp time.Time `json:"timestamp"`
		Count     int       `json:"count"`
		Uniques   int       `json:"uniques"`
	}
	type answer struct {
		Uniques int      `json:"uniques"`
		Views   []bucket `json:"views"`
		Clones  []bucket `json:"clones"`
	}
	read := func(name string) answer {
		t.Helper()
		var a answer
		body := srctest.Read(t, "internal/platform/github/testdata/"+name)
		if err := json.Unmarshal([]byte(body), &a); err != nil {
			t.Fatalf("decode %s: %v", name, err)
		}
		return a
	}
	viewsDay := read("traffic_zero_views_day.json")
	viewsWeek := read("traffic_zero_views_week.json")
	clonesDay := read("traffic_zero_clones_day.json")
	clonesWeek := read("traffic_zero_clones_week.json")

	tr := &model.RepoTraffic{
		ViewUniques:  viewsDay.Uniques,
		CloneUniques: clonesWeek.Uniques,
	}
	for _, b := range viewsDay.Views {
		tr.Views = append(tr.Views, model.RepoView{Timestamp: b.Timestamp, TotalViews: b.Count, UniqueViews: b.Uniques})
	}
	for _, b := range viewsWeek.Views {
		tr.WeeklyViews = append(tr.WeeklyViews, model.RepoTrafficWeek{Kind: model.TrafficKindViews, WeekStart: b.Timestamp, Uniques: b.Uniques})
	}
	for _, b := range clonesWeek.Clones {
		tr.WeeklyClones = append(tr.WeeklyClones, model.RepoTrafficWeek{Kind: model.TrafficKindClones, WeekStart: b.Timestamp, Uniques: b.Uniques})
	}
	var clones []model.RepoClone
	for _, b := range clonesDay.Clones {
		clones = append(clones, model.RepoClone{Timestamp: b.Timestamp, TotalClones: b.Count, UniqueClones: &b.Uniques})
	}
	// Guard the denominator: a fixture that decoded to nothing would
	// make every expectation below about an empty read.
	if len(tr.Views) != 14 || len(clones) != 14 || len(tr.WeeklyViews) != 3 || len(tr.WeeklyClones) != 3 {
		t.Fatalf("zero-traffic fixture decoded to %d daily views, %d daily clones, %d weekly views, %d weekly clones; want 14, 14, 3, 3",
			len(tr.Views), len(clones), len(tr.WeeklyViews), len(tr.WeeklyClones))
	}
	return tr, clones
}

func formatTrafficWindow(w model.RepoTrafficWindow) string {
	return fmt.Sprintf("%s %s..%s uniques=%d", w.Kind, w.Start.Format(time.RFC3339), w.End.Format(time.RFC3339), w.Uniques)
}

func formatTrafficWeek(w model.RepoTrafficWeek) string {
	return fmt.Sprintf("%s %s uniques=%d", w.Kind, w.WeekStart.Format(time.RFC3339), w.Uniques)
}

// TestDeriveTrafficRowsKeepsWindowsAndCompleteWeeks pins how one read
// becomes window and weekly rows: per kind, a window from the first to
// the last daily bucket with the 14-day uniques, and the weekly buckets
// that start on a Monday and lie entirely inside that window, all in
// UTC. Nothing is dropped for being 0. Expected rows are written in
// RFC 3339, so a row left in another time zone fails as well as a
// wrong day.
func TestDeriveTrafficRowsKeepsWindowsAndCompleteWeeks(t *testing.T) {
	zeroTraffic, zeroClones := zeroTrafficFixture(t)

	// Five hours behind UTC: midnight UTC is 19:00 on the day before,
	// so every Monday bucket reads as a Sunday in this zone.
	behind := time.FixedZone("UTC-5", -5*60*60)
	// Two hours ahead of UTC: midnight UTC is 02:00 the same day.
	ahead := time.FixedZone("UTC+2", 2*60*60)

	cases := []struct {
		name        string
		traffic     *model.RepoTraffic
		snapshot    time.Time
		clones      []model.RepoClone
		wantWindows []string
		wantWeeks   []string
	}{
		{
			// GitHub returned 14 zero-filled daily buckets per kind,
			// 2026-10-05 to 2026-10-18 (the 14 UTC days before the
			// collection day), for repositories with traffic read on
			// 2026-10-19; the weekly buckets are labelled with the
			// Mondays of those days. Counts are illustrative.
			name: "repository with traffic read on 2026-10-19",
			traffic: &model.RepoTraffic{
				Views:        trafficViewDays(trafficDay(5), 14),
				ViewUniques:  37,
				CloneUniques: 9,
				WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, []time.Time{trafficDay(5), trafficDay(12)}, []int{21, 25}),
				WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, []time.Time{trafficDay(5), trafficDay(12)}, []int{5, 6}),
			},
			clones:   trafficCloneDays(trafficDay(5), 14),
			snapshot: trafficDay(19),
			wantWindows: []string{
				"views 2026-10-05T00:00:00Z..2026-10-18T00:00:00Z uniques=37",
				"clones 2026-10-05T00:00:00Z..2026-10-18T00:00:00Z uniques=9",
			},
			wantWeeks: []string{
				"views 2026-10-05T00:00:00Z uniques=21",
				"views 2026-10-12T00:00:00Z uniques=25",
				"clones 2026-10-05T00:00:00Z uniques=5",
				"clones 2026-10-12T00:00:00Z uniques=6",
			},
		},
		{
			// The recorded repository without traffic: the window ends
			// on the collection day, the Tuesday-labelled first bucket
			// and the 2026-10-19 bucket are partial weeks, and every
			// row is kept with uniques 0.
			name:     "zero-traffic fixture",
			traffic:  zeroTraffic,
			clones:   zeroClones,
			snapshot: trafficDay(19),
			wantWindows: []string{
				"views 2026-10-06T00:00:00Z..2026-10-19T00:00:00Z uniques=0",
				"clones 2026-10-06T00:00:00Z..2026-10-19T00:00:00Z uniques=0",
			},
			wantWeeks: []string{
				"views 2026-10-12T00:00:00Z uniques=0",
				"clones 2026-10-12T00:00:00Z uniques=0",
			},
		},
		{
			// A window from Thursday 2026-10-08 to Wednesday 2026-10-21
			// whose first weekly bucket is labelled with the preceding
			// Monday, 2026-10-05: that week starts before the window,
			// and the 2026-10-19 week ends after it.
			name: "mid-week window with the first bucket on the preceding Monday",
			traffic: &model.RepoTraffic{
				Views:        trafficViewDays(trafficDay(8), 14),
				ViewUniques:  15,
				CloneUniques: 4,
				WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, []time.Time{trafficDay(5), trafficDay(12), trafficDay(19)}, []int{4, 7, 2}),
				WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, []time.Time{trafficDay(5), trafficDay(12), trafficDay(19)}, []int{1, 3, 1}),
			},
			clones:   trafficCloneDays(trafficDay(8), 14),
			snapshot: trafficDay(21),
			wantWindows: []string{
				"views 2026-10-08T00:00:00Z..2026-10-21T00:00:00Z uniques=15",
				"clones 2026-10-08T00:00:00Z..2026-10-21T00:00:00Z uniques=4",
			},
			wantWeeks: []string{
				"views 2026-10-12T00:00:00Z uniques=7",
				"clones 2026-10-12T00:00:00Z uniques=3",
			},
		},
		{
			// The read of 2026-10-19 with the same instants written in
			// other offsets: the Monday buckets of the views are
			// Sundays at 19:00 in UTC-5, the clones are at 02:00 in
			// UTC+2. Weekdays and dates are taken in UTC, and the rows
			// come out in UTC.
			name: "timestamps with a non-UTC offset",
			traffic: &model.RepoTraffic{
				Views:        trafficViewDays(trafficDay(5).In(behind), 14),
				ViewUniques:  37,
				CloneUniques: 9,
				WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, []time.Time{trafficDay(5).In(behind), trafficDay(12).In(behind)}, []int{21, 25}),
				WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, []time.Time{trafficDay(5).In(ahead), trafficDay(12).In(ahead)}, []int{5, 6}),
			},
			clones:   trafficCloneDays(trafficDay(5).In(ahead), 14),
			snapshot: trafficDay(19),
			wantWindows: []string{
				"views 2026-10-05T00:00:00Z..2026-10-18T00:00:00Z uniques=37",
				"clones 2026-10-05T00:00:00Z..2026-10-18T00:00:00Z uniques=9",
			},
			wantWeeks: []string{
				"views 2026-10-05T00:00:00Z uniques=21",
				"views 2026-10-12T00:00:00Z uniques=25",
				"clones 2026-10-05T00:00:00Z uniques=5",
				"clones 2026-10-12T00:00:00Z uniques=6",
			},
		},
		{
			// Each kind is dated by its own daily buckets. The days
			// differ here so a window dated from the other kind's
			// buckets fails: views from 2026-10-05, clones from
			// 2026-10-06 with a Tuesday-labelled first weekly bucket.
			name: "each kind dated by its own daily buckets",
			traffic: &model.RepoTraffic{
				Views:        trafficViewDays(trafficDay(5), 14),
				ViewUniques:  37,
				CloneUniques: 9,
				WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, []time.Time{trafficDay(5), trafficDay(12)}, []int{21, 25}),
				WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, []time.Time{trafficDay(6), trafficDay(12), trafficDay(19)}, []int{4, 6, 1}),
			},
			clones:   trafficCloneDays(trafficDay(6), 14),
			snapshot: trafficDay(19),
			wantWindows: []string{
				"views 2026-10-05T00:00:00Z..2026-10-18T00:00:00Z uniques=37",
				"clones 2026-10-06T00:00:00Z..2026-10-19T00:00:00Z uniques=9",
			},
			wantWeeks: []string{
				"views 2026-10-05T00:00:00Z uniques=21",
				"views 2026-10-12T00:00:00Z uniques=25",
				"clones 2026-10-12T00:00:00Z uniques=6",
			},
		},
		{
			// The daily clones come from a separate read; when it
			// fails there are no clones buckets to date a window, so
			// the read leaves no clones window and no clones weeks,
			// and the views rows are still derived.
			name: "no daily clones",
			traffic: &model.RepoTraffic{
				Views:        trafficViewDays(trafficDay(5), 14),
				ViewUniques:  37,
				CloneUniques: 9,
				WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, []time.Time{trafficDay(5), trafficDay(12)}, []int{21, 25}),
				WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, []time.Time{trafficDay(5), trafficDay(12)}, []int{5, 6}),
			},
			clones:   nil,
			snapshot: trafficDay(19),
			wantWindows: []string{
				"views 2026-10-05T00:00:00Z..2026-10-18T00:00:00Z uniques=37",
			},
			wantWeeks: []string{
				"views 2026-10-05T00:00:00Z uniques=21",
				"views 2026-10-12T00:00:00Z uniques=25",
			},
		},
		{
			// A zero-traffic read on Sunday 2026-10-25 (UTC): the
			// window ends on the collection day, so it contains the
			// current Monday 2026-10-19 bucket, which is still
			// counting and whose uniques (0) must not be stored as a
			// complete week. The previous Monday 2026-10-12 week ended
			// on 2026-10-18 and is kept.
			name: "zero-traffic read on a Sunday rejects the still-counting week",
			traffic: &model.RepoTraffic{
				Views:        trafficViewDays(trafficDay(12), 14),
				ViewUniques:  0,
				CloneUniques: 0,
				WeeklyViews:  trafficWeekBuckets(model.TrafficKindViews, []time.Time{trafficDay(12), trafficDay(19)}, []int{0, 0}),
				WeeklyClones: trafficWeekBuckets(model.TrafficKindClones, []time.Time{trafficDay(12), trafficDay(19)}, []int{0, 0}),
			},
			clones:   trafficCloneDays(trafficDay(12), 14),
			snapshot: trafficDay(25),
			wantWindows: []string{
				"views 2026-10-12T00:00:00Z..2026-10-25T00:00:00Z uniques=0",
				"clones 2026-10-12T00:00:00Z..2026-10-25T00:00:00Z uniques=0",
			},
			wantWeeks: []string{
				"views 2026-10-12T00:00:00Z uniques=0",
				"clones 2026-10-12T00:00:00Z uniques=0",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tc.traffic.SnapshotDate = tc.snapshot
			windows, weeks := deriveTrafficRows(tc.traffic, tc.clones)

			gotWindows := make([]string, len(windows))
			for i, w := range windows {
				gotWindows[i] = formatTrafficWindow(w)
			}
			gotWeeks := make([]string, len(weeks))
			for i, w := range weeks {
				gotWeeks[i] = formatTrafficWeek(w)
			}
			assertTrafficRows(t, "windows", gotWindows, tc.wantWindows)
			assertTrafficRows(t, "weeks", gotWeeks, tc.wantWeeks)
		})
	}
}

func assertTrafficRows(t *testing.T, what string, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s: got %d rows, want %d\n got: %q\nwant: %q", what, len(got), len(want), got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d] = %q, want %q", what, i, got[i], want[i])
		}
	}
}

// TestStagedCloneKeepsAbsentUniquesAbsent: the staging table keeps a
// clone day as JSON between the collection step and the processor, so a
// day without a distinct count must come back without one, not as 0,
// and a staged day that has one (also one staged before the count could
// be absent) must keep it, 0 included.
func TestStagedCloneKeepsAbsentUniquesAbsent(t *testing.T) {
	zero, three := 0, 3
	for _, tc := range []struct {
		name string
		in   model.RepoClone
	}{
		{"absent", model.RepoClone{Timestamp: trafficDay(5), TotalClones: 4}},
		{"zero", model.RepoClone{Timestamp: trafficDay(5), TotalClones: 4, UniqueClones: &zero}},
		{"number", model.RepoClone{Timestamp: trafficDay(5), TotalClones: 4, UniqueClones: &three}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(tc.in)
			if err != nil {
				t.Fatal(err)
			}
			var out model.RepoClone
			if err := json.Unmarshal(payload, &out); err != nil {
				t.Fatal(err)
			}
			switch {
			case tc.in.UniqueClones == nil && out.UniqueClones != nil:
				t.Errorf("UniqueClones came back as %d from %s, want absent", *out.UniqueClones, payload)
			case tc.in.UniqueClones != nil && (out.UniqueClones == nil || *out.UniqueClones != *tc.in.UniqueClones):
				t.Errorf("UniqueClones came back as %v from %s, want %d", out.UniqueClones, payload, *tc.in.UniqueClones)
			}
		})
	}

	var old model.RepoClone
	if err := json.Unmarshal([]byte(`{"Timestamp":"2026-10-05T00:00:00Z","TotalClones":4,"UniqueClones":2}`), &old); err != nil {
		t.Fatal(err)
	}
	if old.UniqueClones == nil || *old.UniqueClones != 2 {
		t.Errorf("a row staged with a plain number came back as %v, want 2", old.UniqueClones)
	}
}
