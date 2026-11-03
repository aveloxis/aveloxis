// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// The testdata/traffic_*.json fixtures come from two authorities, and
// the tests below pin the facts of each that the traffic code relies
// on, so a refreshed fixture with another shape fails here first.
//
// traffic_openapi_*.json are GitHub's own example responses, taken
// verbatim from GitHub's official OpenAPI description
// (github/rest-api-description, MIT license), file
// descriptions/api.github.com/api.github.com.json at commit
// 755ee33b4a81d51f6f07a902cce26cbec3b822f2 (2026-10-18). Each file is
// the example's "value", extracted with jq (two-space indent, values
// unchanged):
//
//	jq '.components.examples["view-traffic"].value'           > traffic_openapi_views.json
//	jq '.components.examples["clone-traffic"].value'          > traffic_openapi_clones.json
//	jq '.components.examples["content-traffic-items"].value'  > traffic_openapi_popular_paths.json
//	jq '.components.examples["referrer-traffic-items"].value' > traffic_openapi_popular_referrers.json
//
// They are the 200 examples of GET /repos/{owner}/{repo}/traffic/views,
// .../traffic/clones, .../traffic/popular/paths and
// .../traffic/popular/referrers. The description has one example per
// endpoint, for the default per=day, and none for per=week. Its daily
// examples hold 15 buckets (2016-10-10 to 2016-10-24); the live API
// returns 14 zero-filled buckets, so tests that need the live 14-day
// shape with traffic build their inputs in the test.
//
// traffic_zero_*.json are real responses of a repository nobody viewed
// or cloned in the 14 days before the capture, fetched with read-only
// `gh api` calls on Monday 2026-10-19 (UTC) and committed verbatim; all
// their counts are 0:
//
//	traffic_zero_views_day.json         GET .../traffic/views?per=day
//	traffic_zero_views_week.json        GET .../traffic/views?per=week
//	traffic_zero_clones_day.json        GET .../traffic/clones?per=day
//	traffic_zero_clones_week.json       GET .../traffic/clones?per=week
//	traffic_zero_popular_paths.json     GET .../traffic/popular/paths
//	traffic_zero_popular_referrers.json GET .../traffic/popular/referrers
//
// For a repository without any traffic GitHub's window ends ON the
// collection day (2026-10-06 to 2026-10-19), and its first weekly
// bucket is labeled with the window start, a Tuesday (2026-10-06,
// 2026-10-12, 2026-10-19), so window dates must come from the buckets,
// never from the collection date.

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

type trafficFixtureBucket struct {
	Timestamp time.Time `json:"timestamp"`
	Count     int       `json:"count"`
	Uniques   int       `json:"uniques"`
}

type trafficFixtureSeries struct {
	Count   int                    `json:"count"`
	Uniques int                    `json:"uniques"`
	Views   []trafficFixtureBucket `json:"views"`
	Clones  []trafficFixtureBucket `json:"clones"`
}

type trafficFixturePath struct {
	Path    string `json:"path"`
	Title   string `json:"title"`
	Count   int    `json:"count"`
	Uniques int    `json:"uniques"`
}

type trafficFixtureReferrer struct {
	Referrer string `json:"referrer"`
	Count    int    `json:"count"`
	Uniques  int    `json:"uniques"`
}

var trafficFixtureKinds = []string{"views", "clones"}

func trafficFixtureDate(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func readTrafficFixture(t *testing.T, name string, v any) {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("reading traffic fixture: %v", err)
	}
	if err := json.Unmarshal(data, v); err != nil {
		t.Fatalf("decoding traffic fixture %s: %v", name, err)
	}
}

// series returns the buckets of a views or clones response.
func (s trafficFixtureSeries) series(kind string) []trafficFixtureBucket {
	if kind == "views" {
		return s.Views
	}
	return s.Clones
}

// assertConsecutiveDays checks that buckets are want consecutive UTC
// days starting at start.
func assertConsecutiveDays(t *testing.T, name string, buckets []trafficFixtureBucket, start time.Time, want int) {
	t.Helper()
	if len(buckets) != want {
		t.Fatalf("%s: %d daily buckets, want %d", name, len(buckets), want)
	}
	for i, b := range buckets {
		day := start.AddDate(0, 0, i)
		if !b.Timestamp.Equal(day) {
			t.Errorf("%s: bucket %d at %s, want %s", name, i, b.Timestamp, day)
		}
	}
}

func TestTrafficFixtures_OpenAPIDailyExamplesAreConsecutiveDays(t *testing.T) {
	for _, kind := range trafficFixtureKinds {
		name := "traffic_openapi_" + kind + ".json"
		var s trafficFixtureSeries
		readTrafficFixture(t, name, &s)
		assertConsecutiveDays(t, name, s.series(kind), trafficFixtureDate(2016, time.October, 10), 15)
	}
}

// The window's count is the sum of its daily counts, so a total never
// needs storing; its uniques are distinct visitors or cloners over the
// whole window and cannot be rebuilt from the daily uniques, so they
// must be read from the top level. Values from the OpenAPI examples.
func TestTrafficFixtures_OpenAPIWindowCountIsTheSumOfDaysButUniquesAreNot(t *testing.T) {
	want := map[string]struct{ count, uniques, uniquesSum int }{
		"views":  {count: 14850, uniques: 3782, uniquesSum: 4630},
		"clones": {count: 173, uniques: 128, uniquesSum: 136},
	}
	for _, kind := range trafficFixtureKinds {
		name := "traffic_openapi_" + kind + ".json"
		var s trafficFixtureSeries
		readTrafficFixture(t, name, &s)
		countSum, uniquesSum := 0, 0
		for _, b := range s.series(kind) {
			countSum += b.Count
			uniquesSum += b.Uniques
		}
		w := want[kind]
		if s.Count != w.count || s.Uniques != w.uniques {
			t.Errorf("%s: count/uniques %d/%d, want %d/%d", name, s.Count, s.Uniques, w.count, w.uniques)
		}
		if countSum != s.Count {
			t.Errorf("%s: daily counts sum to %d, top-level count is %d", name, countSum, s.Count)
		}
		if uniquesSum != w.uniquesSum || uniquesSum == s.Uniques {
			t.Errorf("%s: daily uniques sum to %d, want %d (not the top-level %d)", name, uniquesSum, w.uniquesSum, s.Uniques)
		}
	}
}

func TestTrafficFixtures_OpenAPIPopularPaths(t *testing.T) {
	var paths []trafficFixturePath
	readTrafficFixture(t, "traffic_openapi_popular_paths.json", &paths)
	if len(paths) != 10 {
		t.Fatalf("popular paths: %d entries, want 10", len(paths))
	}
	for i, p := range paths {
		if p.Path == "" || p.Title == "" || p.Count == 0 || p.Uniques == 0 {
			t.Errorf("popular path %d: %+v, want path, title, count and uniques set", i, p)
		}
	}
	// Titles carry non-ASCII text (U+00B7 MIDDLE DOT); it must decode
	// intact.
	want := trafficFixturePath{
		Path:    "/github/hubot/blob/master/docs/scripting.md",
		Title:   "hubot/scripting.md at master · github/hubot · GitHub",
		Count:   1707,
		Uniques: 804,
	}
	if paths[1] != want {
		t.Errorf("popular path 1: %+v, want %+v", paths[1], want)
	}
}

func TestTrafficFixtures_OpenAPIPopularReferrers(t *testing.T) {
	var referrers []trafficFixtureReferrer
	readTrafficFixture(t, "traffic_openapi_popular_referrers.json", &referrers)
	want := []trafficFixtureReferrer{
		{Referrer: "Google", Count: 4, Uniques: 3},
		{Referrer: "stackoverflow.com", Count: 2, Uniques: 2},
		{Referrer: "eggsonbread.com", Count: 1, Uniques: 1},
		{Referrer: "yandex.ru", Count: 1, Uniques: 1},
	}
	if len(referrers) != len(want) {
		t.Fatalf("popular referrers: %d entries, want %d", len(referrers), len(want))
	}
	for i := range want {
		if referrers[i] != want[i] {
			t.Errorf("popular referrer %d: %+v, want %+v", i, referrers[i], want[i])
		}
	}
}

func TestTrafficFixtures_ZeroTrafficDailyBucketsEndOnTheCollectionDay(t *testing.T) {
	for _, kind := range trafficFixtureKinds {
		name := "traffic_zero_" + kind + "_day.json"
		var s trafficFixtureSeries
		readTrafficFixture(t, name, &s)
		assertConsecutiveDays(t, name, s.series(kind), trafficFixtureDate(2026, time.October, 6), 14)
	}
}

func TestTrafficFixtures_ZeroTrafficWeeklyBucketLabels(t *testing.T) {
	want := []time.Time{
		trafficFixtureDate(2026, time.October, 6),
		trafficFixtureDate(2026, time.October, 12),
		trafficFixtureDate(2026, time.October, 19),
	}
	for _, kind := range trafficFixtureKinds {
		name := "traffic_zero_" + kind + "_week.json"
		var s trafficFixtureSeries
		readTrafficFixture(t, name, &s)
		buckets := s.series(kind)
		if len(buckets) != len(want) {
			t.Fatalf("%s: %d weekly buckets, want %d", name, len(buckets), len(want))
		}
		for i, b := range buckets {
			if !b.Timestamp.Equal(want[i]) {
				t.Errorf("%s: weekly bucket %d at %s, want %s", name, i, b.Timestamp, want[i])
			}
		}
	}
}

func TestTrafficFixtures_ZeroTrafficDayAndWeekReportTheSameWindowTotals(t *testing.T) {
	for _, kind := range trafficFixtureKinds {
		var day, week trafficFixtureSeries
		readTrafficFixture(t, "traffic_zero_"+kind+"_day.json", &day)
		readTrafficFixture(t, "traffic_zero_"+kind+"_week.json", &week)
		if day.Count != week.Count || day.Uniques != week.Uniques {
			t.Errorf("zero %s: per=day count/uniques %d/%d, per=week %d/%d",
				kind, day.Count, day.Uniques, week.Count, week.Uniques)
		}
	}
}

func TestTrafficFixtures_ZeroTrafficIsZeroFilled(t *testing.T) {
	for _, kind := range trafficFixtureKinds {
		for _, per := range []string{"day", "week"} {
			name := "traffic_zero_" + kind + "_" + per + ".json"
			var s trafficFixtureSeries
			readTrafficFixture(t, name, &s)
			if s.Count != 0 || s.Uniques != 0 {
				t.Errorf("%s: count/uniques %d/%d, want 0/0", name, s.Count, s.Uniques)
			}
			for i, b := range s.series(kind) {
				if b.Count != 0 || b.Uniques != 0 {
					t.Errorf("%s: bucket %d count/uniques %d/%d, want 0/0", name, i, b.Count, b.Uniques)
				}
			}
		}
	}
	var paths []trafficFixturePath
	readTrafficFixture(t, "traffic_zero_popular_paths.json", &paths)
	if paths == nil || len(paths) != 0 {
		t.Errorf("zero popular paths: %v, want an empty JSON array", paths)
	}
	var referrers []trafficFixtureReferrer
	readTrafficFixture(t, "traffic_zero_popular_referrers.json", &referrers)
	if referrers == nil || len(referrers) != 0 {
		t.Errorf("zero popular referrers: %v, want an empty JSON array", referrers)
	}
}
