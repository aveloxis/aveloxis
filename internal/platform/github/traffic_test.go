// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The bodies these tests serve are the fixtures described in
// traffic_fixtures_test.go, except the per=week answers of the full
// read: GitHub's OpenAPI description has no per=week example (see the
// list of reads below).

// trafficRead names one GitHub traffic request by its endpoint below
// /repos/{owner}/{repo}/traffic/, with the per= query it carries.
type trafficRead struct {
	path string // e.g. "/repos/o/r/traffic/views"
	per  string // "day", "week" or "" (no per= parameter)
}

func (r trafficRead) String() string {
	if r.per == "" {
		return r.path
	}
	return r.path + "?per=" + r.per
}

// The five reads of FetchTraffic, in the order it makes them. A read
// that fails stops the call, so the order decides which reads a failure
// skips. Its error starts with "traffic <endpoint>: ", the form the
// collector records as a collection error, as it does for releases; the
// HTTP client's cause alone carries the URL but not that form.
var fetchTrafficReads = []struct {
	read     trafficRead
	endpoint string // how the error names it, after "traffic "
}{
	{trafficRead{"/repos/o/r/traffic/views", "day"}, "views?per=day"},
	{trafficRead{"/repos/o/r/traffic/views", "week"}, "views?per=week"},
	{trafficRead{"/repos/o/r/traffic/clones", "week"}, "clones?per=week"},
	{trafficRead{"/repos/o/r/traffic/popular/paths", ""}, "popular/paths"},
	{trafficRead{"/repos/o/r/traffic/popular/referrers", ""}, "popular/referrers"},
}

// trafficServer answers each traffic read with the body bodies holds for
// it, records every request, and fails the test on a read it does not
// know.
type trafficServer struct {
	t      *testing.T
	bodies map[trafficRead][]byte
	status map[trafficRead]int // non-200 answers, empty body

	mu   sync.Mutex
	seen []trafficRead
}

func (s *trafficServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	read := trafficRead{path: r.URL.Path, per: r.URL.Query().Get("per")}
	s.mu.Lock()
	s.seen = append(s.seen, read)
	s.mu.Unlock()
	if code, ok := s.status[read]; ok {
		if code == http.StatusForbidden {
			// GitHub's basic-error body for a key without push access.
			// A positive X-RateLimit-Remaining keeps the client from
			// reading the 403 as a rate-limit refusal.
			w.Header().Set("X-RateLimit-Remaining", "4999")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"message":"Must have push access to repository","documentation_url":"https://docs.github.com/rest/metrics/traffic#get-page-views","status":"403"}`))
			return
		}
		w.WriteHeader(code)
		return
	}
	body, ok := s.bodies[read]
	if !ok {
		s.t.Errorf("unexpected traffic request %s", read)
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body)
}

// requests returns how many times each read was requested.
func (s *trafficServer) requests() map[trafficRead]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := map[trafficRead]int{}
	for _, r := range s.seen {
		n[r]++
	}
	return n
}

// readTrafficBody returns a traffic fixture's bytes as GitHub sent them.
func readTrafficBody(t *testing.T, name string) []byte {
	t.Helper()
	return []byte(srctest.Read(t, "internal/platform/github/testdata/"+name))
}

// Weekly bodies for the full read. GitHub's OpenAPI description has no
// per=week example, so these are built here to the description's
// view-traffic and clone-traffic schemas (top-level count and uniques,
// and an array of {timestamp, count, uniques} buckets), over the same
// 2016-10-10 to 2016-10-24 window as the description's per=day examples:
//
//   - the top-level count and uniques are those of the per=day example,
//     because GitHub reports the same top-level figures for per=day and
//     per=week (observed on the live API in October 2026);
//   - each bucket starts on a Monday and its count is the sum of the
//     per=day example's counts for that week's days (views: 6291 =
//     440+1308+1486+1170+868+495+524, 7945 = 1263+1402+1394+1492+1153+
//     566+675, 614; clones: 63, 103, 7);
//   - bucket uniques cannot be derived from daily uniques; the values
//     are illustrative, at least the week's largest daily uniques and at
//     most the window's, and distinct so each bucket is told apart.
const (
	trafficTestViewsWeek = `{"count":14850,"uniques":3782,"views":[` +
		`{"timestamp":"2016-10-10T00:00:00Z","count":6291,"uniques":1801},` +
		`{"timestamp":"2016-10-17T00:00:00Z","count":7945,"uniques":2140},` +
		`{"timestamp":"2016-10-24T00:00:00Z","count":614,"uniques":237}]}`
	trafficTestClonesWeek = `{"count":173,"uniques":128,"clones":[` +
		`{"timestamp":"2016-10-10T00:00:00Z","count":63,"uniques":51},` +
		`{"timestamp":"2016-10-17T00:00:00Z","count":103,"uniques":77},` +
		`{"timestamp":"2016-10-24T00:00:00Z","count":7,"uniques":5}]}`
)

func TestFetchTrafficReadsTheFiveEndpointsIntoOneBundle(t *testing.T) {
	srv := &trafficServer{t: t, bodies: map[trafficRead][]byte{
		{"/repos/o/r/traffic/views", "day"}:          readTrafficBody(t, "traffic_openapi_views.json"),
		{"/repos/o/r/traffic/views", "week"}:         []byte(trafficTestViewsWeek),
		{"/repos/o/r/traffic/clones", "week"}:        []byte(trafficTestClonesWeek),
		{"/repos/o/r/traffic/popular/paths", ""}:     readTrafficBody(t, "traffic_openapi_popular_paths.json"),
		{"/repos/o/r/traffic/popular/referrers", ""}: readTrafficBody(t, "traffic_openapi_popular_referrers.json"),
	}}
	client := testGHClient(t, srv)

	got, err := client.FetchTraffic(context.Background(), "o", "r")
	if err != nil {
		t.Fatalf("FetchTraffic: %v", err)
	}
	if got == nil {
		t.Fatal("FetchTraffic returned nil traffic with no error; nil means a forge without traffic")
	}

	// Every read once: no read repeated, none skipped, and the daily
	// clones not read again: FetchCloneStats reads them, so a collection
	// spends one request on them, as before traffic was added.
	reqs := srv.requests()
	for _, r := range fetchTrafficReads {
		if reqs[r.read] != 1 {
			t.Errorf("%s requested %d times, want 1", r.read, reqs[r.read])
		}
	}
	if len(reqs) != len(fetchTrafficReads) {
		t.Errorf("requests %v, want exactly the five traffic reads", reqs)
	}

	// Daily views: every bucket of the OpenAPI example, in order.
	var viewsDay trafficFixtureSeries
	readTrafficFixture(t, "traffic_openapi_views.json", &viewsDay)
	if len(got.Views) != len(viewsDay.Views) {
		t.Fatalf("Views: %d buckets, want the example's %d", len(got.Views), len(viewsDay.Views))
	}
	for i, b := range viewsDay.Views {
		v := got.Views[i]
		if !v.Timestamp.Equal(b.Timestamp) || v.TotalViews != b.Count || v.UniqueViews != b.Uniques {
			t.Errorf("Views[%d] = %s %d/%d, want %s %d/%d",
				i, v.Timestamp, v.TotalViews, v.UniqueViews, b.Timestamp, b.Count, b.Uniques)
		}
	}

	// 14-day uniques: views from the per=day top level, clones from the
	// per=week top level: FetchTraffic reads clones only with per=week,
	// and GitHub reports the same top-level uniques for per=day and
	// per=week. Neither is a sum of buckets.
	if got.ViewUniques != viewsDay.Uniques {
		t.Errorf("ViewUniques = %d, want the per=day top-level uniques %d", got.ViewUniques, viewsDay.Uniques)
	}
	if got.CloneUniques != 128 {
		t.Errorf("CloneUniques = %d, want the clones?per=week top-level uniques 128", got.CloneUniques)
	}

	// Weekly buckets: all of them, partial weeks included, with the kind.
	day := func(d int) time.Time { return trafficFixtureDate(2016, time.October, d) }
	wantViews := []model.RepoTrafficWeek{
		{Kind: model.TrafficKindViews, WeekStart: day(10), Uniques: 1801},
		{Kind: model.TrafficKindViews, WeekStart: day(17), Uniques: 2140},
		{Kind: model.TrafficKindViews, WeekStart: day(24), Uniques: 237},
	}
	wantClones := []model.RepoTrafficWeek{
		{Kind: model.TrafficKindClones, WeekStart: day(10), Uniques: 51},
		{Kind: model.TrafficKindClones, WeekStart: day(17), Uniques: 77},
		{Kind: model.TrafficKindClones, WeekStart: day(24), Uniques: 5},
	}
	assertTrafficWeeks(t, "WeeklyViews", got.WeeklyViews, wantViews)
	assertTrafficWeeks(t, "WeeklyClones", got.WeeklyClones, wantClones)

	// Paths and referrers: the OpenAPI examples, entry for entry.
	var paths []trafficFixturePath
	readTrafficFixture(t, "traffic_openapi_popular_paths.json", &paths)
	if len(got.Paths) != len(paths) {
		t.Fatalf("Paths: %d entries, want the example's %d", len(got.Paths), len(paths))
	}
	for i, p := range paths {
		want := model.RepoTrafficPath{Path: p.Path, Title: p.Title, TotalViews: p.Count, UniqueViews: p.Uniques}
		if got.Paths[i] != want {
			t.Errorf("Paths[%d] = %+v, want %+v", i, got.Paths[i], want)
		}
	}
	var referrers []trafficFixtureReferrer
	readTrafficFixture(t, "traffic_openapi_popular_referrers.json", &referrers)
	if len(got.Referrers) != len(referrers) {
		t.Fatalf("Referrers: %d entries, want the example's %d", len(got.Referrers), len(referrers))
	}
	for i, r := range referrers {
		want := model.RepoTrafficReferrer{Referrer: r.Referrer, TotalViews: r.Count, UniqueViews: r.Uniques}
		if got.Referrers[i] != want {
			t.Errorf("Referrers[%d] = %+v, want %+v", i, got.Referrers[i], want)
		}
	}

	// The collector's fields stay empty: the client reports what GitHub
	// returned and derives nothing (platform.RepoCollector.FetchTraffic).
	if !got.SnapshotDate.IsZero() || got.Windows != nil || got.Weeks != nil {
		t.Errorf("SnapshotDate %s, Windows %v, Weeks %v: want them left for the collector",
			got.SnapshotDate, got.Windows, got.Weeks)
	}
}

// A repository nobody viewed or cloned: GitHub still returns 14 daily
// buckets and three weekly buckets, all 0, and every one of them must
// reach the caller: a stored 0 is how a reader of the history tells a
// day without traffic from a day Aveloxis did not read. All six
// reads of a collection are served from the zero-traffic recording; the
// daily clones come through FetchCloneStats.
func TestFetchTrafficKeepsTheZeroBucketsOfARepositoryWithoutTraffic(t *testing.T) {
	srv := &trafficServer{t: t, bodies: map[trafficRead][]byte{
		{"/repos/o/r/traffic/clones", ""}:            readTrafficBody(t, "traffic_zero_clones_day.json"),
		{"/repos/o/r/traffic/views", "day"}:          readTrafficBody(t, "traffic_zero_views_day.json"),
		{"/repos/o/r/traffic/views", "week"}:         readTrafficBody(t, "traffic_zero_views_week.json"),
		{"/repos/o/r/traffic/clones", "week"}:        readTrafficBody(t, "traffic_zero_clones_week.json"),
		{"/repos/o/r/traffic/popular/paths", ""}:     readTrafficBody(t, "traffic_zero_popular_paths.json"),
		{"/repos/o/r/traffic/popular/referrers", ""}: readTrafficBody(t, "traffic_zero_popular_referrers.json"),
	}}
	client := testGHClient(t, srv)
	ctx := context.Background()

	clones, err := client.FetchCloneStats(ctx, "o", "r")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	got, err := client.FetchTraffic(ctx, "o", "r")
	if err != nil {
		t.Fatalf("FetchTraffic: %v", err)
	}
	if got == nil {
		t.Fatal("FetchTraffic returned nil traffic with no error; nil means a forge without traffic")
	}

	start := trafficFixtureDate(2026, time.October, 6)
	if len(got.Views) != 14 {
		t.Fatalf("Views: %d daily buckets, want the 14 zero buckets GitHub returned", len(got.Views))
	}
	for i, v := range got.Views {
		if !v.Timestamp.Equal(start.AddDate(0, 0, i)) || v.TotalViews != 0 || v.UniqueViews != 0 {
			t.Errorf("Views[%d] = %s %d/%d, want %s 0/0", i, v.Timestamp, v.TotalViews, v.UniqueViews, start.AddDate(0, 0, i))
		}
	}
	if len(clones) != 14 {
		t.Fatalf("FetchCloneStats: %d daily buckets, want the 14 zero buckets GitHub returned", len(clones))
	}
	for i, c := range clones {
		if !c.Timestamp.Equal(start.AddDate(0, 0, i)) || c.TotalClones != 0 || c.UniqueClones == nil || *c.UniqueClones != 0 {
			t.Errorf("clones[%d] = %s %d/%s, want %s 0/0", i, c.Timestamp, c.TotalClones, uniquesText(c.UniqueClones), start.AddDate(0, 0, i))
		}
	}

	if got.ViewUniques != 0 || got.CloneUniques != 0 {
		t.Errorf("ViewUniques/CloneUniques = %d/%d, want 0/0", got.ViewUniques, got.CloneUniques)
	}

	// The weekly labels of a window that starts on a Tuesday: the first
	// bucket carries the window start (traffic_fixtures_test.go).
	weeks := []time.Time{start, trafficFixtureDate(2026, time.October, 12), trafficFixtureDate(2026, time.October, 19)}
	var wantViews, wantClones []model.RepoTrafficWeek
	for _, w := range weeks {
		wantViews = append(wantViews, model.RepoTrafficWeek{Kind: model.TrafficKindViews, WeekStart: w})
		wantClones = append(wantClones, model.RepoTrafficWeek{Kind: model.TrafficKindClones, WeekStart: w})
	}
	assertTrafficWeeks(t, "WeeklyViews", got.WeeklyViews, wantViews)
	assertTrafficWeeks(t, "WeeklyClones", got.WeeklyClones, wantClones)

	if len(got.Paths) != 0 || len(got.Referrers) != 0 {
		t.Errorf("Paths %v, Referrers %v: want both empty", got.Paths, got.Referrers)
	}
}

// A key without push access gets 403 on the first read. The error must
// still classify as a routine skip, so the collector logs one Info line
// instead of recording a failure, as for a 403 on releases, and no
// further traffic request is spent on the repository.
func TestFetchTraffic403OnTheFirstReadIsASkip(t *testing.T) {
	first := fetchTrafficReads[0]
	srv := &trafficServer{t: t, status: map[trafficRead]int{first.read: http.StatusForbidden}}
	client := testGHClient(t, srv)

	got, err := client.FetchTraffic(context.Background(), "o", "r")
	if err == nil {
		t.Fatal("FetchTraffic on a 403 returned no error")
	}
	if got != nil {
		t.Errorf("FetchTraffic on a 403 returned traffic %+v, want nil", got)
	}
	if !errors.Is(err, platform.ErrForbidden) {
		t.Errorf("errors.Is(err, platform.ErrForbidden) = false for %v", err)
	}
	if class := platform.ClassifyError(err); class != platform.ClassSkip {
		t.Errorf("ClassifyError = %v, want ClassSkip (a 403 on traffic is a routine skip): %v", class, err)
	}
	if want := "traffic " + first.endpoint + ": "; !strings.HasPrefix(err.Error(), want) {
		t.Errorf("error %q does not start with %q", err, want)
	}
	if reqs := srv.requests(); len(reqs) != 1 || reqs[first.read] != 1 {
		t.Errorf("requests %v, want only one %s", reqs, first.read)
	}
}

// A server error on any read stops the call with an error that names
// that read's endpoint and keeps the cause classifiable, and the reads
// after it are not made.
func TestFetchTraffic500OnAReadNamesThatEndpointAndStops(t *testing.T) {
	// The client retries a 500 with backoff; skip the waits.
	t.Cleanup(platform.SetServerErrorSleepForTest(func(ctx context.Context, _ time.Duration) error { return ctx.Err() }))

	for i, failing := range fetchTrafficReads {
		t.Run(failing.endpoint, func(t *testing.T) {
			// The full-read bodies for every read before the failing one.
			bodies := map[trafficRead][]byte{
				fetchTrafficReads[0].read: readTrafficBody(t, "traffic_openapi_views.json"),
				fetchTrafficReads[1].read: []byte(trafficTestViewsWeek),
				fetchTrafficReads[2].read: []byte(trafficTestClonesWeek),
				fetchTrafficReads[3].read: readTrafficBody(t, "traffic_openapi_popular_paths.json"),
			}
			srv := &trafficServer{t: t, bodies: bodies, status: map[trafficRead]int{failing.read: http.StatusInternalServerError}}
			client := testGHClient(t, srv)

			got, err := client.FetchTraffic(context.Background(), "o", "r")
			if err == nil {
				t.Fatalf("FetchTraffic with a 500 on %s returned no error", failing.read)
			}
			if got != nil {
				t.Errorf("FetchTraffic with a failed read returned traffic %+v, want nil", got)
			}
			if want := "traffic " + failing.endpoint + ": "; !strings.HasPrefix(err.Error(), want) {
				t.Errorf("error %q does not start with %q", err, want)
			}
			if !errors.Is(err, platform.ErrTransient) || platform.ClassifyError(err) != platform.ClassTransient {
				t.Errorf("error %v lost its cause: want ErrTransient, ClassTransient", err)
			}
			reqs := srv.requests()
			for j, r := range fetchTrafficReads {
				switch {
				case j < i && reqs[r.read] != 1:
					t.Errorf("%s requested %d times before the failure, want 1", r.read, reqs[r.read])
				case j > i && reqs[r.read] != 0:
					t.Errorf("%s requested %d times after the failed read, want 0", r.read, reqs[r.read])
				}
			}
		})
	}
}

// FetchCloneStats shares its bucket type with the traffic reads; this
// pins its mapping of GitHub's daily clones, from the OpenAPI clones
// example, so the shared type cannot change it.
func TestFetchCloneStatsMapsEveryDailyBucket(t *testing.T) {
	srv := &trafficServer{t: t, bodies: map[trafficRead][]byte{
		{"/repos/o/r/traffic/clones", ""}: readTrafficBody(t, "traffic_openapi_clones.json"),
	}}
	client := testGHClient(t, srv)

	got, err := client.FetchCloneStats(context.Background(), "o", "r")
	if err != nil {
		t.Fatalf("FetchCloneStats: %v", err)
	}
	var want trafficFixtureSeries
	readTrafficFixture(t, "traffic_openapi_clones.json", &want)
	if len(got) != len(want.Clones) {
		t.Fatalf("FetchCloneStats: %d buckets, want the example's %d", len(got), len(want.Clones))
	}
	for i, b := range want.Clones {
		c := got[i]
		if !c.Timestamp.Equal(b.Timestamp) || c.TotalClones != b.Count || c.UniqueClones == nil || *c.UniqueClones != b.Uniques {
			t.Errorf("clones[%d] = %s %d/%s, want %s %d/%d",
				i, c.Timestamp, c.TotalClones, uniquesText(c.UniqueClones), b.Timestamp, b.Count, b.Uniques)
		}
	}
}

func assertTrafficWeeks(t *testing.T, name string, got, want []model.RepoTrafficWeek) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: %d weekly buckets, want %d (every bucket, partial weeks included)", name, len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.Kind != w.Kind || !g.WeekStart.Equal(w.WeekStart) || g.Uniques != w.Uniques {
			t.Errorf("%s[%d] = %s %s %d, want %s %s %d", name, i, g.Kind, g.WeekStart, g.Uniques, w.Kind, w.WeekStart, w.Uniques)
		}
	}
}

// uniquesText renders a clone day's distinct count for a failure
// message: a number, or "absent" for a day that has none.
func uniquesText(p *int) string {
	if p == nil {
		return "absent"
	}
	return strconv.Itoa(*p)
}
