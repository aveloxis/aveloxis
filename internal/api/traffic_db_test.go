// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// GET /api/v1/repos/{repoID}/traffic on the real server and database: the
// full stored history in one answer, read by an API token whose owner's
// group holds the repository, as a reporting script reads it
// (aveloxis/aveloxis#234). The rows are written by the production
// writers (UpsertRepoTraffic, UpsertRepoClone), so the expected answer is
// what those writes stored, and Postgres decides the dates and the order.

// trafficAPITables are the tables the endpoint reads.
var trafficAPITables = []string{
	"repo_clones", "repo_views", "repo_traffic_paths",
	"repo_traffic_referrers", "repo_traffic_windows", "repo_traffic_weeks",
}

// trafficDay is midnight UTC of a day in October 2026.
func trafficDay(day int) time.Time {
	return time.Date(2026, time.October, day, 0, 0, 0, 0, time.UTC)
}

// trafficAPIFixture seeds the repositories, the two accounts and their API
// tokens. withTraffic, referrersOnly, withoutTraffic and withoutUniques
// are in the owner's group; the stranger's group holds none of them.
// withTraffic's latest read listed paths and no referrer, referrersOnly's
// latest read the other way round, withoutTraffic has no traffic at all,
// and withoutUniques is a GitLab project whose days have a count and no
// distinct cloners.
type trafficAPIFixture struct {
	server                                                     *Server
	withTraffic, referrersOnly, withoutTraffic, withoutUniques int64
	ownerToken, strangerToken                                  string
}

func seedTrafficAPIFixture(t *testing.T) trafficAPIFixture {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	const slug = "_avtrafficapi"
	exec := func(sql string, args ...any) {
		t.Helper()
		if _, err := pool.Exec(ctx, sql, args...); err != nil {
			t.Fatalf("%s: %v", strings.Fields(sql)[0], err)
		}
	}
	cleanup := func() {
		bg := context.Background() // never t.Context() in a cleanup (TEST-4)
		for _, table := range trafficAPITables {
			_, _ = pool.Exec(bg, `DELETE FROM aveloxis_data.`+table+` WHERE repo_id IN
				(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, slug)
		}
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.api_tokens WHERE user_id IN
			(SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, slug+"%")
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN
			(SELECT g.group_id FROM aveloxis_ops.user_groups g JOIN aveloxis_ops.users u USING (user_id)
			 WHERE u.login_name LIKE $1)`, slug+"%")
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN
			(SELECT user_id FROM aveloxis_ops.users WHERE login_name LIKE $1)`, slug+"%")
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE $1`, slug+"%")
		_, _ = pool.Exec(bg, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, slug)
	}
	cleanup()
	t.Cleanup(cleanup)

	repo := func(platform model.Platform, host, name string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, $2, $3, $4) RETURNING repo_id`, "https://"+host+"/"+slug+"/"+name, name, slug, int(platform)).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	github := func(name string) int64 { return repo(model.PlatformGitHub, "github.com", name) }
	fx := trafficAPIFixture{
		withTraffic: github("widget"), referrersOnly: github("linked"), withoutTraffic: github("quiet"),
		withoutUniques: repo(model.PlatformGitLab, "gitlab.com", "counted"),
	}
	other := github("other")

	account := func(login string, repos ...int64) string {
		t.Helper()
		var userID int
		if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider) VALUES ($1, 'github')
			RETURNING user_id`, login).Scan(&userID); err != nil {
			t.Fatal(err)
		}
		var groupID int64
		if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.user_groups (user_id, name) VALUES ($1, 'traffic readers')
			RETURNING group_id`, userID).Scan(&groupID); err != nil {
			t.Fatal(err)
		}
		for _, r := range repos {
			exec(`INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, groupID, r)
		}
		token, _, err := store.CreateAPIToken(ctx, db.APITokenGrant{UserID: userID, Label: "traffic reader",
			Lifetime: time.Hour, RateLimitPerHour: 1000, RateLimitPerDay: 10000})
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	fx.ownerToken = account(slug+"_owner", fx.withTraffic, fx.referrersOnly, fx.withoutTraffic, fx.withoutUniques)
	fx.strangerToken = account(slug+"_stranger", other)

	// Two reads of the repository with traffic, a day apart. The views
	// are written newest first, so only the reader's ORDER BY can put
	// them in date order.
	views := func(days ...[3]int) []model.RepoView {
		var out []model.RepoView
		for _, d := range days {
			out = append(out, model.RepoView{Timestamp: trafficDay(d[0]), TotalViews: d[1], UniqueViews: d[2]})
		}
		return out
	}
	first := &model.RepoTraffic{
		SnapshotDate: trafficDay(18),
		Views:        views([3]int{7, 12, 4}, [3]int{6, 0, 0}, [3]int{5, 5, 3}),
		Windows: []model.RepoTrafficWindow{
			{Kind: model.TrafficKindViews, Start: trafficDay(4), End: trafficDay(17), Uniques: 9},
		},
		Paths: []model.RepoTrafficPath{{Path: "/" + slug + "/widget", Title: slug + "/widget", TotalViews: 30, UniqueViews: 7}},
		Referrers: []model.RepoTrafficReferrer{
			{Referrer: "github.com", TotalViews: 20, UniqueViews: 5},
			{Referrer: "google.com", TotalViews: 8, UniqueViews: 2},
		},
	}
	// The second read lists no referrer: its snapshot holds paths only,
	// and the answer must not fall back to the first read's referrers.
	second := &model.RepoTraffic{
		SnapshotDate: trafficDay(19),
		Views:        views([3]int{8, 40, 9}),
		Windows: []model.RepoTrafficWindow{
			{Kind: model.TrafficKindViews, Start: trafficDay(5), End: trafficDay(18), Uniques: 11},
			{Kind: model.TrafficKindClones, Start: trafficDay(5), End: trafficDay(18), Uniques: 2},
		},
		Weeks: []model.RepoTrafficWeek{
			{Kind: model.TrafficKindViews, WeekStart: trafficDay(12), Uniques: 0},
			{Kind: model.TrafficKindViews, WeekStart: trafficDay(5), Uniques: 8},
			{Kind: model.TrafficKindClones, WeekStart: trafficDay(5), Uniques: 1},
		},
		Paths: []model.RepoTrafficPath{
			{Path: "/" + slug + "/widget/blob/main/README.md", Title: "README.md at main", TotalViews: 6, UniqueViews: 2},
			{Path: "/" + slug + "/widget", Title: slug + "/widget", TotalViews: 41, UniqueViews: 9},
		},
	}
	for _, read := range []*model.RepoTraffic{first, second} {
		if err := store.UpsertRepoTraffic(ctx, fx.withTraffic, read); err != nil {
			t.Fatalf("write traffic read of %s: %v", read.SnapshotDate.Format(time.DateOnly), err)
		}
	}
	// The mirror case: the latest read lists referrers and no path, so
	// the snapshot date must come from either list.
	for _, read := range []*model.RepoTraffic{
		{
			SnapshotDate: trafficDay(18),
			Paths:        []model.RepoTrafficPath{{Path: "/" + slug + "/linked", Title: slug + "/linked", TotalViews: 3, UniqueViews: 1}},
			Referrers:    []model.RepoTrafficReferrer{{Referrer: "news.example.com", TotalViews: 4, UniqueViews: 4}},
		},
		{
			SnapshotDate: trafficDay(19),
			Referrers: []model.RepoTrafficReferrer{
				{Referrer: "github.com", TotalViews: 2, UniqueViews: 1},
				{Referrer: "docs.example.com", TotalViews: 7, UniqueViews: 3},
			},
		},
	} {
		if err := store.UpsertRepoTraffic(ctx, fx.referrersOnly, read); err != nil {
			t.Fatalf("write traffic read of %s: %v", read.SnapshotDate.Format(time.DateOnly), err)
		}
	}
	zero, one := 0, 1
	for _, c := range []model.RepoClone{
		{RepoID: fx.withTraffic, Timestamp: trafficDay(6), TotalClones: 0, UniqueClones: &zero},
		{RepoID: fx.withTraffic, Timestamp: trafficDay(5), TotalClones: 2, UniqueClones: &one},
	} {
		if err := store.UpsertRepoClone(ctx, &c); err != nil {
			t.Fatalf("write daily clones: %v", err)
		}
	}

	// The GitLab project's days carry the fetch count and no uniques; 0
	// is a day nobody fetched, which differs from a day with no uniques.
	for _, c := range []model.RepoClone{
		{RepoID: fx.withoutUniques, Timestamp: trafficDay(7), TotalClones: 6,
			Origin: model.DataOrigin{DataSource: "/projects/:id/statistics"}},
		{RepoID: fx.withoutUniques, Timestamp: trafficDay(6), TotalClones: 0,
			Origin: model.DataOrigin{DataSource: "/projects/:id/statistics"}},
	} {
		if err := store.UpsertRepoClone(ctx, &c); err != nil {
			t.Fatalf("write daily clones without uniques: %v", err)
		}
	}

	fx.server, err = NewWithOptions(store, logger, Options{RequireAuth: true})
	if err != nil {
		t.Fatal(err)
	}
	return fx
}

// get calls the endpoint through the whole middleware chain, from a
// public address, with an API token.
func (fx trafficAPIFixture) get(repoID int64, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodGet, "/api/v1/repos/"+strconv.FormatInt(repoID, 10)+"/traffic", nil)
	r.RemoteAddr = "203.0.113.77:1"
	r.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	fx.server.Handler().ServeHTTP(w, r)
	return w
}

// trafficAnswerFields are the answer's fields, all always present.
var trafficAnswerFields = []string{"clones", "paths", "referrers", "repo_id", "snapshot_date", "views", "weeks", "windows"}

// decodeTrafficAnswer checks the answer's field set and returns its raw
// fields.
func decodeTrafficAnswer(t *testing.T, w *httptest.ResponseRecorder) map[string]json.RawMessage {
	t.Helper()
	if w.Code != http.StatusOK {
		t.Fatalf("status %d %q, want 200", w.Code, w.Body.String())
	}
	if ct := w.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type %q, want application/json", ct)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &fields); err != nil {
		t.Fatalf("the answer is not a JSON object: %v\n%s", err, w.Body.String())
	}
	var names []string
	for k := range fields {
		names = append(names, k)
	}
	sort.Strings(names)
	if !slices.Equal(names, trafficAnswerFields) {
		t.Fatalf("answer fields %v, want %v", names, trafficAnswerFields)
	}
	return fields
}

// trafficRowStrings decodes a list of rows, refusing a field the row
// shape does not name, and renders each row as a string; a JSON null
// count prints as null.
func trafficRowStrings[T any](t *testing.T, field string, raw json.RawMessage, render func(T) string) []string {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var rows []T
	if err := dec.Decode(&rows); err != nil {
		t.Fatalf("%s: %v\n%s", field, err, raw)
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, render(r))
	}
	return out
}

func trafficCount(p *int) string {
	if p == nil {
		return "null"
	}
	return strconv.Itoa(*p)
}

type trafficDayRow struct {
	Date    string `json:"date"`
	Count   *int   `json:"count"`
	Uniques *int   `json:"uniques"`
}

func (r trafficDayRow) String() string {
	return r.Date + " " + trafficCount(r.Count) + " " + trafficCount(r.Uniques)
}

type trafficWindowRow struct {
	Kind    string `json:"kind"`
	Start   string `json:"start"`
	End     string `json:"end"`
	Uniques *int   `json:"uniques"`
}

type trafficWeekRow struct {
	Kind      string `json:"kind"`
	WeekStart string `json:"week_start"`
	Uniques   *int   `json:"uniques"`
}

type trafficPathRow struct {
	Path    string `json:"path"`
	Title   string `json:"title"`
	Count   *int   `json:"count"`
	Uniques *int   `json:"uniques"`
}

type trafficReferrerRow struct {
	Referrer string `json:"referrer"`
	Count    *int   `json:"count"`
	Uniques  *int   `json:"uniques"`
}

func TestRepoTrafficServesTheStoredHistory(t *testing.T) {
	fx := seedTrafficAPIFixture(t)

	t.Run("token in the repository's group: the full history", func(t *testing.T) {
		fields := decodeTrafficAnswer(t, fx.get(fx.withTraffic, fx.ownerToken))
		if got := string(fields["repo_id"]); got != strconv.FormatInt(fx.withTraffic, 10) {
			t.Errorf("repo_id %s, want %d", got, fx.withTraffic)
		}
		check := func(field string, got, want []string) {
			t.Helper()
			if !slices.Equal(got, want) {
				t.Errorf("%s:\n got  %q\n want %q", field, got, want)
			}
		}
		day := func(r trafficDayRow) string { return r.String() }
		check("clones", trafficRowStrings(t, "clones", fields["clones"], day),
			[]string{"2026-10-05 2 1", "2026-10-06 0 0"})
		check("views", trafficRowStrings(t, "views", fields["views"], day),
			[]string{"2026-10-05 5 3", "2026-10-06 0 0", "2026-10-07 12 4", "2026-10-08 40 9"})
		check("windows", trafficRowStrings(t, "windows", fields["windows"], func(r trafficWindowRow) string {
			return r.Kind + " " + r.Start + " " + r.End + " " + trafficCount(r.Uniques)
		}), []string{
			"views 2026-10-04 2026-10-17 9",
			"clones 2026-10-05 2026-10-18 2",
			"views 2026-10-05 2026-10-18 11",
		})
		check("weeks", trafficRowStrings(t, "weeks", fields["weeks"], func(r trafficWeekRow) string {
			return r.Kind + " " + r.WeekStart + " " + trafficCount(r.Uniques)
		}), []string{"clones 2026-10-05 1", "views 2026-10-05 8", "views 2026-10-12 0"})
		if got := string(fields["snapshot_date"]); got != `"2026-10-19"` {
			t.Errorf("snapshot_date %s, want \"2026-10-19\" (the latest read's date)", got)
		}
		check("paths", trafficRowStrings(t, "paths", fields["paths"], func(r trafficPathRow) string {
			return r.Path + " | " + r.Title + " | " + trafficCount(r.Count) + " " + trafficCount(r.Uniques)
		}), []string{
			"/_avtrafficapi/widget | _avtrafficapi/widget | 41 9",
			"/_avtrafficapi/widget/blob/main/README.md | README.md at main | 6 2",
		})
		check("referrers", trafficRowStrings(t, "referrers", fields["referrers"], func(r trafficReferrerRow) string {
			return fmt.Sprintf("%s %s %s", r.Referrer, trafficCount(r.Count), trafficCount(r.Uniques))
		}), []string{})
	})

	t.Run("latest read listed referrers only: that date's referrers and no path", func(t *testing.T) {
		fields := decodeTrafficAnswer(t, fx.get(fx.referrersOnly, fx.ownerToken))
		if got := string(fields["snapshot_date"]); got != `"2026-10-19"` {
			t.Errorf("snapshot_date %s, want \"2026-10-19\" (the latest read's date)", got)
		}
		if got := string(fields["paths"]); got != "[]" {
			t.Errorf("paths = %s, want [] (the 2026-10-19 read listed no path)", got)
		}
		got := trafficRowStrings(t, "referrers", fields["referrers"], func(r trafficReferrerRow) string {
			return fmt.Sprintf("%s %s %s", r.Referrer, trafficCount(r.Count), trafficCount(r.Uniques))
		})
		if want := []string{"docs.example.com 7 3", "github.com 2 1"}; !slices.Equal(got, want) {
			t.Errorf("referrers:\n got  %q\n want %q", got, want)
		}
	})

	t.Run("repository without traffic: empty arrays and a null snapshot date", func(t *testing.T) {
		fields := decodeTrafficAnswer(t, fx.get(fx.withoutTraffic, fx.ownerToken))
		for _, f := range []string{"clones", "views", "windows", "weeks", "paths", "referrers"} {
			if got := string(fields[f]); got != "[]" {
				t.Errorf("%s = %s, want [] (an empty list, never null)", f, got)
			}
		}
		if got := string(fields["snapshot_date"]); got != "null" {
			t.Errorf("snapshot_date = %s, want null", got)
		}
	})

	t.Run("token outside the repository's groups: refused", func(t *testing.T) {
		w := fx.get(fx.withTraffic, fx.strangerToken)
		if w.Code != http.StatusForbidden || !strings.Contains(w.Body.String(), "repo_out_of_scope") {
			t.Fatalf("status %d %q, want the structured 403", w.Code, w.Body.String())
		}
		for _, leak := range []string{"github.com\",\"count", "2026-10-", "snapshot_date"} {
			if strings.Contains(w.Body.String(), leak) {
				t.Errorf("the refusal carries traffic data (%q):\n%s", leak, w.Body.String())
			}
		}
	})
}

// TestRepoTrafficServesNullUniquesForDaysWithoutThem: a GitLab project's
// days have a fetch count and no distinct cloners, so the answer carries
// "uniques": null on them, the key present, while a day of 0 fetches has
// count 0. The reader must not turn the missing count into 0, which
// would claim that nobody cloned.
func TestRepoTrafficServesNullUniquesForDaysWithoutThem(t *testing.T) {
	fx := seedTrafficAPIFixture(t)
	fields := decodeTrafficAnswer(t, fx.get(fx.withoutUniques, fx.ownerToken))

	got := trafficRowStrings(t, "clones", fields["clones"], func(r trafficDayRow) string { return r.String() })
	if want := []string{"2026-10-06 0 null", "2026-10-07 6 null"}; !slices.Equal(got, want) {
		t.Errorf("clones:\n got  %q\n want %q", got, want)
	}
	// The typed decode cannot tell a null from a missing key.
	if raw := string(fields["clones"]); strings.Count(raw, `"uniques":null`) != 2 {
		t.Errorf("clones = %s, want \"uniques\":null on both days", raw)
	}
	if got := string(fields["views"]); got != "[]" {
		t.Errorf("views = %s, want [] (GitLab has no views)", got)
	}
}
