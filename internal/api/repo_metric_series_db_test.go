// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// The repository page's metric charts on the repository-page cache
// (0.29.119, summary/63 §4): GET /api/v1/repos/{id}/metric-series answers
// what /compare answers for that one repository, and opening a repository
// page no longer writes it into the viewer's Comparisons group.

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// metricSeriesFixture is a repository with commits, issues and change
// requests inside the default window (the three years before the last
// complete week), so every temporal metric has non-zero points.
func metricSeriesFixture(t *testing.T) (*Server, *db.PostgresStore, int64, int) {
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
	suffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	var repoID int64
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
		VALUES ($1, 'r', $2, 1) RETURNING repo_id`, "https://github.com/_avmseries"+suffix+"/r", "_avmseries"+suffix).Scan(&repoID); err != nil {
		t.Fatal(err)
	}
	var userID int
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_ops.users (login_name, oauth_provider, admin)
		VALUES ($1, 'github', TRUE) RETURNING user_id`, "_avmseries"+suffix).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	var cntrb string
	if err := store.Pool().QueryRow(ctx, `INSERT INTO aveloxis_data.contributors (cntrb_login, gh_login, gh_type)
		VALUES ($1, $1, 'User') RETURNING cntrb_id::text`, "_avmseries"+suffix).Scan(&cntrb); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		c := context.Background()
		for _, q := range []string{
			`DELETE FROM aveloxis_data.commits WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issues WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
			`DELETE FROM aveloxis_ops.user_repos WHERE repo_id = $1`,
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		} {
			_, _ = store.Pool().Exec(c, q, repoID)
		}
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, userID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, userID)
		_, _ = store.Pool().Exec(c, `DELETE FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, cntrb)
	})
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_ops.collection_queue (repo_id, last_collected) VALUES ($1, NOW() - interval '1 day')`, repoID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	for i, days := range []int{40, 41, 47, 200, 500} {
		when := now.AddDate(0, 0, -days)
		n := int64(i + 1)
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp, cmt_ght_author_id)
			VALUES ($1, $2, 'f.go', $3, $4::uuid)`, repoID, strings.Repeat("0", 39)+strconv.FormatInt(n, 10), when, cntrb); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, reporter_id, created_at, closed_at)
			VALUES ($1, $2, $3, $4::uuid, $5::timestamptz, $5::timestamptz + interval '2 days')`, repoID, n, n, cntrb, when); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, pr_number, author_id, created_at, merged_at)
			VALUES ($1, $2, $3, $4::uuid, $5::timestamptz, $5::timestamptz + interval '1 day')`, repoID, n, n, cntrb, when); err != nil {
			t.Fatal(err)
		}
	}
	s, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs, ResponseCacheBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	return s, store, repoID, userID
}

func metricSeriesCall(t *testing.T, s *Server, path string, userID int) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(http.MethodGet, path, nil)
	r.RemoteAddr = "127.0.0.1:1"
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, asUser(r, userID, true))
	return w
}

// The route answers /compare's single-entity body for every temporal
// metric and every parameter the repository page sends, so the GUI's
// chart renderer and its exact-payload export are unchanged.
func TestRepoMetricSeriesAnswersWhatCompareAnswers(t *testing.T) {
	s, _, repoID, userID := metricSeriesFixture(t)
	id := strconv.FormatInt(repoID, 10)
	since := time.Now().UTC().AddDate(-1, 0, 0).Format("2006-01-02")
	until := time.Now().UTC().AddDate(0, 0, -30).Format("2006-01-02")
	metrics := 0
	nonZero := map[string]bool{}
	for _, m := range metricCatalog {
		if m.Kind != "temporal" {
			continue
		}
		metrics++
		variants := []string{"", "&bucket=month", "&since=" + since + "&until=" + until}
		if m.ID == retentionMetricID {
			variants = append(variants, "&bucket=month&retention_threshold=2")
		}
		for _, v := range variants {
			cmp := metricSeriesCall(t, s, "/api/v1/compare?entities=repo:"+id+"&metric="+m.ID+v, userID)
			got := metricSeriesCall(t, s, "/api/v1/repos/"+id+"/metric-series?metric="+m.ID+v, userID)
			if cmp.Code != http.StatusOK || got.Code != http.StatusOK {
				t.Fatalf("%s%s: compare %d %s; metric-series %d %s", m.ID, v, cmp.Code, cmp.Body.String(), got.Code, got.Body.String())
			}
			var want, have map[string]any
			if err := json.Unmarshal(cmp.Body.Bytes(), &want); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(got.Body.Bytes(), &have); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(want, have) {
				t.Errorf("%s%s: metric-series answers\n%s\nbut /compare answers\n%s", m.ID, v, got.Body.String(), cmp.Body.String())
			}
			if strings.Contains(got.Body.String(), `"value":1`) || strings.Contains(got.Body.String(), `"value":2`) {
				nonZero[m.ID] = true
			}
		}
	}
	if metrics < 10 {
		t.Fatalf("drove %d temporal metrics; the catalog has at least 10", metrics)
	}
	// The fixture is not vacuous: the count metrics see its rows.
	for _, m := range []string{"code_change_commits", "committers", "issues", "change_requests", "issues_closed", "change_requests_merged", "contributors"} {
		if !nonZero[m] {
			t.Errorf("%s answered only zeros: the fixture's rows are outside the window, and the comparison proves nothing for it", m)
		}
	}
}

// Opening a repository page's charts records nothing: /compare records every
// compared repository in the caller's Comparisons group (the positive
// control below proves the count sees such a record), the repository route
// does not.
func TestRepoMetricSeriesRecordsNoComparison(t *testing.T) {
	s, store, repoID, userID := metricSeriesFixture(t)
	id := strconv.FormatInt(repoID, 10)
	links := func() int {
		t.Helper()
		var n int
		if err := store.Pool().QueryRow(context.Background(), `
			SELECT COUNT(*) FROM aveloxis_ops.user_repos ur JOIN aveloxis_ops.user_groups g USING (group_id)
			WHERE g.user_id = $1 AND g.name = $2 AND ur.repo_id = $3`, userID, db.ComparisonsGroupName, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	for _, m := range []string{"code_change_commits", "burstiness", retentionMetricID} {
		if w := metricSeriesCall(t, s, "/api/v1/repos/"+id+"/metric-series?metric="+m, userID); w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", m, w.Code, w.Body.String())
		}
	}
	if n := links(); n != 0 {
		t.Errorf("the repository route linked the repository into the Comparisons group (%d links): opening a repository page is not a comparison", n)
	}
	if w := metricSeriesCall(t, s, "/api/v1/compare?entities=repo:"+id+"&metric=issues", userID); w.Code != http.StatusOK {
		t.Fatalf("compare: %d %s", w.Code, w.Body.String())
	}
	if n := links(); n != 1 {
		t.Fatalf("positive control: /compare recorded %d links, want 1 — the count above cannot see a record", n)
	}
}

// The second request for a series is served from the page cache, and the
// repository's next collection replaces it.
func TestRepoMetricSeriesIsServedFromThePageCache(t *testing.T) {
	s, store, repoID, userID := metricSeriesFixture(t)
	path := "/api/v1/repos/" + strconv.FormatInt(repoID, 10) + "/metric-series?metric=burstiness"
	first := metricSeriesCall(t, s, path, userID)
	if first.Code != http.StatusOK || first.Header().Get("X-Cache") != "" || first.Header().Get("ETag") == "" {
		t.Fatalf("first: %d X-Cache %q ETag %q", first.Code, first.Header().Get("X-Cache"), first.Header().Get("ETag"))
	}
	if second := metricSeriesCall(t, s, path, userID); second.Header().Get("X-Cache") != "hit" || second.Body.String() != first.Body.String() {
		t.Errorf("second: X-Cache %q, want hit with the same body", second.Header().Get("X-Cache"))
	}
	if _, err := store.Pool().Exec(context.Background(), `UPDATE aveloxis_ops.collection_queue SET last_collected = NOW(), updated_at = NOW() WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	if third := metricSeriesCall(t, s, path, userID); third.Header().Get("X-Cache") == "hit" {
		t.Error("after the next collection the series must be recomputed")
	}
}

// A label the store cannot read is a degraded answer: logged, and marked
// partial so the page cache never stores it (it was dropped silently
// before 0.29.119).
func TestEntityLabelFailureIsAPartialAnswer(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	store, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	store.Close() // every read fails from here on
	var logs lockedBuffer
	s := &Server{store: store, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	ctl := &pageCacheCtl{}
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(context.WithValue(r.Context(), pageCacheCtlKey{}, ctl))
	entities := []entity{{Kind: "repo", RepoID: 1, Label: "repo:1"}}
	s.labelEntities(r, entities)
	ctl.mu.Lock()
	partial := ctl.partial
	ctl.mu.Unlock()
	if !partial {
		t.Error("a failed label lookup must mark the answer partial (never stored)")
	}
	if !strings.Contains(logs.String(), "label") {
		t.Errorf("a failed label lookup must be logged, got %q", logs.String())
	}
	if entities[0].Label != "repo:1" {
		t.Errorf("the label falls back to the raw token, got %q", entities[0].Label)
	}
}

// The 2026-10-26 nixpkgs case, end to end (D2, 0.29.122): a chart visitors
// came back to is saved by a re-warm pass; the api restarts (empty memory);
// the repository is collected; the new process's re-warm replays the chart
// from the saved list, so the first visitor after the collection gets a hit.
func TestRestartedAPIReplaysTheSavedList(t *testing.T) {
	s, store, repoID, userID := metricSeriesFixture(t)
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_ops.page_cache_replay WHERE repo_id = $1`, repoID)
	})
	path := "/api/v1/repos/" + strconv.FormatInt(repoID, 10) + "/metric-series?metric=code_change_commits"
	metricSeriesCall(t, s, path, userID)
	metricSeriesCall(t, s, path, userID) // asked for again
	ctx := context.Background()
	s.rewarmOnce(ctx)
	var saved int
	if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.page_cache_replay WHERE repo_id = $1`, repoID).Scan(&saved); err != nil || saved != 1 {
		t.Fatalf("the pass must save the asked-for chart: %d rows (%v)", saved, err)
	}
	restarted, err := NewWithOptions(store, s.logger, Options{ExemptCIDRs: DefaultExemptCIDRs, ResponseCacheBytes: 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Pool().Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET last_collected = NOW(), updated_at = NOW() WHERE repo_id = $1`, repoID); err != nil {
		t.Fatal(err)
	}
	restarted.loadReplaySeeds(ctx)
	if _, reqs := restarted.rewarmOnce(ctx); reqs != 1 {
		t.Fatalf("the restarted process must replay the saved chart once, replayed %d", reqs)
	}
	if w := metricSeriesCall(t, restarted, path, userID); w.Header().Get("X-Cache") != "hit" {
		t.Errorf("the first visitor after the restart and the collection: X-Cache %q, want hit", w.Header().Get("X-Cache"))
	}
}

// Cross-area finding of round 1 (0.29.123): the first-activity floor follows
// the data — a durable answer recomputed after its repository changed must
// not take a floor memoised for the process's life (it was, in faCache).
// /compare's floor is kept under the collection generation (round 2,
// cachedFirstActivity), so the earlier issue lands the way a collection
// writes one, with the state moved, and the cached floor follows it.
func TestFirstActivityFloorIsNotMemoised(t *testing.T) {
	s, store, repoID, _ := metricSeriesFixture(t)
	ctx := context.Background()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	first, ok, err := s.cachedFirstActivity(r, []int64{repoID})
	if err != nil || !ok {
		t.Fatalf("floor: %v %v", ok, err)
	}
	earlier := first.AddDate(-1, 0, 0)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, created_at)
		VALUES ($1, 990001, 990001, $2)`, repoID, earlier); err != nil {
		t.Fatal(err)
	}
	if err := store.StampReposChangedOutsideJob(ctx, []int64{repoID}); err != nil {
		t.Fatal(err)
	}
	if again, _, _ := s.cachedFirstActivity(r, []int64{repoID}); !again.Equal(earlier) {
		t.Errorf("after an earlier issue landed the floor is %v, want %v", again, earlier)
	}
}

// Round 3 finding 1 (0.29.123): the repository chart route is cached by the
// page cache and stays out of the comparison's series cache (D5: "the
// repository page's ten series leave the map") — its first-activity floor
// too, or a warm run's floors flood the 1,000-entry map and reset it.
func TestRepoChartRouteLeavesTheSeriesCacheAlone(t *testing.T) {
	s, _, repoID, userID := metricSeriesFixture(t)
	w := metricSeriesCall(t, s, "/api/v1/repos/"+strconv.FormatInt(repoID, 10)+"/metric-series?metric=issues", userID)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	s.seriesCache.mu.Lock()
	defer s.seriesCache.mu.Unlock()
	if n := len(s.seriesCache.m); n != 0 {
		t.Errorf("the repository chart route stored %d entries in the comparison's series cache, want 0", n)
	}
}
