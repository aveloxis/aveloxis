// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

// stagedTrafficFixture is a migrated scratch database with one
// repository seeded for a staged traffic test, and an independent
// connection that checks the stored rows without trusting the store
// under test.
type stagedTrafficFixture struct {
	ctx    context.Context
	logger *slog.Logger
	store  *db.PostgresStore
	raw    *pgxpool.Pool
	repoID int64
}

// newStagedTrafficFixture seeds one repository of platform at host,
// named from slug, and removes it and everything staged or stored for
// it before and after the test. Gated on AVELOXIS_TEST_DB.
func newStagedTrafficFixture(t *testing.T, slug string, platform model.Platform, host string) stagedTrafficFixture {
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
	if err := db.RunMigrations(ctx, store, logger); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close) // not defer: deferred calls run before the data cleanups (SR-9)

	cleanup := func() {
		bg := context.Background() // never t.Context() in a cleanup (TEST-4)
		of := `(SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`
		for _, sql := range []string{
			`DELETE FROM aveloxis_data.repo_clones WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_data.repo_views WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_data.repo_traffic_paths WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_data.repo_traffic_referrers WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_data.repo_traffic_windows WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_data.repo_traffic_weeks WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_ops.staging WHERE repo_id IN ` + of,
			// ProcessRepo records the repository's collection status.
			`DELETE FROM aveloxis_ops.collection_status WHERE repo_id IN ` + of,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%'`,
		} {
			if _, err := raw.Exec(bg, sql); err != nil {
				t.Errorf("traffic fixture cleanup: %v", err)
			}
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: platform,
		GitURL:   "https://" + host + "/" + slug + "/Repo",
		Owner:    slug, Name: "Repo",
	})
	if err != nil {
		t.Fatal(err)
	}
	return stagedTrafficFixture{ctx: ctx, logger: logger, store: store, raw: raw, repoID: repoID}
}

// rows returns the rows a query yields, one string per row; the query
// takes the repository id as $1 and selects one text column.
func (f stagedTrafficFixture) rows(t *testing.T, query string) []string {
	t.Helper()
	rows, err := f.raw.Query(f.ctx, query, f.repoID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			t.Fatal(err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestZeroTrafficReadIsStoredEndToEnd drives the recorded read of a
// repository without traffic through the whole write path: the
// collection step stages it, the staging writer flushes it, and
// ProcessRepo writes it with UpsertRepoClone and UpsertRepoTraffic.
// A successful read with no traffic must leave rows of 0, never no
// rows: the stored days are the days observed, so a consumer can tell
// a repository nobody visited (rows of 0) from one Aveloxis cannot
// read (no rows) and from a missed read (missing days).
//
// The fixture holds real GitHub responses read on Monday 2026-10-19
// (UTC): 14 zero-filled daily buckets per kind from 2026-10-06 to
// 2026-10-19, and weekly buckets labelled 2026-10-06 (the window start,
// a Tuesday), 2026-10-12 and 2026-10-19. Of those weeks only the one
// from Monday 2026-10-12 to Sunday 2026-10-18 lies entirely inside the
// window. The empty path and referrer lists store no snapshot row.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestZeroTrafficReadIsStoredEndToEnd(t *testing.T) {
	const slug = "_avtraffic_zero_e2e"
	fx := newStagedTrafficFixture(t, slug, model.PlatformGitHub, "github.com")
	ctx, logger, store, repoID := fx.ctx, fx.logger, fx.store, fx.repoID

	traffic, clones := zeroTrafficFixture(t)
	client := &trafficFakeClient{plat: model.PlatformGitHub, clones: clones, traffic: traffic}
	sc := &StagedCollector{client: client, logger: logger}
	sw := db.NewStagingWriter(store, repoID, int16(model.PlatformGitHub), logger)
	result := &CollectResult{}
	sc.collectTraffic(ctx, sw, slug, "Repo", time.Date(2026, time.October, 19, 16, 45, 0, 0, time.UTC), result)
	if len(result.Errors) != 0 {
		t.Fatalf("collecting the zero-traffic read recorded errors: %v", result.Errors)
	}
	if err := sw.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	proc := NewProcessor(store, logger)
	if err := proc.ProcessRepo(ctx, repoID, int16(model.PlatformGitHub)); err != nil {
		t.Fatalf("ProcessRepo: %v", err)
	}
	if proc.errors != 0 {
		t.Errorf("ProcessRepo counted %d row processing failures, want 0", proc.errors)
	}

	rowsOf := func(query string) []string { return fx.rows(t, query) }
	check := func(what string, got, want []string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("%s:\n got  %q\n want %q", what, got, want)
		}
	}

	var zeroDays []string
	for d := 6; d <= 19; d++ {
		zeroDays = append(zeroDays, fmt.Sprintf("2026-10-%02d 0/0", d))
	}
	check("daily views", rowsOf(`
		SELECT to_char(view_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_views || '/' || unique_views
		FROM aveloxis_data.repo_views WHERE repo_id = $1 ORDER BY view_timestamp`), zeroDays)
	check("daily clones", rowsOf(`
		SELECT to_char(clone_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_clones || '/' || unique_clones
		FROM aveloxis_data.repo_clones WHERE repo_id = $1 ORDER BY clone_timestamp`), zeroDays)
	check("window rows", rowsOf(`
		SELECT traffic_kind || ' ' || window_start::text || '..' || window_end::text || ' ' || uniques
		FROM aveloxis_data.repo_traffic_windows WHERE repo_id = $1 ORDER BY 1`),
		[]string{"clones 2026-10-06..2026-10-19 0", "views 2026-10-06..2026-10-19 0"})
	check("weekly rows", rowsOf(`
		SELECT traffic_kind || ' ' || week_start::text || ' ' || uniques
		FROM aveloxis_data.repo_traffic_weeks WHERE repo_id = $1 ORDER BY 1`),
		[]string{"clones 2026-10-12 0", "views 2026-10-12 0"})
	check("path snapshot", rowsOf(`
		SELECT path FROM aveloxis_data.repo_traffic_paths WHERE repo_id = $1 ORDER BY 1`), nil)
	check("referrer snapshot", rowsOf(`
		SELECT referrer FROM aveloxis_data.repo_traffic_referrers WHERE repo_id = $1 ORDER BY 1`), nil)
	check("unprocessed staging rows", rowsOf(`
		SELECT entity_type FROM aveloxis_ops.staging WHERE repo_id = $1 AND NOT processed ORDER BY 1`), nil)
}

// TestStagedCloneWithoutUniquesIsStoredAsNull sends daily clones through
// the staging table, which keeps them as JSON between the collection
// step and ProcessRepo: a day with no distinct cloners must come out as
// NULL, not 0, and a day whose count is 0 must stay a 0 with its
// uniques. The data source is the one the forge's client sets.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestStagedCloneWithoutUniquesIsStoredAsNull(t *testing.T) {
	fx := newStagedTrafficFixture(t, "_avtraffic_nulluniques_e2e", model.PlatformGitLab, "gitlab.com")
	const source = "/projects/:id/statistics"
	zero, three := 0, 3

	sw := db.NewStagingWriter(fx.store, fx.repoID, int16(model.PlatformGitLab), fx.logger)
	for _, c := range []model.RepoClone{
		{Timestamp: trafficDay(5), TotalClones: 0, Origin: model.DataOrigin{DataSource: source}},
		{Timestamp: trafficDay(6), TotalClones: 9, Origin: model.DataOrigin{DataSource: source}},
		{Timestamp: trafficDay(7), TotalClones: 0, UniqueClones: &zero, Origin: model.DataOrigin{DataSource: source}},
		{Timestamp: trafficDay(8), TotalClones: 4, UniqueClones: &three, Origin: model.DataOrigin{DataSource: source}},
	} {
		if err := sw.Stage(fx.ctx, EntityCloneStats, c); err != nil {
			t.Fatalf("stage: %v", err)
		}
	}
	if err := sw.Flush(fx.ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	proc := NewProcessor(fx.store, fx.logger)
	if err := proc.ProcessRepo(fx.ctx, fx.repoID, int16(model.PlatformGitLab)); err != nil {
		t.Fatalf("ProcessRepo: %v", err)
	}
	if proc.errors != 0 {
		t.Errorf("ProcessRepo counted %d row processing failures, want 0", proc.errors)
	}

	got := fx.rows(t, `
		SELECT to_char(clone_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_clones
			|| '/' || COALESCE(unique_clones::text, 'NULL') || ' ' || data_source
		FROM aveloxis_data.repo_clones WHERE repo_id = $1 ORDER BY clone_timestamp`)
	want := []string{
		"2026-10-05 0/NULL " + source,
		"2026-10-06 9/NULL " + source,
		"2026-10-07 0/0 " + source,
		"2026-10-08 4/3 " + source,
	}
	if !slices.Equal(got, want) {
		t.Errorf("daily clones after staging:\n got  %q\n want %q", got, want)
	}
}

// trafficTables are the tables only a traffic read fills; GitLab fills
// none of them.
var trafficTables = []string{
	"repo_views", "repo_traffic_paths", "repo_traffic_referrers",
	"repo_traffic_windows", "repo_traffic_weeks",
}

// collectAndProcess runs the traffic step of a collection on now with
// client, flushes the staged rows and processes them as the metadata
// step does, and returns what the step recorded and logged.
func (f stagedTrafficFixture) collectAndProcess(t *testing.T, client *trafficFakeClient, slug string, now time.Time) (*CollectResult, string) {
	t.Helper()
	logger, logs := bufLogger()
	sc := &StagedCollector{client: client, logger: logger}
	sw := db.NewStagingWriter(f.store, f.repoID, int16(client.plat), f.logger)
	result := &CollectResult{}
	sc.collectTraffic(f.ctx, sw, slug, "Repo", now, result)
	if err := sw.Flush(f.ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	proc := NewProcessor(f.store, f.logger)
	if err := proc.ProcessRepo(f.ctx, f.repoID, int16(client.plat)); err != nil {
		t.Fatalf("ProcessRepo: %v", err)
	}
	if proc.errors != 0 {
		t.Errorf("ProcessRepo counted %d row processing failures, want 0", proc.errors)
	}
	return result, logs.String()
}

// storedClones returns the stored daily clones as "day total/uniques
// source", NULL uniques as NULL, oldest first.
func (f stagedTrafficFixture) storedClones(t *testing.T) []string {
	t.Helper()
	return f.rows(t, `
		SELECT to_char(clone_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD') || ' ' || total_clones
			|| '/' || COALESCE(unique_clones::text, 'NULL') || ' ' || data_source
		FROM aveloxis_data.repo_clones WHERE repo_id = $1 ORDER BY clone_timestamp`)
}

// requireNoTrafficRows fails when any table a traffic read fills, or the
// staging table, holds a row of the repository.
func (f stagedTrafficFixture) requireNoTrafficRows(t *testing.T) {
	t.Helper()
	for _, table := range trafficTables {
		if got := f.rows(t, `SELECT '1' FROM aveloxis_data.`+table+` WHERE repo_id = $1`); len(got) != 0 {
			t.Errorf("%s holds %d rows of a GitLab repository, want none", table, len(got))
		}
	}
	if got := f.rows(t, `SELECT entity_type FROM aveloxis_ops.staging WHERE repo_id = $1 AND NOT processed`); len(got) != 0 {
		t.Errorf("unprocessed staging rows: %q, want none", got)
	}
}

// TestGitLabCloneCountsAreStoredEndToEnd drives a GitLab collection of
// the daily clones through the whole write path with a client that
// answers as the GitLab client does: the 30 UTC days ending on the
// collection day land in repo_clones with NULL uniques and the GitLab
// data source, the days the forge did not list as 0, and no row lands in
// any table only a traffic read fills. A read on the next day moves the
// period by one day and revises the day it shares; a later read the key
// cannot make (403) leaves the stored days as they were.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestGitLabCloneCountsAreStoredEndToEnd(t *testing.T) {
	const slug = "_avtraffic_gitlab_e2e"
	fx := newStagedTrafficFixture(t, slug, model.PlatformGitLab, "gitlab.com")
	day := func(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }
	want := func(from, to time.Time, counts map[string]int) []string {
		var out []string
		for d := from; !d.After(to); d = d.AddDate(0, 0, 1) {
			out = append(out, fmt.Sprintf("%s %d/NULL %s", d.Format(time.DateOnly), counts[d.Format(time.DateOnly)], gitlabStatsSource))
		}
		return out
	}
	check := func(what string, got, want []string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Errorf("%s:\n got  %q\n want %q", what, got, want)
		}
	}

	// Collection on 2026-11-30: fetches listed on two days only.
	first := map[string]int{"2026-11-02": 7, "2026-11-30": 3}
	client := &trafficFakeClient{plat: model.PlatformGitLab, clones: gitlabCloneRows(day(time.November, 30), first)}
	result, logs := fx.collectAndProcess(t, client, slug, day(time.November, 30).Add(8*time.Hour))
	if len(result.Errors) != 0 {
		t.Fatalf("collecting recorded errors: %v", result.Errors)
	}
	if logs != "" {
		t.Errorf("a successful read logged:\n%s", logs)
	}
	check("daily clones after the first read", fx.storedClones(t), want(day(time.November, 1), day(time.November, 30), first))
	fx.requireNoTrafficRows(t)

	// Collection on 2026-12-01: the period moves one day; the shared
	// current day is revised from 3 to 11 and the new day is a 0.
	second := map[string]int{"2026-11-02": 7, "2026-11-30": 11}
	client = &trafficFakeClient{plat: model.PlatformGitLab, clones: gitlabCloneRows(day(time.December, 1), second)}
	result, _ = fx.collectAndProcess(t, client, slug, day(time.December, 1).Add(8*time.Hour))
	if len(result.Errors) != 0 {
		t.Fatalf("collecting recorded errors: %v", result.Errors)
	}
	check("daily clones after the second read", fx.storedClones(t), want(day(time.November, 1), day(time.December, 1), second))
	fx.requireNoTrafficRows(t)

	// The key loses the Reporter role: one skip line, no error, no change.
	client = &trafficFakeClient{plat: model.PlatformGitLab, cloneErr: errGitLabStatsForbidden}
	result, logs = fx.collectAndProcess(t, client, slug, day(time.December, 2).Add(8*time.Hour))
	if len(result.Errors) != 0 {
		t.Errorf("a 403 recorded errors: %v", result.Errors)
	}
	if lines := skipLines(logs); len(lines) != 1 {
		t.Errorf("skip lines after a 403 = %d, want 1:\n%s", len(lines), logs)
	}
	check("daily clones after a 403", fx.storedClones(t), want(day(time.November, 1), day(time.December, 1), second))
	fx.requireNoTrafficRows(t)
}

// TestGitLabCloneReadThatSkipsOrFailsStoresNothing runs the skip and the
// error cases of the statistics read through the write path: a 403, a
// 404 and a 500 leave no clone row and no traffic row. The first two
// record no error and log one Info line; the 500 records the clones
// error and logs no skip.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).
func TestGitLabCloneReadThatSkipsOrFailsStoresNothing(t *testing.T) {
	last := time.Date(2026, time.November, 30, 0, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		slug     string
		err      error
		wantSkip int
		wantErrs int
	}{
		{"403", "_avtraffic_gitlab_403", errGitLabStatsForbidden, 1, 0},
		{"404", "_avtraffic_gitlab_404", errGitLabStatsNotFound, 1, 0},
		{"500", "_avtraffic_gitlab_500", errGitLabStatsServer, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := newStagedTrafficFixture(t, tc.slug, model.PlatformGitLab, "gitlab.com")
			// The fake returns rows with the error, as a client bug would.
			client := &trafficFakeClient{plat: model.PlatformGitLab, cloneErr: tc.err,
				clones: gitlabCloneRows(last, map[string]int{"2026-11-30": 3})}

			result, logs := fx.collectAndProcess(t, client, tc.slug, last)

			if len(result.Errors) != tc.wantErrs {
				t.Errorf("result.Errors = %v, want %d", result.Errors, tc.wantErrs)
			}
			if got := len(skipLines(logs)); got != tc.wantSkip {
				t.Errorf("skip lines = %d, want %d:\n%s", got, tc.wantSkip, logs)
			}
			if got := fx.storedClones(t); len(got) != 0 {
				t.Errorf("repo_clones holds %q after a %s, want none", got, tc.name)
			}
			fx.requireNoTrafficRows(t)
		})
	}
}
