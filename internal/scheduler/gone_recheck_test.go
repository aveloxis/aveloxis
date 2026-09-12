// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// v0.29.7 — the gone-repo recheck ticker. Wiring pin: the run loop
// owns an hourly ticker gated on the config off switch, routes the
// tick through singleFlight, and the probe seam defaults to the ONE
// redirect-following probe prelim and mark-gone-repos use (SR-17).
func TestGoneRecheckWiring(t *testing.T) {
	run := srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"), "func (s *Scheduler) Run(")
	for _, needle := range []string{
		"case <-goneRecheckC:",
		`s.singleFlight(&s.goneRecheckActive, "gone-recheck", func() { s.runGoneRecheck(ctx) })`,
	} {
		if !strings.Contains(run, needle) {
			t.Errorf("Scheduler.Run must contain %q", needle)
		}
	}
	// The off switch is structural (review round 1): the ticker is
	// created AND its channel assigned INSIDE the enabled gate, so a
	// disabled ticker is a nil channel the select never fires. Matching
	// the gate's block, not the file, is what makes "the accessor
	// string appears somewhere" (it also appears in the startup log
	// line) insufficient.
	gate := "if s.cfg.Collection.GoneRepoRecheckEnabled() {"
	gi := strings.Index(run, gate)
	if gi < 0 {
		t.Fatalf("Scheduler.Run must gate the ticker on %q", gate)
	}
	block := run[gi:]
	if end := strings.Index(block, "\n\t}\n"); end > 0 {
		block = block[:end]
	}
	for _, needle := range []string{
		"goneRecheckTicker := time.NewTicker(goneRecheckTick)",
		"goneRecheckC = goneRecheckTicker.C",
	} {
		if !strings.Contains(block, needle) {
			t.Errorf("%q must sit inside the enabled gate's block, not outside it", needle)
		}
	}
	if strings.Count(run, "goneRecheckC = ") != 1 {
		t.Error("goneRecheckC must be assigned exactly once — inside the gate")
	}
	src, err := os.ReadFile("gone_recheck.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "var goneProbe = collector.ResolveRedirectTarget") {
		t.Error("the probe seam must default to collector.ResolveRedirectTarget — one probe, every consumer agrees on what gone means")
	}
}

// End-to-end against a live database with the probe seamed: every
// verdict class lands where the docs say it does.
func TestGoneRecheckVerdictsEndToEnd(t *testing.T) {
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
	const slug = "_avgonetick"
	cleanup := func() {
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN
			(SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`')`)
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '`+slug+`'`)
	}
	cleanup()
	t.Cleanup(cleanup)

	type row struct {
		id  int64
		url string
	}
	mk := func(name string) row {
		url := "https://github.com/" + slug + "/" + name
		id, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: model.PlatformGitHub, GitURL: url, Owner: slug, Name: name,
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.MarkRepoGone(ctx, id); err != nil {
			t.Fatal(err)
		}
		// Age the sideline stamp so every row is due.
		if _, err := store.Pool().Exec(ctx,
			`UPDATE aveloxis_data.repos SET repo_gone_checked_at = NULL WHERE repo_id = $1`, id); err != nil {
			t.Fatal(err)
		}
		return row{id, url}
	}
	back := mk("back")       // 200 → resurrected + queued
	still := mk("still")     // 404 → stays gone, checked
	flaky := mk("flaky")     // 503 → stays gone, checked (bounded: retried next cadence, not next tick)
	unreach := mk("unreach") // transport error → stays gone, checked (same bound — review round 1 MEDIUM)

	// Isolation on the shared scratch DB (review round 1): the claim set
	// is global, so any OTHER gone row with a NULL/aged stamp would be
	// claimed too. Park every foreign gone row as freshly checked.
	if _, err := store.Pool().Exec(ctx,
		`UPDATE aveloxis_data.repos SET repo_gone_checked_at = NOW()
		 WHERE repo_gone_at IS NOT NULL AND repo_owner <> $1`, slug); err != nil {
		t.Fatal(err)
	}

	verdicts := map[string]struct {
		status int
		err    error
	}{
		back.url:    {http.StatusOK, nil},
		still.url:   {http.StatusNotFound, nil},
		flaky.url:   {http.StatusServiceUnavailable, nil},
		unreach.url: {0, errors.New("dial tcp: i/o timeout")},
	}
	prev := goneProbe
	ours := func(url string) bool { return strings.Contains(url, "/"+slug+"/") }
	goneProbe = func(_ context.Context, url string) (string, int, error) {
		if !ours(url) {
			// A foreign row slipped past the parking above (a concurrent
			// package's fixture on the shared scratch DB): answer "still
			// gone". NOTE this is not a no-op — the 404 arm stamps the
			// foreign row's check, which is the known cross-package
			// class on this DB (reference_runlocal_db); the claim set is
			// global by design and the fixture assertions below are
			// per-row, so the foreign write cannot make them vacuous.
			return url, http.StatusNotFound, nil
		}
		v, ok := verdicts[url]
		if !ok {
			t.Errorf("probe called for an unexpected URL %q", url)
			return "", 0, errors.New("unexpected")
		}
		return url, v.status, v.err
	}
	t.Cleanup(func() { goneProbe = prev })

	cfg := config.DefaultConfig()
	s := New(store, nil, nil, logger, Config{Collection: &cfg.Collection})
	s.runGoneRecheck(ctx)

	state := func(id int64) (gone bool, checked bool, queued bool) {
		var g, c *time.Time
		if err := store.Pool().QueryRow(ctx,
			`SELECT repo_gone_at, repo_gone_checked_at FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&g, &c); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := store.Pool().QueryRow(ctx,
			`SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return g != nil, c != nil, n > 0
	}
	if gone, _, queued := state(back.id); gone || !queued {
		t.Errorf("200 must resurrect: gone=%v queued=%v", gone, queued)
	}
	if gone, checked, queued := state(still.id); !gone || !checked || queued {
		t.Errorf("404 must stay gone and stamp the check: gone=%v checked=%v queued=%v", gone, checked, queued)
	}
	if gone, checked, queued := state(flaky.id); !gone || !checked || queued {
		t.Errorf("5xx must stay gone AND stamp the check (a sticky-indeterminate cohort must not dominate every tick): gone=%v checked=%v queued=%v", gone, checked, queued)
	}
	if gone, checked, queued := state(unreach.id); !gone || !checked || queued {
		t.Errorf("a transport error must stay gone AND stamp the check (an unreachable cohort must not head every tick): gone=%v checked=%v queued=%v", gone, checked, queued)
	}
	// A second run within the cadence probes NONE of our rows: every
	// non-definitive answer was bounded to the cadence, and the
	// resurrected row left the gone set.
	probed := 0
	goneProbe = func(_ context.Context, url string) (string, int, error) {
		if ours(url) {
			probed++
			t.Errorf("second run must not re-probe a stamped row, got %q", url)
		}
		return url, http.StatusNotFound, nil
	}
	s.runGoneRecheck(ctx)
	if probed != 0 {
		t.Errorf("second run must probe none of the fixture rows, probed %d", probed)
	}
}
