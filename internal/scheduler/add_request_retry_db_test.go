// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestRetryApprovedAddRequests — O17 (v0.29.71): serve re-runs the approved
// add requests whose pass stopped early. A retryable failure leaves its item
// for the next pass (nothing is dropped) and the pass logs one WARN naming
// the requests still unfinished.
func TestRetryApprovedAddRequests(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()
	const login = "_avschedaddretry_probe"
	const urlPrefix = "https://github.com/_avschedaddretry-owner/_avschedaddretry-"
	const trigger = "_avtest_sched_add_retry"
	clean := func() {
		_, _ = pool.Exec(ctx, `DROP TRIGGER IF EXISTS `+trigger+` ON aveloxis_data.repos`)
		_, _ = pool.Exec(ctx, `DROP FUNCTION IF EXISTS aveloxis_data.`+trigger+`()`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%')`, urlPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_git LIKE $1 || '%'`, urlPrefix)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1))`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id IN (SELECT user_id FROM aveloxis_ops.users WHERE login_name = $1)`, login)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name = $1`, login)
	}
	clean()
	t.Cleanup(clean)
	uid, err := store.UpsertOAuthUser(ctx, db.OAuthUserInfo{Login: login, Provider: "github"})
	if err != nil {
		t.Fatal(err)
	}
	gid, err := store.CreateUserGroup(ctx, uid, "sched add retry probe")
	if err != nil {
		t.Fatal(err)
	}
	stuck := func(suffix string) int64 {
		t.Helper()
		var id int64
		if err := pool.QueryRow(ctx, `INSERT INTO aveloxis_ops.collection_add_requests
			(user_id, group_id, kind, status, item_count, created_at, decided_by, decided_at)
			VALUES ($1, $2, 'repos', 'approved', 1, NOW() - interval '3 hours', 0, NOW() - interval '3 hours')
			RETURNING request_id`, uid, gid).Scan(&id); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO aveloxis_ops.collection_add_request_items (request_id, repo_url) VALUES ($1, $2)`, id, urlPrefix+suffix); err != nil {
			t.Fatal(err)
		}
		return id
	}
	ok := stuck("works")
	flaky := stuck("flaky")
	flaky2 := stuck("flakytoo")
	if _, err := pool.Exec(ctx, `CREATE FUNCTION aveloxis_data.`+trigger+`() RETURNS trigger LANGUAGE plpgsql AS $f$
		BEGIN
			IF NEW.repo_git LIKE '%flaky' THEN
				RAISE EXCEPTION 'injected retryable failure' USING ERRCODE = '23503';
			END IF;
			IF NEW.repo_git LIKE '%flakytoo' THEN
				RAISE EXCEPTION 'second injected failure' USING ERRCODE = '23503';
			END IF;
			RETURN NEW;
		END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `CREATE TRIGGER `+trigger+` BEFORE INSERT ON aveloxis_data.repos FOR EACH ROW EXECUTE FUNCTION aveloxis_data.`+trigger+`()`); err != nil {
		t.Fatal(err)
	}

	var logs bytes.Buffer
	s := &Scheduler{store: store, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	s.retryApprovedAddRequests(ctx)

	var okLeft, flakyLeft int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_id IS NULL`, ok).Scan(&okLeft)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM aveloxis_ops.collection_add_request_items WHERE request_id = $1 AND repo_id IS NULL`, flaky).Scan(&flakyLeft)
	if okLeft != 0 {
		t.Errorf("the stuck request's item was not processed (%d left)", okLeft)
	}
	if flakyLeft != 1 {
		t.Errorf("a retryable failure must leave its item for the next pass, not drop it (%d left)", flakyLeft)
	}
	l := logs.String()
	if strings.Count(l, "approved add requests retried") != 1 || !strings.Contains(l, "level=WARN") {
		t.Errorf("want one WARN summarising the pass; log:\n%s", l)
	}
	// Whole-branch review D4: two requests stopped by different errors —
	// the WARN carried only the last one, and processAddRequest logs
	// nothing itself, so the other cause reached no log at all.
	for _, want := range []string{"injected retryable failure", "second injected failure", strconv.FormatInt(flaky, 10) + ": ", strconv.FormatInt(flaky2, 10) + ": "} {
		if !strings.Contains(l, want) {
			t.Errorf("the WARN must carry each unfinished request's own error (%q); log:\n%s", want, l)
		}
	}
}

// TestAddRequestRetryRunsHourly pins the wiring: the retry runs on the
// hourly maintenance tick, single-flight.
func TestAddRequestRetryRunsHourly(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "internal/scheduler/scheduler.go"))
	if !strings.Contains(src, `stagingCleanupTicker := time.NewTicker(hourlyMaintenanceInterval)`) {
		t.Error("the hourly maintenance ticker must use hourlyMaintenanceInterval (the retry's age threshold)")
	}
	if !strings.Contains(src, `s.singleFlight(&s.addRequestRetryActive, "add-request-retry", func() { s.retryApprovedAddRequests(ctx) })`) {
		t.Error("the hourly tick must run retryApprovedAddRequests single-flight")
	}
}
