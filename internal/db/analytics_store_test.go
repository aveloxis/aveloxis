// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

// v0.27.2 — integration coverage for the comparison-analytics queries,
// exercised against the REAL oss-aspen/8Knot collection in the scratch
// DB (repo 2160, collected live 2026-07-09).

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

func TestAnalyticsQueriesOnRealData(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	// Locate the live-collected 8Knot repo; skip gracefully if this
	// scratch DB doesn't have it.
	var repoID int64
	var owner string
	err = store.Pool().QueryRow(ctx, `
		SELECT repo_id, repo_owner FROM aveloxis_data.repos
		WHERE repo_name ILIKE '8knot' LIMIT 1`).Scan(&repoID, &owner)
	if err != nil {
		t.Skip("no 8Knot collection in this scratch DB")
	}
	ids := []int64{repoID}
	until := time.Now().UTC()
	since := until.AddDate(-3, 0, 0)

	// Every base temporal metric must execute and, for this active
	// repo, produce a non-empty 3-year weekly series.
	for _, metric := range []string{"contributors", "change_requests",
		"change_requests_merged", "issues", "issues_closed",
		"code_change_commits", "committers"} {
		pts, err := store.MetricWeeklySeries(ctx, ids, metric, "week", since, until)
		if err != nil {
			t.Fatalf("%s: %v", metric, err)
		}
		var sum float64
		for _, p := range pts {
			sum += p.Value
		}
		if len(pts) == 0 || sum == 0 {
			t.Errorf("%s: expected non-empty NONZERO series on real 8Knot data (len=%d sum=%f)", metric, len(pts), sum)
		}
	}
	// Month bucket + injection guard.
	if _, err := store.MetricWeeklySeries(ctx, ids, "issues", "month", since, until); err != nil {
		t.Errorf("month bucket: %v", err)
	}
	if _, err := store.MetricWeeklySeries(ctx, ids, "issues", "week'); DROP TABLE x;--", since, until); err == nil {
		t.Error("bucket whitelist must reject arbitrary strings")
	}

	// Snapshot queries must execute (values may be zero — the one-shot
	// collect doesn't run SCC/scancode/libyear).
	if _, err := store.LaborInvestmentSnapshot(ctx, ids); err != nil {
		t.Errorf("labor snapshot: %v", err)
	}
	if _, err := store.UpstreamDependenciesSnapshot(ctx, ids); err != nil {
		t.Errorf("deps snapshot: %v", err)
	}
	if _, err := store.LicenseCoverageSnapshot(ctx, ids); err != nil {
		t.Errorf("license snapshot: %v", err)
	}

	// Org entity resolution must include the repo under its owner.
	orgIDs, err := store.ResolveOrgRepos(ctx, "github.com", owner)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, id := range orgIDs {
		if id == repoID {
			found = true
		}
	}
	if !found {
		t.Errorf("org:github.com/%s must resolve to include repo %d, got %v", owner, repoID, orgIDs)
	}

	// Picker searches execute.
	if _, err := store.SearchOrgs(ctx, "aspen", 10); err != nil {
		t.Errorf("SearchOrgs: %v", err)
	}
}
