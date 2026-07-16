// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// v0.27.16 — END-TO-END coverage for the contributor_retention
// compare path. The retention_threshold query param is a config knob
// controlling runtime behavior, so per the house rule it gets a full
// request → SQL → response-shape test, not just the parser unit test:
// the 2026-06-03 mailing_list_backfill_months lesson was that
// per-layer tests can all pass while a downstream layer clobbers the
// value. This test also proves different thresholds cannot collide
// in the 60s compare cache (the key carries the threshold).

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
)

func TestRetentionCompareEndToEnd(t *testing.T) {
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
	// Migrate BEFORE seeding (house rule — fresh-DB race class).
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	// Seed: one repeat contributor (5 issues, first 2025-02) and one
	// drive-by (1 issue, 2025-05).
	uniq := time.Now().UnixNano()
	slug := fmt.Sprintf("_avrete2e-%d", uniq)
	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Owner: "_avrete2e", Name: slug,
		GitURL:   "https://github.com/_avrete2e/" + slug,
		Platform: model.PlatformGitHub,
	})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	seedContributor := func(login string) string {
		var id string
		if err := store.Pool().QueryRow(ctx, `
			INSERT INTO aveloxis_data.contributors (cntrb_login)
			VALUES ($1) ON CONFLICT (cntrb_login) WHERE cntrb_login != ''
			DO UPDATE SET cntrb_login = EXCLUDED.cntrb_login
			RETURNING cntrb_id::text`, login).Scan(&id); err != nil {
			t.Fatalf("seed contributor: %v", err)
		}
		return id
	}
	seedIssue := func(cntrb string, n int64, when time.Time) {
		if _, err := store.Pool().Exec(ctx, `
			INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, issue_number, reporter_id, created_at)
			VALUES ($1, $2, $3, $4::uuid, $5)`, repoID, n, n%1000000, cntrb, when); err != nil {
			t.Fatalf("seed issue: %v", err)
		}
	}
	feb := time.Date(2025, 2, 10, 12, 0, 0, 0, time.UTC)
	may := time.Date(2025, 5, 20, 12, 0, 0, 0, time.UTC)
	repeater := seedContributor(fmt.Sprintf("_avrete2e_rep_%d", uniq))
	for i := 0; i < 5; i++ {
		seedIssue(repeater, uniq+int64(i), feb.Add(time.Duration(i)*time.Hour))
	}
	driveby := seedContributor(fmt.Sprintf("_avrete2e_drv_%d", uniq))
	seedIssue(driveby, uniq+100, may)

	srv, err := NewWithOptions(store, logger, Options{ExemptCIDRs: DefaultExemptCIDRs})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	type point struct {
		Bucket time.Time `json:"bucket"`
		Value  float64   `json:"value"`
	}
	type payload struct {
		Series []struct {
			Points []point            `json:"points"`
			Parts  map[string][]point `json:"parts"`
		} `json:"series"`
	}
	get := func(extra string) (payload, *http.Response) {
		url := fmt.Sprintf("%s/api/v1/compare?entities=repo:%d&metric=contributor_retention&bucket=month&since=2025-01-01&until=2025-08-01%s",
			ts.URL, repoID, extra)
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			body, _ := io.ReadAll(resp.Body)
			t.Fatalf("status %d: %s", resp.StatusCode, body)
		}
		var p payload
		if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return p, resp
	}
	sumAt := func(pts []point, month time.Month) float64 {
		for _, p := range pts {
			b := p.Bucket.UTC()
			if b.Year() == 2025 && b.Month() == month {
				return p.Value
			}
		}
		return 0
	}

	// Default threshold (4): repeater(5) → repeat in Feb; driveby(1)
	// → drive_by in May. Parts must be present; points = totals.
	p4, _ := get("")
	if len(p4.Series) != 1 || p4.Series[0].Parts == nil {
		t.Fatalf("retention response must carry one series with parts, got %+v", p4.Series)
	}
	parts := p4.Series[0].Parts
	if got := sumAt(parts["repeat"], time.February); got != 1 {
		t.Errorf("default threshold: Feb repeat = %v, want 1", got)
	}
	if got := sumAt(parts["drive_by"], time.May); got != 1 {
		t.Errorf("default threshold: May drive_by = %v, want 1", got)
	}
	if got := sumAt(p4.Series[0].Points, time.February); got != 1 {
		t.Errorf("points must carry per-bucket totals, Feb = %v, want 1", got)
	}

	// Threshold 10 (same entities/window, sent immediately after):
	// repeater flips to drive_by. If the cache key ignored the
	// threshold this request would return the threshold-4 body.
	p10, _ := get("&retention_threshold=10")
	if got := sumAt(p10.Series[0].Parts["repeat"], time.February); got != 0 {
		t.Errorf("threshold 10: Feb repeat = %v, want 0 (cache must not collide across thresholds)", got)
	}
	if got := sumAt(p10.Series[0].Parts["drive_by"], time.February); got != 1 {
		t.Errorf("threshold 10: Feb drive_by = %v, want 1", got)
	}

	// Bad thresholds are a 400, not a 500 or a silent default.
	for _, bad := range []string{"&retention_threshold=0", "&retention_threshold=abc"} {
		resp, err := http.Get(fmt.Sprintf("%s/api/v1/compare?entities=repo:%d&metric=contributor_retention%s", ts.URL, repoID, bad))
		if err != nil {
			t.Fatalf("GET bad threshold: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: status %d, want 400", bad, resp.StatusCode)
		}
	}

	// Single-series metrics on the same route must NOT grow a parts
	// field (omitempty contract).
	respC, err := http.Get(fmt.Sprintf("%s/api/v1/compare?entities=repo:%d&metric=issues&bucket=month&since=2025-01-01&until=2025-08-01", ts.URL, repoID))
	if err != nil {
		t.Fatalf("GET issues: %v", err)
	}
	defer respC.Body.Close()
	var raw map[string]any
	if err := json.NewDecoder(respC.Body).Decode(&raw); err != nil {
		t.Fatalf("decode issues: %v", err)
	}
	if series, ok := raw["series"].([]any); ok && len(series) > 0 {
		if first, ok := series[0].(map[string]any); ok {
			if _, has := first["parts"]; has {
				t.Error("single-series metrics must omit the parts field (omitempty)")
			}
		}
	}
}
