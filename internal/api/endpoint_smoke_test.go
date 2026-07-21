// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

// Endpoint smoke harness (operator directive, 2026-07-15: "we should
// be testing that our APIs return data as part of our routine
// testing"). Third instance of the same defect class in one week made
// the gap undeniable: handler tests with fake stores verify wiring,
// but nothing routinely EXECUTED each endpoint's real SQL against the
// real schema — so /project-languages returned SQLSTATE 42703 (Augur
// column names that never existed in this schema) for its entire
// life, invisibly.
//
// This test boots a real Server on a real store (AVELOXIS_TEST_DB),
// seeds a minimal fixture, and fires EVERY registered route with
// valid parameters, asserting the response status and JSON validity.
// A completeness tripwire extracts the route list from server.go +
// metrics.go source: adding an endpoint without a smoke recipe fails
// the build. Skips must be explicit and carry a reason — a silent
// omission is exactly the hole this harness closes.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

type smokeRecipe struct {
	query      string // query string, no leading '?'
	body       string // JSON body for POST/PUT
	auth       string // "", "user", "admin"
	wantStatus []int  // default {200}
	skip       string // non-empty = skipped, with a visible reason
	after      string // "METHOD path" of a recipe that must run first
}

// smokeRecipes must cover every route registered in server.go and
// metrics.go. Path placeholders are filled from the seeded fixture.
var smokeRecipes = map[string]smokeRecipe{
	"GET /api/v1/health":                                    {},
	"GET /api/v1/metrics":                                   {},
	"GET /api/v1/repos":                                     {},
	"GET /api/v1/repos/search":                              {query: "q=smokerepo"},
	"GET /api/v1/repos/stats":                               {query: "ids={repoID}"},
	"GET /api/v1/repos/{repoID}":                            {},
	"GET /api/v1/repos/{repoID}/stats":                      {},
	"GET /api/v1/repos/{repoID}/timeseries":                 {},
	"GET /api/v1/repos/{repoID}/licenses":                   {},
	"GET /api/v1/repos/{repoID}/sbom":                       {},
	"GET /api/v1/repos/{repoID}/sbom?":                      {}, // placeholder-collision guard; unused
	"GET /api/v1/repos/{repoID}/scancode-licenses":          {},
	"GET /api/v1/repos/{repoID}/scancode-files":             {},
	"GET /api/v1/repos/{repoID}/scorecard":                  {},
	"GET /api/v1/repos/{repoID}/vulnerabilities":            {},
	"GET /api/v1/repos/{repoID}/contributions/identities":   {},
	"GET /api/v1/repos/{repoID}/contributions/affiliations": {},
	"GET /api/v1/repos/{repoID}/contributions/coverage":     {},
	"GET /api/v1/compare":                                   {query: "entities=repo:{repoID}&metric=contributors"},
	"GET /api/v1/compare/snapshot":                          {query: "entities=repo:{repoID}&metric=labor_investment"},
	"GET /api/v1/entities/search":                           {query: "q=smokerepo", auth: "user"},
	"GET /api/v1/mailing-list/stats":                        {},

	// Portal (Bearer unconditionally).
	"GET /api/v1/me":                            {auth: "user"},
	"GET /api/v1/groups":                        {auth: "user"},
	"POST /api/v1/groups":                       {auth: "user", body: `{"name":"smoke-extra-group"}`},
	"GET /api/v1/groups/{groupID}/repos":        {auth: "user"},
	"GET /api/v1/groups/{groupID}/orgs":         {auth: "user"}, // 2026-07-21 tracked-orgs section
	"GET /api/v1/groups/{groupID}/pending-adds": {auth: "user"}, // v0.27.20
	// The urls-array body exercises the 2026-07-21 bulk-add path (the
	// legacy single-url body is a parse-layer fallback into the same
	// code, pinned by portal_bulk_add_test.go).
	"POST /api/v1/groups/{groupID}/repos": {auth: "user", body: `{"urls":["https://github.com/_avsmoke/{repoName}"],"kind":"repo"}`},
	"GET /api/v1/home/repos":              {auth: "user"},
	"PUT /api/v1/repos/{repoID}/star":     {auth: "user"},
	"DELETE /api/v1/repos/{repoID}/star":  {auth: "user", after: "PUT /api/v1/repos/{repoID}/star"},

	// Admin.
	"GET /api/v1/admin/users":                                {auth: "admin"},
	"GET /api/v1/admin/groups/pending":                       {auth: "admin"},
	"GET /api/v1/admin/monitor/stats":                        {auth: "admin"},
	"GET /api/v1/admin/monitor/queue":                        {auth: "admin"},
	"POST /api/v1/admin/users/{userID}/admin":                {auth: "admin", body: `{"admin":true}`},
	"POST /api/v1/admin/groups/{groupID}/{decision}":         {auth: "admin"},
	"POST /api/v1/admin/monitor/queue/{repoID}/prioritize":   {auth: "admin"}, // v0.27.14 Boost (fixture seeds the queue row)
	"GET /api/v1/admin/add-requests":                         {auth: "admin"}, // v0.27.20 per-add approval queue
	"POST /api/v1/admin/add-requests/{requestID}/{decision}": {auth: "admin"}, // v0.27.20 (fixture seeds the pending request)

	// Augur-compat metric routes (metrics.go).
	"GET /api/v1/owner/{owner}/repo/{repo}":                    {},
	"GET /api/v1/rg-name/{rgName}":                             {},
	"GET /api/v1/rg-name/{rgName}/repo-name/{repoName}":        {},
	"GET /api/v1/repo-groups":                                  {},
	"GET /api/v1/repo-groups/{groupID}/repos":                  {},
	"GET /api/v1/repos/{repoID}/abandoned-issues":              {},
	"GET /api/v1/repos/{repoID}/average-issue-resolution-time": {},
	"GET /api/v1/repos/{repoID}/closed-issues-count":           {},
	"GET /api/v1/repos/{repoID}/code-changes":                  {},
	"GET /api/v1/repos/{repoID}/code-changes-lines":            {},
	"GET /api/v1/repos/{repoID}/committers":                    {},
	"GET /api/v1/repos/{repoID}/contributors":                  {},
	"GET /api/v1/repos/{repoID}/contributors-new":              {},
	"GET /api/v1/repos/{repoID}/deps":                          {},
	"GET /api/v1/repos/{repoID}/fork-count":                    {},
	"GET /api/v1/repos/{repoID}/forks":                         {},
	"GET /api/v1/repos/{repoID}/issue-backlog":                 {},
	"GET /api/v1/repos/{repoID}/issue-duration":                {},
	"GET /api/v1/repos/{repoID}/issue-throughput":              {},
	"GET /api/v1/repos/{repoID}/issues-active":                 {},
	"GET /api/v1/repos/{repoID}/issues-closed":                 {},
	"GET /api/v1/repos/{repoID}/issues-new":                    {},
	"GET /api/v1/repos/{repoID}/languages":                     {},
	"GET /api/v1/repos/{repoID}/libyear":                       {},
	"GET /api/v1/repos/{repoID}/open-issues-count":             {},
	"GET /api/v1/repos/{repoID}/pull-requests-new":             {},
	"GET /api/v1/repos/{repoID}/releases":                      {},
	"GET /api/v1/repos/{repoID}/repo-messages":                 {},
	"GET /api/v1/repos/{repoID}/review-duration":               {},
	"GET /api/v1/repos/{repoID}/reviews":                       {},
	"GET /api/v1/repos/{repoID}/reviews-accepted":              {},
	"GET /api/v1/repos/{repoID}/reviews-declined":              {},
	"GET /api/v1/repos/{repoID}/stars":                         {},
	"GET /api/v1/repos/{repoID}/stars-count":                   {},
	"GET /api/v1/repos/{repoID}/watchers":                      {},
	"GET /api/v1/repos/{repoID}/watchers-count":                {},

	"GET /api/v1/repos/{repoID}/project-languages": {},
	"GET /api/v1/repos/{repoID}/project-files":     {},
	"GET /api/v1/repos/{repoID}/project-lines":     {},
}

var smokeRouteRe = regexp.MustCompile(`HandleFunc\("((?:GET|POST|PUT|DELETE) [^"]+)"`)

// TestSmokeRecipesCoverEveryRoute is the completeness tripwire — it
// runs WITHOUT a database, so a missing recipe fails even the unit
// tier.
func TestSmokeRecipesCoverEveryRoute(t *testing.T) {
	src := mustReadFile(t, "server.go") + mustReadFile(t, "metrics.go")
	seen := 0
	for _, m := range smokeRouteRe.FindAllStringSubmatch(src, -1) {
		route := m[1]
		if _, ok := smokeRecipes[route]; !ok {
			t.Errorf("route %q has no smoke recipe — add one to smokeRecipes (or an explicit skip with a reason). Every endpoint must be executed against the real schema by TestEveryEndpointExecutes.", route)
		}
		seen++
	}
	if seen < 70 {
		t.Fatalf("route extraction found only %d routes — the regex likely rotted; fix it rather than letting the harness go blind", seen)
	}
}

// TestEveryEndpointExecutes boots the real server on the real store
// and fires every recipe. The bar: the endpoint's actual SQL executes
// against the actual schema (no 42703-class failures masked by fake
// stores) and the response is valid JSON with an expected status.
func TestEveryEndpointExecutes(t *testing.T) {
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

	// Migrate before seeding (v0.27.13): on a truly FRESH database
	// this package's tests can run BEFORE internal/db's migration
	// tests under go test's package parallelism, and the fixture
	// seed fails on missing relations. Populated scratch DBs masked
	// this — same cross-package fresh-DB race class as the
	// v0251Connect repo-group seed. Migrate is idempotent and cheap
	// when already applied.
	if err := store.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	fx := seedSmokeFixture(t, ctx, store)

	srv, err := NewWithOptions(store, logger, Options{
		ExemptCIDRs: DefaultExemptCIDRs,
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	fill := strings.NewReplacer(
		"{repoID}", fmt.Sprint(fx.repoID),
		"{groupID}", fmt.Sprint(fx.groupID),
		"{userID}", fmt.Sprint(fx.userID),
		"{decision}", "approve",
		"{requestID}", fmt.Sprint(fx.requestID),
		"{owner}", fx.owner,
		"{repo}", fx.repoName,
		"{repoName}", fx.repoName,
		"{rgName}", fx.rgName,
	)

	run := func(route string, r smokeRecipe) {
		t.Run(route, func(t *testing.T) {
			if r.skip != "" {
				t.Skip(r.skip)
			}
			method, path, _ := strings.Cut(route, " ")
			url := ts.URL + fill.Replace(path)
			if r.query != "" {
				url += "?" + fill.Replace(r.query)
			}
			var body io.Reader
			if r.body != "" {
				body = strings.NewReader(fill.Replace(r.body))
			}
			req, err := http.NewRequest(method, url, body)
			if err != nil {
				t.Fatal(err)
			}
			if r.auth != "" {
				req.Header.Set("Authorization", "Bearer "+fx.tokens[r.auth])
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			raw, _ := io.ReadAll(resp.Body)

			want := r.wantStatus
			if len(want) == 0 {
				want = []int{200}
			}
			ok := false
			for _, w := range want {
				if resp.StatusCode == w {
					ok = true
				}
			}
			if !ok {
				t.Fatalf("status %d (want %v) — body: %.300s", resp.StatusCode, want, raw)
			}
			ct := resp.Header.Get("Content-Type")
			if strings.Contains(ct, "json") && !json.Valid(raw) {
				t.Fatalf("response claims JSON but does not parse: %.200s", raw)
			}
		})
	}

	// Ordered pass first (recipes with dependencies), then the rest.
	ordered := []string{"PUT /api/v1/repos/{repoID}/star", "DELETE /api/v1/repos/{repoID}/star"}
	done := map[string]bool{}
	for _, route := range ordered {
		if r, ok := smokeRecipes[route]; ok {
			run(route, r)
			done[route] = true
		}
	}
	for route, r := range smokeRecipes {
		if done[route] || strings.HasSuffix(route, "?") { // collision-guard placeholder
			continue
		}
		run(route, r)
	}
}

type smokeFixture struct {
	repoID    int64
	groupID   int64
	userID    int
	owner     string
	repoName  string
	rgName    string
	tokens    map[string]string // "user" and "admin" bearer tokens
	requestID int64             // v0.27.20 pending add-request
}

// seedSmokeFixture creates the minimal graph the recipes need: a repo
// with one issue/PR/commit (so data-bearing endpoints exercise their
// JOINs), an admin user with a token, a PENDING group owned by that
// user containing the repo (so the approve decision and group reads
// work), and a queue row (repo listings join it).
func seedSmokeFixture(t *testing.T, ctx context.Context, store *db.PostgresStore) smokeFixture {
	t.Helper()
	suffix := time.Now().UnixNano()
	fx := smokeFixture{
		owner:    "_avsmoke",
		repoName: fmt.Sprintf("smokerepo%d", suffix),
		rgName:   fmt.Sprintf("_avsmoke_rg%d", suffix),
		tokens:   map[string]string{},
	}
	pool := store.Pool()

	var rgID int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repo_groups (rg_name) VALUES ($1) RETURNING repo_group_id`,
		fx.rgName).Scan(&rgID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id, repo_group_id)
		VALUES ($1, $2, $3, 1, $4) RETURNING repo_id`,
		fmt.Sprintf("https://github.com/_avsmoke/%s", fx.repoName),
		fx.owner, fx.repoName, rgID).Scan(&fx.repoID); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_queue (repo_id, status, priority, due_at)
		VALUES ($1, 'queued', 100, NOW()) ON CONFLICT (repo_id) DO NOTHING`, fx.repoID)
	_, _ = pool.Exec(ctx, `
		INSERT INTO aveloxis_data.issues (repo_id, platform_issue_id, platform_id, title, created_at)
		VALUES ($1, 1, 1, 'smoke issue', NOW())`, fx.repoID)
	_, _ = pool.Exec(ctx, `
		INSERT INTO aveloxis_data.pull_requests (repo_id, platform_pr_id, platform_id, pr_title, created_at)
		VALUES ($1, 1, 1, 'smoke pr', NOW())`, fx.repoID)
	_, _ = pool.Exec(ctx, `
		INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_name,
			cmt_author_email, cmt_author_date, cmt_author_timestamp)
		VALUES ($1, 'smokehash', 'f.go', 'smoke', 's@example.com', NOW()::date::text, NOW())`, fx.repoID)

	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, oauth_provider, email, admin)
		VALUES ($1, 'github', '', TRUE) RETURNING user_id`,
		fmt.Sprintf("_avsmoke_%d", suffix)).Scan(&fx.userID); err != nil {
		t.Fatal(err)
	}
	tok, err := store.CreateSessionToken(ctx, fx.userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	fx.tokens["user"], fx.tokens["admin"] = tok, tok // admin user serves both roles

	// PENDING group owned by the user, containing the repo — feeds the
	// group reads AND the admin approve decision.
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name, status)
		VALUES ($1, $2, 'pending') RETURNING group_id`,
		fx.userID, fmt.Sprintf("smoke-group-%d", suffix)).Scan(&fx.groupID); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`,
		fx.groupID, fx.repoID)

	// v0.27.20: pending add-request feeding the admin approvals routes.
	// The item URL is the already-tracked fixture repo so the approve
	// decision's background processing creates no new rows.
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.collection_add_requests (user_id, group_id, kind, status, item_count)
		VALUES ($1, $2, 'repos', 'pending', 1) RETURNING request_id`,
		fx.userID, fx.groupID).Scan(&fx.requestID); err != nil {
		t.Fatal(err)
	}
	_, _ = pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.collection_add_request_items (request_id, repo_url) VALUES ($1, $2)`,
		fx.requestID, fmt.Sprintf("https://github.com/_avsmoke/%s", fx.repoName))

	t.Cleanup(func() {
		for _, q := range []string{
			`DELETE FROM aveloxis_ops.user_repo_stars WHERE user_id = $1`,
			`DELETE FROM aveloxis_ops.user_session_tokens WHERE user_id = $1`,
		} {
			_, _ = pool.Exec(ctx, q, fx.userID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_request_items WHERE request_id IN (SELECT request_id FROM aveloxis_ops.collection_add_requests WHERE user_id = $1)`, fx.userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_add_requests WHERE user_id = $1`, fx.userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE user_id = $1)`, fx.userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE user_id = $1`, fx.userID)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE user_id = $1`, fx.userID)
		for _, q := range []string{
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.issues WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.pull_requests WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.commits WHERE repo_id = $1`,
			`DELETE FROM aveloxis_data.repos WHERE repo_id = $1`,
		} {
			_, _ = pool.Exec(ctx, q, fx.repoID)
		}
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_groups WHERE rg_name = $1`, fx.rgName)
	})
	return fx
}
