<!--
SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
SPDX-License-Identifier: MIT
-->

# Adding a REST endpoint

The aveloxis REST API runs as a separate process on `:8383` and is consumed by the web GUI's Chart.js panels and any external client. Adding an endpoint is a thin task — most of the work is shaping the query and the response.

## Where the API lives

```
internal/api/
├── server.go              # Mux + route registration + handler signatures
├── metrics.go             # /api/v1/repos/{id}/stats and similar count endpoints
├── server_test.go         # End-to-end handler tests via httptest
├── cors_test.go           # CORS preflight + Access-Control-* header pins
├── metrics_test.go        # Per-handler behavioral tests
├── scancode_files_test.go
└── scancode_freshness_test.go
```

The server uses Go 1.22+ ServeMux pattern matching (`{repoID}` path params). It's intentionally plain — no router library, no middleware stack beyond CORS.

## Existing endpoints

The API has grown to ~95 registered routes — the authoritative, tripwire-enforced reference is [`docs/guide/api.md`](../guide/api.md) (every registered route must appear there AND carry a smoke recipe in `internal/api/endpoint_smoke_test.go`, or the build fails). Skim `internal/api/server.go`'s route registrations for the code-side list.

## Walkthrough: add `GET /api/v1/repos/{repoID}/contributors`

We'll add an endpoint that returns the top-N contributors for a repo, ranked by commit count. This is a representative example.

### Step 1 — write the failing test

```go
// internal/api/contributors_test.go
// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
    "encoding/json"
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestRepoContributorsHandler(t *testing.T) {
    srv := newTestServer(t)
    defer srv.Close()

    resp, err := http.Get(srv.URL + "/api/v1/repos/1/contributors?limit=5")
    if err != nil {
        t.Fatal(err)
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        t.Errorf("expected 200, got %d", resp.StatusCode)
    }

    var body struct {
        Contributors []struct {
            Login    string `json:"login"`
            Commits  int    `json:"commits"`
            Email    string `json:"email"`
        } `json:"contributors"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
        t.Fatalf("decode: %v", err)
    }
    // Test fixtures set up by newTestServer should produce specific
    // contributors — assert on them.
}

func TestRepoContributorsHandlerRejectsInvalidRepoID(t *testing.T) {
    srv := newTestServer(t)
    defer srv.Close()
    resp, _ := http.Get(srv.URL + "/api/v1/repos/notanumber/contributors")
    if resp.StatusCode != http.StatusBadRequest {
        t.Errorf("expected 400 for non-numeric repoID, got %d", resp.StatusCode)
    }
}

func TestRepoContributorsHandlerLimitDefaults(t *testing.T) {
    srv := newTestServer(t)
    defer srv.Close()
    resp, _ := http.Get(srv.URL + "/api/v1/repos/1/contributors")  // no limit
    if resp.StatusCode != http.StatusOK {
        t.Errorf("expected default limit to succeed, got %d", resp.StatusCode)
    }
}
```

Run it — it'll fail because the route doesn't exist.

### Step 2 — add the store method

The handler should be a thin shell. Put the SQL in `internal/db/`:

```go
// internal/db/repo_contributors.go
// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
    "context"
    "fmt"
)

// RepoContributor is the per-row return type for GetRepoContributors.
type RepoContributor struct {
    Login   string `json:"login"`
    Email   string `json:"email"`
    Commits int    `json:"commits"`
}

// GetRepoContributors returns up to `limit` contributors ranked by
// commit count for the given repo. Returns an empty slice (no error)
// when the repo has no commits yet.
func (s *PostgresStore) GetRepoContributors(ctx context.Context, repoID int64, limit int) ([]RepoContributor, error) {
    if limit <= 0 {
        limit = 25  // default
    }
    if limit > 500 {
        limit = 500  // cap
    }
    rows, err := s.pool.Query(ctx, `
        SELECT
            c.gh_login,
            COALESCE(c.cntrb_canonical, c.cntrb_email, '') AS email,
            COUNT(DISTINCT cm.cmt_commit_hash) AS commits
        FROM aveloxis_data.commits cm
        JOIN aveloxis_data.contributors c ON cm.cmt_ght_author_id = c.cntrb_id
        WHERE cm.repo_id = $1 AND COALESCE(c.cntrb_deleted, 0) = 0
        GROUP BY c.cntrb_id, c.gh_login, c.cntrb_canonical, c.cntrb_email
        ORDER BY commits DESC, c.gh_login ASC
        LIMIT $2`, repoID, limit)
    if err != nil {
        return nil, fmt.Errorf("query repo contributors: %w", err)
    }
    defer rows.Close()

    var out []RepoContributor
    for rows.Next() {
        var rc RepoContributor
        if err := rows.Scan(&rc.Login, &rc.Email, &rc.Commits); err != nil {
            return nil, fmt.Errorf("scan: %w", err)
        }
        out = append(out, rc)
    }
    return out, rows.Err()
}
```

### Step 3 — add the handler

```go
// internal/api/contributors.go
// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
    "encoding/json"
    "net/http"
    "strconv"
)

func (s *Server) handleRepoContributors(w http.ResponseWriter, r *http.Request) {
    repoID, err := strconv.ParseInt(r.PathValue("repoID"), 10, 64)
    if err != nil {
        http.Error(w, "invalid repoID", http.StatusBadRequest)
        return
    }

    limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
    contributors, err := s.store.GetRepoContributors(r.Context(), repoID, limit)
    if err != nil {
        s.logger.Warn("GetRepoContributors failed",
            "repo_id", repoID, "error", err)
        http.Error(w, "internal error", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]any{
        "repo_id":      repoID,
        "contributors": contributors,
    })
}
```

### Step 4 — register the route

```go
// internal/api/server.go — inside NewServer / registerRoutes
s.mux.HandleFunc("GET /api/v1/repos/{repoID}/contributors", s.handleRepoContributors)
```

### Step 5 — run the tests

```bash
go test ./internal/api/ -run TestRepoContributors -v
```

Iterate until green.

### Step 6 — bump version, document

Bump `internal/db/version.go`. Add a changelog entry in `CLAUDE.md`. Update `docs/guide/api.md` with the new endpoint signature and response shape.

## Patterns to follow

### Path parameters via `r.PathValue("name")`

Go 1.22+ ServeMux pattern matching. Mux registration uses `{name}` placeholders; the handler reads via `r.PathValue("name")`. Always parse + validate immediately and return 400 on parse failure.

### Query params via `r.URL.Query().Get`

Don't bind query params via reflection / a library. Read them directly. Validate. Apply defaults and caps.

### Errors

- 400 for client errors (invalid input).
- 404 for resources that don't exist (use the existing patterns; look at `handleRepoStats` for the pattern of distinguishing "repo not found" from "repo has no data yet").
- 500 for server-side errors. Log the actual error; return a generic message to the client.

Never leak SQL errors or stack traces to the client.

### Response shape

JSON. Wrap collections in a named field:

```jsonc
{"contributors": [ /* ... */ ], "repo_id": 123}
```

Not just `[...]` — that locks you in if you ever need to add pagination metadata.

For paginated endpoints, follow the existing `ListQueuePage` shape:

```jsonc
{"items": [ /* ... */ ], "total": 1234, "page": 1, "page_size": 100}
```

### CORS

The existing server.go enables CORS via middleware. Don't disable it. The web GUI's Chart.js fetches go through CORS — without it, charts silently fail.

### Caching

If the underlying query is expensive (full table scan, complex aggregate), consider adding a process-level cache like `internal/monitor/queue_stats_cache.go`. 60-second TTL + stale-on-error is the established pattern. See v0.18.30 for the rationale.

### Logging

`s.logger.Warn` on errors. Include repo_id and any other context. NEVER log the API key or session token — those don't appear on REST endpoints anyway since the API is currently unauthenticated, but be careful if you add auth.

## What NOT to do

- **Don't add middleware libraries.** Stdlib net/http is sufficient. Aveloxis has resisted gin/echo/chi for years and isn't adding them now.
- **Don't write SQL in the handler.** Put it in `internal/db/`. Handlers are thin shells.
- **Don't change response shape of existing endpoints.** The web GUI templates and external consumers pin them. Add new endpoints; don't repurpose old ones.
- **Don't add authentication ad hoc.** The current REST API is intentionally unauthenticated (read-only, local-network). If you need auth, design it across all endpoints with the maintainers; don't slip it into one handler.

## Wiring to the web GUI

If your endpoint feeds a chart or panel, that's a separate task — see [`adding-a-visualization.md`](adding-a-visualization.md).

The web GUI consumes the REST API via `fetch()` in the templates. The base URL is hardcoded to `http://localhost:8383` in the templates currently (one of the known-issues items in the CLAUDE.md status section). When you add an endpoint, you'll likely also update a template under `internal/web/templates.go` to call it.
