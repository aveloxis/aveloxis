package web

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/config"
	"github.com/augurlabs/aveloxis/internal/db"
)

// This file implements recommendation #1 from the compare-autocomplete
// diagnosis: a test that would have caught the original outage by connecting
// "what the browser sees in the rendered JS" to "what URL the web server
// actually serves". The pre-v0.18.18 bug was that these two diverged silently:
// the JS pointed at http://localhost:8383 (the user's own machine) while the
// web server knew nothing about /api/*, so the autocomplete dropdown never
// populated and there was no server-side error to notice.
//
// These tests simulate the full production path described by the operator's
// nginx config (browser → nginx → web :8082 → internal proxy → api :8383).
// Nginx is transparent at the handler level, so we test web → proxy → api
// directly — the same invariants hold.

// TestCompareSearchWidgetRendersRelativeFetch renders the shared widget and
// asserts that the JS it produces calls a relative /api/v1/repos/search URL.
// A regression to an absolute http://localhost:... URL would fail here.
func TestCompareSearchWidgetRendersRelativeFetch(t *testing.T) {
	s := newTestServerWithAPIURL(t, "http://127.0.0.1:8383")

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "compareSearchWidget", map[string]any{"Prefix": "dash"}); err != nil {
		t.Fatalf("render compareSearchWidget: %v", err)
	}
	rendered := buf.String()

	// Positive: the relative endpoint must appear in the rendered JS.
	if !strings.Contains(rendered, "/api/v1/repos/search") {
		t.Errorf("rendered widget missing /api/v1/repos/search fetch call:\n%s", rendered)
	}
	// Positive: widget-specific IDs (derived from Prefix) must appear.
	for _, id := range []string{`id="dash-repo-search"`, `id="dash-search-results"`, `id="dash-repo-ids"`, `id="dash-selected"`} {
		if !strings.Contains(rendered, id) {
			t.Errorf("rendered widget missing %s", id)
		}
	}
	// Negative: no absolute API host should ever appear in the rendered JS.
	// This is the literal string that caused the outage.
	for _, forbidden := range []string{"http://localhost:8383", "https://localhost:8383", "http://127.0.0.1:8383"} {
		if strings.Contains(rendered, forbidden) {
			t.Errorf("rendered widget contains forbidden absolute URL %q — use relative fetch paths so the browser always resolves against its own origin", forbidden)
		}
	}
}

// TestCompareSearchWidgetRendersGrpPrefix is the same check for the "grp"
// prefix — the two-widget invariant: swapping the prefix is the ONLY
// difference between the dashboard and group page.
func TestCompareSearchWidgetRendersGrpPrefix(t *testing.T) {
	s := newTestServerWithAPIURL(t, "http://127.0.0.1:8383")

	var buf bytes.Buffer
	if err := s.tmpl.ExecuteTemplate(&buf, "compareSearchWidget", map[string]any{"Prefix": "grp"}); err != nil {
		t.Fatalf("render compareSearchWidget: %v", err)
	}
	rendered := buf.String()

	for _, id := range []string{`id="grp-repo-search"`, `id="grp-search-results"`, `id="grp-repo-ids"`, `id="grp-selected"`} {
		if !strings.Contains(rendered, id) {
			t.Errorf("rendered widget missing %s", id)
		}
	}
	// Cross-contamination check: rendering with Prefix=grp must not emit
	// any dash-* IDs. Pre-extraction copies were hand-edited and did drift;
	// the template now forbids that by construction, so this test pins it.
	for _, wrongID := range []string{"dash-repo-search", "dash-search-results"} {
		if strings.Contains(rendered, wrongID) {
			t.Errorf("rendered grp widget contains wrong-prefix %q — prefix isolation broken", wrongID)
		}
	}
}

// TestDashboardFullRenderHasRelativeFetch renders the real dashboard template
// with stub data and verifies the rendered HTML wires the compare widget to
// a relative API URL. This is the template-level equivalent of "load the
// page in a browser and look at the source".
func TestDashboardFullRenderHasRelativeFetch(t *testing.T) {
	s := newTestServerWithAPIURL(t, "http://127.0.0.1:8383")

	var buf bytes.Buffer
	err := s.tmpl.ExecuteTemplate(&buf, "dashboard", map[string]any{
		"Session": &Session{LoginName: "tester"},
		"Groups":  []db.UserGroup{{GroupID: 1, Name: "chaoss", RepoCount: 42}},
	})
	if err != nil {
		t.Fatalf("render dashboard: %v", err)
	}
	html := buf.String()

	if !strings.Contains(html, `id="compare-card"`) {
		t.Error("dashboard missing compare card")
	}
	if !strings.Contains(html, `id="dash-repo-search"`) {
		t.Error("dashboard missing compare search input (dash-repo-search)")
	}
	if !strings.Contains(html, "/api/v1/repos/search") {
		t.Error("dashboard JS does not call /api/v1/repos/search")
	}
	if strings.Contains(html, "http://localhost:8383") {
		t.Error("dashboard rendered with hardcoded http://localhost:8383 — regression to pre-v0.18.18 bug")
	}
}

// TestGroupFullRenderHasRelativeFetch is the companion for the group page.
// This is the page that "never worked" before: if the shared widget ever
// gets accidentally removed from the group template, this test catches it
// before ship.
func TestGroupFullRenderHasRelativeFetch(t *testing.T) {
	s := newTestServerWithAPIURL(t, "http://127.0.0.1:8383")

	group := &db.GroupDetail{GroupID: 7, Name: "chaoss", Repos: nil, Orgs: nil}
	var buf bytes.Buffer
	err := s.tmpl.ExecuteTemplate(&buf, "group", map[string]any{
		"Session":    &Session{LoginName: "tester"},
		"Group":      group,
		"Page":       1,
		"TotalPages": 1,
		"TotalRepos": 0,
		"Query":      "",
		"PageWindow": []int{1},
	})
	if err != nil {
		t.Fatalf("render group: %v", err)
	}
	html := buf.String()

	if !strings.Contains(html, `id="grp-compare-card"`) {
		t.Error("group page missing grp-compare-card")
	}
	if !strings.Contains(html, `id="grp-repo-search"`) {
		t.Error("group page missing compare search input (grp-repo-search)")
	}
	if !strings.Contains(html, "/api/v1/repos/search") {
		t.Error("group page JS does not call /api/v1/repos/search")
	}
	if strings.Contains(html, "http://localhost:8383") {
		t.Error("group page rendered with hardcoded http://localhost:8383")
	}
}

// TestFetchURLFromRenderedPageIsServedByProxy is the tightest coupling test:
// we extract the literal fetch URL from the rendered dashboard HTML and then
// issue that exact URL against the web server's Handler, asserting the fake
// api's response comes back. If the rendered JS ever starts fetching from a
// path that the proxy doesn't route, this test fails loudly — exactly the
// class of regression that silently broke the feature for months.
func TestFetchURLFromRenderedPageIsServedByProxy(t *testing.T) {
	// Fake api server: matches any /api/v1/repos/search?... and returns JSON.
	fakeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":1,"owner":"chaoss","name":"augur"}]`))
	}))
	defer fakeAPI.Close()

	s := newTestServerWithAPIURL(t, fakeAPI.URL)

	// 1. Render the dashboard (what a logged-in browser would see).
	var buf bytes.Buffer
	err := s.tmpl.ExecuteTemplate(&buf, "dashboard", map[string]any{
		"Session": &Session{LoginName: "tester"},
		"Groups":  []db.UserGroup{},
	})
	if err != nil {
		t.Fatalf("render dashboard: %v", err)
	}
	html := buf.String()

	// 2. Extract the fetch URL pattern from the JS. The widget builds it as
	//    API + '/api/v1/repos/search?q=' + encodeURIComponent(q).
	//    With API='' (same-origin), the effective URL is the suffix alone.
	re := regexp.MustCompile(`'(/api/v1/repos/search\?q=)'`)
	m := re.FindStringSubmatch(html)
	if m == nil {
		t.Fatalf("could not locate the fetch URL fragment in rendered dashboard JS; regex miss on /api/v1/repos/search")
	}
	fetchURL := m[1] + "chaoss"

	// 3. Issue that exact URL against the web server's Handler with a
	//    session cookie (what the browser would do automatically for a
	//    same-origin fetch).
	token := s.createSession(1, "tester", "", "github")
	req := httptest.NewRequest(http.MethodGet, fetchURL, nil)
	req.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: token})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("fetch URL %q from rendered page returned %d (want 200). body=%q", fetchURL, w.Code, w.Body.String())
	}
	body, _ := io.ReadAll(w.Body)
	if !strings.Contains(string(body), "chaoss") {
		t.Errorf("upstream response not forwarded for fetch URL %q; body=%q", fetchURL, body)
	}
}

// discardLogger is used when a test wants a Server but doesn't care about logs.
// Keeps test output clean.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// silenceServer replaces the default logger used by newTestServerWithAPIURL
// when a test wants to suppress the "api reverse proxy error" warning during
// the 502 test. Not used yet — left here so follow-up tests that expect an
// error path can opt in without adding plumbing.
var _ = discardLogger

// init guards the package-level config default — if someone accidentally
// reverts the default APIInternalURL to empty, the proxy would silently
// disable and the integration tests would fail. This check fails fast with
// a clearer message than a generic proxy-unreachable error.
func TestDefaultConfigHasAPIInternalURL(t *testing.T) {
	cfg := config.DefaultConfig()
	if cfg.Web.APIInternalURL == "" {
		t.Error("DefaultConfig.Web.APIInternalURL must be non-empty so the reverse proxy is wired by default")
	}
}
