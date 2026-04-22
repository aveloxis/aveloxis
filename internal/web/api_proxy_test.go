package web

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/augurlabs/aveloxis/internal/config"
)

// TestTemplatesHaveNoHardcodedLocalhostAPI regresses the root cause of the
// compare-autocomplete outage: the templates used to hardcode
// 'http://localhost:8383', which silently failed for any browser not running
// on the same host as the api process. Same-origin relative URLs fix this.
// If someone re-introduces a hardcoded API host, this test fails before ship.
func TestTemplatesHaveNoHardcodedLocalhostAPI(t *testing.T) {
	forbidden := []string{
		"http://localhost:8383",
		"https://localhost:8383",
		"http://127.0.0.1:8383",
		"https://127.0.0.1:8383",
	}
	for _, f := range forbidden {
		if strings.Contains(allTemplates, f) {
			t.Errorf("templates contain hardcoded API host %q — use relative /api/v1/... URLs instead so the browser always fetches from the page's own origin (same-origin, no CORS, works behind nginx/TLS)", f)
		}
	}
}

// TestTemplatesUseRelativeAPIFetches ensures the three compare pages still
// call /api/v1/repos/search — the endpoint that drives the autocomplete
// dropdown. Without a search fetch, the dropdown stays empty.
func TestTemplatesUseRelativeAPIFetches(t *testing.T) {
	if !strings.Contains(allTemplates, "/api/v1/repos/search") {
		t.Error("templates must call /api/v1/repos/search for the compare autocomplete dropdown")
	}
	// At least one API fetch must be wired. More thorough shape checks live
	// in dashboard_test.go (structural) and api_proxy_integration_test.go
	// (runtime).
}

// TestAPIProxyForwardsToInternalURL is the mini-e2e for recommendation #1:
// stand up a fake api on httptest, configure the web server to proxy to it,
// make an authenticated /api/v1/repos/search request to the web server's
// handler, and verify the fake api's JSON comes back. This simulates the
// full production path (browser → nginx → web :8082 → internal proxy →
// api :8383) because nginx is transparent to the handler-level test.
//
// This is the test that would have caught the original outage: it fails if
// the web server doesn't wire a /api/* proxy, if it points at the wrong URL,
// if auth isn't required (security regression), or if the upstream response
// doesn't make it back to the browser.
func TestAPIProxyForwardsToInternalURL(t *testing.T) {
	// Fake api server stands in for `aveloxis api` on :8383.
	var gotPath, gotQuery string
	fakeAPI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[{"id":42,"owner":"augurlabs","name":"aveloxis"}]`))
	}))
	defer fakeAPI.Close()

	s := newTestServerWithAPIURL(t, fakeAPI.URL)

	// Unauthenticated request: must NOT proxy (leaks data otherwise).
	reqAnon := httptest.NewRequest(http.MethodGet, "/api/v1/repos/search?q=av", nil)
	wAnon := httptest.NewRecorder()
	s.Handler().ServeHTTP(wAnon, reqAnon)
	if wAnon.Code == http.StatusOK {
		t.Errorf("anonymous /api request returned 200 — auth gate is missing; got body=%q", wAnon.Body.String())
	}

	// Authenticated request: must proxy.
	token := s.createSession(1, "tester", "", "github")
	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/search?q=av", nil)
	req.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: token})
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("authenticated proxy request: got %d, want 200. body=%q", w.Code, w.Body.String())
	}
	body, _ := io.ReadAll(w.Body)
	if !strings.Contains(string(body), "augurlabs") {
		t.Errorf("upstream response not forwarded: body=%q", body)
	}
	if gotPath != "/api/v1/repos/search" {
		t.Errorf("upstream saw path=%q, want /api/v1/repos/search", gotPath)
	}
	if gotQuery != "q=av" {
		t.Errorf("upstream saw query=%q, want q=av", gotQuery)
	}
}

// TestAPIProxyReturns502WhenBackendDown verifies the fail-fast behavior.
// If aveloxis api is stopped, the web server must return a quick 502 instead
// of hanging the browser — operators (and the existing JS .catch) can react.
func TestAPIProxyReturns502WhenBackendDown(t *testing.T) {
	// Point at a port nothing is listening on. 127.0.0.1:1 is almost always
	// closed; the proxy should fail immediately.
	s := newTestServerWithAPIURL(t, "http://127.0.0.1:1")
	token := s.createSession(1, "tester", "", "github")

	req := httptest.NewRequest(http.MethodGet, "/api/v1/repos/search?q=a", nil)
	req.AddCookie(&http.Cookie{Name: "aveloxis_session", Value: token})
	w := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		s.Handler().ServeHTTP(w, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("proxy hung when backend was down — fail-fast is broken")
	}
	if w.Code != http.StatusBadGateway {
		t.Errorf("backend-down proxy: got %d, want 502 Bad Gateway. body=%q", w.Code, w.Body.String())
	}
}

// TestAPIProxyDefaultURLFallback ensures an empty api_internal_url doesn't
// silently disable the proxy; it falls back to 127.0.0.1:8383 so the "just
// works" configuration (aveloxis start all, no custom config) keeps working.
func TestAPIProxyDefaultURLFallback(t *testing.T) {
	s := New(nil, config.WebConfig{Addr: ":0"}, nil, slog.Default())
	if s.apiProxy == nil {
		t.Fatal("empty api_internal_url should fall back to a default, not disable the proxy")
	}
}

// newTestServerWithAPIURL constructs a web server pointed at the given api URL.
// Store is nil because the proxy path never touches Postgres; if a test ever
// hits a non-/api route, it will nil-deref and we'll fix the test.
func newTestServerWithAPIURL(t *testing.T, apiURL string) *Server {
	t.Helper()
	return New(nil, config.WebConfig{
		Addr:           ":0",
		APIInternalURL: apiURL,
	}, nil, slog.Default())
}
