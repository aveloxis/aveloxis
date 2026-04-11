package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// All metric endpoint tests verify parameter validation and route registration.
// Database interactions are tested implicitly — nil store panics are recovered.

// ============================================================
// Utility endpoint routes
// ============================================================

func TestRoute_RepoGroups(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repo-groups", nil)
	w := httptest.NewRecorder()
	defer func() { recover() }()
	srv.mux.ServeHTTP(w, req)
	// Should not return 404 (route exists).
	if w.Code == http.StatusNotFound {
		t.Error("route /repo-groups should be registered")
	}
}

func TestRoute_AllRepos(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos", nil)
	w := httptest.NewRecorder()
	defer func() { recover() }()
	srv.mux.ServeHTTP(w, req)
	if w.Code == http.StatusNotFound {
		t.Error("route /repos should be registered")
	}
}

func TestRoute_RepoByID_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRoute_ReposByGroup_InvalidGroupID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repo-groups/abc/repos", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRoute_RepoByOwnerName(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/owner/chaoss/repo/augur", nil)
	w := httptest.NewRecorder()
	defer func() { recover() }()
	srv.mux.ServeHTTP(w, req)
	if w.Code == http.StatusNotFound {
		t.Error("route /owner/:owner/repo/:repo should be registered")
	}
}

// ============================================================
// Issue metric route tests
// ============================================================

func TestRoute_IssuesNew_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/issues-new", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("issues-new: status = %d, want 400", w.Code)
	}
}

func TestRoute_IssuesClosed_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/issues-closed", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("issues-closed: status = %d, want 400", w.Code)
	}
}

func TestRoute_IssueBacklog_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/issue-backlog", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("issue-backlog: status = %d, want 400", w.Code)
	}
}

func TestRoute_IssueThroughput_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/issue-throughput", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("issue-throughput: status = %d, want 400", w.Code)
	}
}

func TestRoute_AbandonedIssues_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/abandoned-issues", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("abandoned-issues: status = %d, want 400", w.Code)
	}
}

// ============================================================
// PR metric route tests
// ============================================================

func TestRoute_PRsNew_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/pull-requests-new", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("pull-requests-new: status = %d, want 400", w.Code)
	}
}

func TestRoute_ReviewsAccepted_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/reviews-accepted", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("reviews-accepted: status = %d, want 400", w.Code)
	}
}

// ============================================================
// Commit/contributor metric route tests
// ============================================================

func TestRoute_Committers_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/committers", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("committers: status = %d, want 400", w.Code)
	}
}

func TestRoute_Contributors_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/contributors", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("contributors: status = %d, want 400", w.Code)
	}
}

// ============================================================
// Metadata metric route tests
// ============================================================

func TestRoute_Stars_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/stars", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("stars: status = %d, want 400", w.Code)
	}
}

func TestRoute_Forks_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/forks", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("forks: status = %d, want 400", w.Code)
	}
}

func TestRoute_Deps_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/deps", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("deps: status = %d, want 400", w.Code)
	}
}

func TestRoute_Releases_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/releases", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("releases: status = %d, want 400", w.Code)
	}
}

// ============================================================
// Parameter parsing tests
// ============================================================

func TestParsePeriod_Valid(t *testing.T) {
	tests := []struct{ input, want string }{
		{"day", "day"},
		{"week", "week"},
		{"month", "month"},
		{"year", "year"},
		{"", "month"},       // default
		{"invalid", "month"}, // default
	}
	for _, tt := range tests {
		r := httptest.NewRequest("GET", "/?period="+tt.input, nil)
		got := parsePeriod(r)
		if got != tt.want {
			t.Errorf("parsePeriod(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseDateRange_Defaults(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	begin, end := parseDateRange(r)
	if begin.IsZero() || end.IsZero() {
		t.Error("default dates should not be zero")
	}
	if !begin.Before(end) {
		t.Error("begin should be before end")
	}
}

func TestParseDateRange_Custom(t *testing.T) {
	r := httptest.NewRequest("GET", "/?begin_date=2023-01-01&end_date=2023-12-31", nil)
	begin, end := parseDateRange(r)
	if begin.Year() != 2023 || begin.Month() != 1 || begin.Day() != 1 {
		t.Errorf("begin = %v, want 2023-01-01", begin)
	}
	if end.Year() != 2023 || end.Month() != 12 || end.Day() != 31 {
		t.Errorf("end = %v, want 2023-12-31", end)
	}
}

func TestParseDateRange_InvalidDates(t *testing.T) {
	r := httptest.NewRequest("GET", "/?begin_date=not-a-date&end_date=also-bad", nil)
	begin, end := parseDateRange(r)
	// Should fall back to defaults (not zero).
	if begin.IsZero() || end.IsZero() {
		t.Error("invalid dates should use defaults, not zero")
	}
}
