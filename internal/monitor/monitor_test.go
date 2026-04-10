package monitor

// The monitor.Server takes a *db.PostgresStore directly (not an interface),
// so its HTTP handlers cannot be tested without a live PostgreSQL connection.
//
// Integration tests require setting AVELOXIS_TEST_DB to a valid connection
// string. Unit-testable helpers can be added here as the package evolves.

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestMonitorIntegration(t *testing.T) {
	t.Skip("requires live PostgreSQL — set AVELOXIS_TEST_DB to run")
}

// TestHandleAddRepoValidation tests the handleAddRepo endpoint's input
// validation by constructing a Server with a nil store. The endpoint does not
// touch the database for the validation-failure path (empty body / missing URL),
// so this works without a DB connection.
func TestHandleAddRepoEmptyBody(t *testing.T) {
	s := &Server{mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/repos", s.handleAddRepo)

	req := httptest.NewRequest("POST", "/api/repos", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if !strings.Contains(w.Body.String(), "url is required") {
		t.Errorf("body = %q, want it to contain %q", w.Body.String(), "url is required")
	}
}

func TestHandleAddRepoInvalidJSON(t *testing.T) {
	s := &Server{mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/repos", s.handleAddRepo)

	req := httptest.NewRequest("POST", "/api/repos", strings.NewReader("not json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandleAddRepoWithURL(t *testing.T) {
	s := &Server{mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/repos", s.handleAddRepo)

	body := `{"url": "https://github.com/test/repo"}`
	req := httptest.NewRequest("POST", "/api/repos", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if !strings.Contains(w.Body.String(), "aveloxis add-repo") {
		t.Errorf("body = %q, want it to contain CLI instructions", w.Body.String())
	}
}

func TestHandlePrioritizeInvalidID(t *testing.T) {
	s := &Server{mux: http.NewServeMux()}
	s.mux.HandleFunc("POST /api/prioritize/{repoID}", s.handlePrioritize)

	req := httptest.NewRequest("POST", "/api/prioritize/abc", nil)
	w := httptest.NewRecorder()

	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
