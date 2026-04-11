package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRoute_ScancodeFiles_InvalidID(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/abc/scancode-files", nil)
	w := httptest.NewRecorder()
	srv.mux.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

func TestRoute_ScancodeFiles_Registered(t *testing.T) {
	srv := newTestServer()
	req := httptest.NewRequest("GET", "/api/v1/repos/1/scancode-files", nil)
	w := httptest.NewRecorder()
	// Will panic with nil store — we just verify the route is registered (not 404).
	defer func() { recover() }()
	srv.mux.ServeHTTP(w, req)
	if w.Code == http.StatusNotFound {
		t.Error("route /scancode-files should be registered")
	}
}
