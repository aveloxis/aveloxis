package platform

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

// TestGet_404ReturnsErrNotFound verifies that HTTPClient.Get returns an error
// that wraps ErrNotFound when the server responds with 404. Callers (e.g., the
// staged collector) rely on errors.Is(err, ErrNotFound) to treat absent
// optional resources (releases, clone stats) as non-fatal.
func TestGet_404ReturnsErrNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"Not Found"}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"test-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	_, err := client.Get(context.Background(), "/repos/o/r/releases")
	if err == nil {
		t.Fatal("expected error for 404 response")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected errors.Is(err, ErrNotFound) to be true, got err=%v", err)
	}
}

// TestErrNotFoundIsExported verifies the sentinel is exported for collector
// callers to check via errors.Is.
func TestErrNotFoundIsExported(t *testing.T) {
	if ErrNotFound == nil {
		t.Fatal("ErrNotFound must be a non-nil exported sentinel error")
	}
	if ErrNotFound.Error() == "" {
		t.Error("ErrNotFound must have a non-empty message")
	}
}

// TestPaginate_404YieldsErrNotFound verifies that pagination of a 404 endpoint
// yields a single error that wraps ErrNotFound, which the collector uses to
// distinguish "endpoint returns 404" from transient/auth errors.
func TestPaginate_404YieldsErrNotFound(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"Not Found"}`))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"test-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	var gotErr error
	var gotItems int
	type dummy struct{}
	for _, err := range PaginateGitHub[dummy](context.Background(), client, "/repos/o/r/releases") {
		if err != nil {
			gotErr = err
			break
		}
		gotItems++
	}
	if gotErr == nil {
		t.Fatal("expected iter to yield an error for 404")
	}
	if !errors.Is(gotErr, ErrNotFound) {
		t.Errorf("expected errors.Is(gotErr, ErrNotFound), got %v", gotErr)
	}
	if gotItems != 0 {
		t.Errorf("expected zero items before error, got %d", gotItems)
	}
}
