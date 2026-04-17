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

// TestErrForbiddenIsExported — sentinel must exist so callers can check
// via errors.Is(err, ErrForbidden) and treat 403 on an optional endpoint
// (e.g., GitLab merge_requests on a private project, /traffic/clones on a
// repo without push access) as non-fatal.
func TestErrForbiddenIsExported(t *testing.T) {
	if ErrForbidden == nil {
		t.Fatal("ErrForbidden must be a non-nil exported sentinel error")
	}
	if ErrForbidden.Error() == "" {
		t.Error("ErrForbidden must have a non-empty message")
	}
}

// TestGet_403ReturnsErrForbidden — 403 responses without rate-limit headers
// must return an error that wraps ErrForbidden. We pass an X-RateLimit-Remaining
// of 1 so the rate-limit branch is NOT taken.
func TestGet_403ReturnsErrForbidden(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No Retry-After and remaining > 0 → falls into the "forbidden for
		// other reasons" branch (private repo / insufficient scope).
		w.Header().Set("X-RateLimit-Remaining", "1")
		w.WriteHeader(http.StatusForbidden)
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"test-token"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	_, err := client.Get(context.Background(), "/projects/x/merge_requests")
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
	if !errors.Is(err, ErrForbidden) {
		t.Errorf("expected errors.Is(err, ErrForbidden), got %v", err)
	}
}
