package platform

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// TestGitHubAuthHeader verifies that HTTPClient sends "Authorization: token <key>"
// for GitHub-style auth (the default and existing behavior).
func TestGitHubAuthHeader(t *testing.T) {
	var gotHeader string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"gh-test-token-123"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitHub)

	client.Get(context.Background(), "/repos/test")

	if gotHeader != "token gh-test-token-123" {
		t.Errorf("GitHub auth header = %q, want %q", gotHeader, "token gh-test-token-123")
	}
}

// TestGitLabAuthHeader verifies that HTTPClient sends "PRIVATE-TOKEN: <key>"
// for GitLab-style auth. This is the critical fix — previously all requests
// used GitHub's "Authorization: token" format, which GitLab silently ignores,
// causing zero issues/MRs/metadata to be collected.
func TestGitLabAuthHeader(t *testing.T) {
	var gotPrivateToken string
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPrivateToken = r.Header.Get("PRIVATE-TOKEN")
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte("[]"))
	}))
	defer server.Close()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	keys := NewKeyPool([]string{"gl-test-token-456"}, logger)
	client := NewHTTPClient(server.URL, keys, logger, AuthGitLab)

	client.Get(context.Background(), "/projects/test")

	if gotPrivateToken != "gl-test-token-456" {
		t.Errorf("GitLab PRIVATE-TOKEN header = %q, want %q", gotPrivateToken, "gl-test-token-456")
	}
	// Must NOT send Authorization header for GitLab.
	if gotAuth != "" {
		t.Errorf("GitLab should not send Authorization header, got %q", gotAuth)
	}
}

// TestAuthStyleConstants verifies the auth style constants exist.
func TestAuthStyleConstants(t *testing.T) {
	if AuthGitHub == AuthGitLab {
		t.Error("AuthGitHub and AuthGitLab must be different values")
	}
}

// TestHTTPClientSourceHasAuthStyle is a source code contract test verifying
// that NewHTTPClient accepts an authStyle parameter and Get uses it.
func TestHTTPClientSourceHasAuthStyle(t *testing.T) {
	src, err := os.ReadFile("httpclient.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// NewHTTPClient must accept an AuthStyle parameter.
	idx := strings.Index(code, "func NewHTTPClient(")
	if idx < 0 {
		t.Fatal("cannot find NewHTTPClient function")
	}
	sig := code[idx : idx+200]
	if !strings.Contains(sig, "AuthStyle") {
		t.Error("NewHTTPClient must accept an AuthStyle parameter to support platform-specific auth headers")
	}

	// Get must use the auth style (not hardcode "token").
	getIdx := strings.Index(code, "func (c *HTTPClient) Get(")
	if getIdx < 0 {
		t.Fatal("cannot find Get method")
	}
	getBody := code[getIdx:]
	if len(getBody) > 3000 {
		getBody = getBody[:3000]
	}
	if !strings.Contains(getBody, "authStyle") && !strings.Contains(getBody, "AuthStyle") {
		t.Error("Get method must use the client's auth style to set the correct header")
	}
}

// TestGitLabClientUsesGitLabAuth verifies the GitLab client constructs its
// HTTPClient with AuthGitLab style.
func TestGitLabClientUsesGitLabAuth(t *testing.T) {
	src, err := os.ReadFile("gitlab/client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "AuthGitLab") {
		t.Error("GitLab client.go must pass AuthGitLab to NewHTTPClient")
	}
}

// TestGitHubClientUsesGitHubAuth verifies the GitHub client constructs its
// HTTPClient with AuthGitHub style.
func TestGitHubClientUsesGitHubAuth(t *testing.T) {
	src, err := os.ReadFile("github/client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "AuthGitHub") {
		t.Error("GitHub client.go must pass AuthGitHub to NewHTTPClient")
	}
}
