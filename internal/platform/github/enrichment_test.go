package github

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// testGHClient creates a GitHub client pointed at a test HTTP server.
func testGHClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	logger := slog.Default()
	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	httpClient := platform.NewHTTPClient(server.URL, keys, logger, platform.AuthGitHub)
	return &Client{http: httpClient, logger: logger}
}

// TestEnrichContributorSetsCanonicalFromEmail verifies that EnrichContributor
// populates the Canonical field from the user's public email when the email
// is not a noreply address. This eliminates duplicate GET /users/{login} calls
// from ResolveEmailsToCanonical.
func TestEnrichContributorSetsCanonicalFromEmail(t *testing.T) {
	userResp := ghUser{
		ID:        12345,
		Login:     "octocat",
		Name:      "The Octocat",
		Email:     "octocat@example.com",
		Company:   "GitHub",
		Location:  "San Francisco",
		AvatarURL: "https://avatars.githubusercontent.com/u/12345",
		HTMLURL:   "https://github.com/octocat",
		NodeID:    "MDQ6VXNlcjEyMzQ1",
		Type:      "User",
		CreatedAt: "2020-01-15T10:30:00Z",
	}

	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(userResp)
	}))

	c, err := client.EnrichContributor(context.Background(), "octocat")
	if err != nil {
		t.Fatalf("EnrichContributor: %v", err)
	}

	// Canonical should be set from the public email.
	if c.Canonical != "octocat@example.com" {
		t.Errorf("Canonical = %q, want %q", c.Canonical, "octocat@example.com")
	}
	// Email should also be set.
	if c.Email != "octocat@example.com" {
		t.Errorf("Email = %q, want %q", c.Email, "octocat@example.com")
	}
}

// TestEnrichContributorSkipsNoreplyForCanonical verifies that noreply emails
// are NOT used as the canonical email.
func TestEnrichContributorSkipsNoreplyForCanonical(t *testing.T) {
	userResp := ghUser{
		ID:    99,
		Login: "private-user",
		Email: "12345+private-user@users.noreply.github.com",
	}

	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(userResp)
	}))

	c, err := client.EnrichContributor(context.Background(), "private-user")
	if err != nil {
		t.Fatalf("EnrichContributor: %v", err)
	}

	// Canonical should be empty because the email is a noreply address.
	if c.Canonical != "" {
		t.Errorf("Canonical = %q, want empty for noreply email", c.Canonical)
	}
	// Email should still be set (it's a valid email, just not canonical).
	if c.Email != "12345+private-user@users.noreply.github.com" {
		t.Errorf("Email = %q, want noreply address", c.Email)
	}
}

// TestEnrichContributorEmptyEmailNoCanonical verifies that an empty email
// results in an empty canonical.
func TestEnrichContributorEmptyEmailNoCanonical(t *testing.T) {
	userResp := ghUser{
		ID:    88,
		Login: "no-email-user",
	}

	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(userResp)
	}))

	c, err := client.EnrichContributor(context.Background(), "no-email-user")
	if err != nil {
		t.Fatalf("EnrichContributor: %v", err)
	}

	if c.Canonical != "" {
		t.Errorf("Canonical = %q, want empty for user with no email", c.Canonical)
	}
}

// TestEnrichContributorSourceSetsCanonical is a source code contract test
// verifying that EnrichContributor sets the Canonical field.
func TestEnrichContributorSourceSetsCanonical(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the EnrichContributor function body.
	idx := strings.Index(code, "func (c *Client) EnrichContributor(")
	if idx < 0 {
		t.Fatal("cannot find EnrichContributor function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	// Must set Canonical on the returned Contributor.
	if !strings.Contains(fnBody, "Canonical") {
		t.Error("EnrichContributor must set the Canonical field on the returned Contributor " +
			"to eliminate duplicate GET /users/{login} calls from ResolveEmailsToCanonical")
	}

	// Must filter noreply emails from being used as canonical.
	if !strings.Contains(fnBody, "noreply") {
		t.Error("EnrichContributor must filter noreply emails before setting Canonical")
	}
}
