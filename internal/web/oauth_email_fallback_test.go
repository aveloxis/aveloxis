package web

import (
	"os"
	"strings"
	"testing"
)

// TestGitHubCallbackFallsBackToUserEmails pins that handleGitHubCallback
// calls GitHub's /user/emails endpoint when /user returns an empty email
// field. /user only returns the email when the user has set it to
// publicly visible; users with verified-but-private email get an empty
// string. Since the OAuth scope already includes user:email
// (server.go:88), /user/emails returns the full verified-email list and
// we can pick the primary one. Operator-reported issue from new-user
// signup test on 2026-05-08: "their email was not captured".
func TestGitHubCallbackFallsBackToUserEmails(t *testing.T) {
	data, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	fnIdx := strings.Index(src, "func (s *Server) handleGitHubCallback(")
	if fnIdx < 0 {
		t.Fatal("cannot find handleGitHubCallback")
	}
	rest := src[fnIdx:]
	end := strings.Index(rest[1:], "\nfunc ")
	body := rest[:end+1]

	if !strings.Contains(body, "/user/emails") {
		t.Error("handleGitHubCallback must fall back to GitHub's " +
			"/user/emails endpoint when /user returns an empty email " +
			"field. Without this, users with verified-but-private email " +
			"end up with users.email = '' and the operator can't reach " +
			"them. The user:email OAuth scope is already requested.")
	}
}

// TestServerRegistersAccountEmailRoute pins the new fallback prompt
// route. When Option A (/user/emails) also returns nothing — rare but
// possible if the user has no verified email at all — the user is
// redirected to a form where they enter an email manually before
// reaching the dashboard. Two routes: GET form, POST handler.
func TestServerRegistersAccountEmailRoute(t *testing.T) {
	data, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	if !strings.Contains(src, `"/account/email"`) {
		t.Error("server.go must register the /account/email route. " +
			"This is the Option B fallback for new users whose OAuth " +
			"emails (both /user and /user/emails) returned nothing — " +
			"they get prompted to enter an email manually before " +
			"reaching the dashboard.")
	}
	if !strings.Contains(src, "handleAccountEmail") {
		t.Error("server.go must define handleAccountEmail (the GET/POST " +
			"handler for the email-collection form).")
	}
}

// TestDashboardEnforcesEmail pins that handleDashboard (or a middleware
// it depends on) redirects to /account/email when the session user has
// users.email = ''. Without this, the user reaches the dashboard with
// no email collected and the operator has no way to coordinate.
func TestDashboardEnforcesEmail(t *testing.T) {
	data, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Pin the existence of an email-gate helper called from the
	// dashboard path. Implementation flexible: a `requireEmail`
	// middleware OR an inline `GetUserEmail` check inside
	// handleDashboard. Either is fine; just need ONE of them present.
	if !strings.Contains(src, "GetUserEmail") && !strings.Contains(src, "requireEmail") {
		t.Error("server.go must gate dashboard access on users.email " +
			"being non-empty. Add either a requireEmail middleware OR " +
			"call store.GetUserEmail in handleDashboard and redirect " +
			"to /account/email when empty.")
	}
}

// TestStoreHasUserEmailMethods pins the DB-side methods for reading and
// writing the user's email field used by the email-collection form.
func TestStoreHasUserEmailMethods(t *testing.T) {
	data, err := os.ReadFile("../db/web_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "func (s *PostgresStore) GetUserEmail(") {
		t.Error("web_store.go must define GetUserEmail(ctx, userID) " +
			"(string, error) — used by the email-gate check on the " +
			"dashboard path.")
	}
	if !strings.Contains(src, "func (s *PostgresStore) UpdateUserEmail(") {
		t.Error("web_store.go must define UpdateUserEmail(ctx, userID, email) " +
			"error — used by the POST handler for the /account/email form.")
	}
}

// TestDashboardTemplateShowsPendingBanner pins the v0.19.0 follow-up:
// non-admin users whose groups are all pending see a banner explaining
// their account is awaiting administrator approval. Without this they
// see an empty dashboard indistinguishable from a fresh admin login.
func TestDashboardTemplateShowsPendingBanner(t *testing.T) {
	data, err := os.ReadFile("templates.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	dashIdx := strings.Index(src, `{{define "dashboard"}}`)
	if dashIdx < 0 {
		t.Fatal("cannot find dashboard template")
	}
	// Scan from the dashboard's define to the next top-level
	// `{{define `, since the dashboard template has many inner
	// `{{end}}` markers from nested {{if}} blocks.
	rest := src[dashIdx:]
	nextDef := strings.Index(rest[1:], `{{define "`)
	var body string
	if nextDef < 0 {
		body = rest
	} else {
		body = rest[:nextDef+1]
	}

	// Pin some recognizable text or check on group status. The exact
	// copy is up to the implementation but the dashboard must mention
	// "pending" or "approval" and check group status.
	lower := strings.ToLower(body)
	if !strings.Contains(lower, "pending") && !strings.Contains(lower, "approval") {
		t.Error("dashboard template must include a pending-approval " +
			"banner shown to non-admin users whose groups are all " +
			"status='pending'. Without it, the user sees no signal " +
			"their request is awaiting admin review.")
	}
}
