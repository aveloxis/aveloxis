// Source-contract tests for the OAuth callback handlers' defensive
// validation. The motivating incident: GitHub's /user response on
// one production callback returned without a `login` field; the
// handler json.Unmarshal'd into a zero-valued struct, passed
// Login="" through to UpsertOAuthUser, and inserted a junk
// user row that shadowed the real account on every subsequent
// login. The fix is layered:
//
//   1. handleGitHubCallback / handleGitLabCallback check the HTTP
//      status of the user-info response.
//   2. They check json.Unmarshal returned without error.
//   3. They check the resulting Login/Username field is non-empty
//      BEFORE invoking UpsertOAuthUser.
//
// UpsertOAuthUser still rejects an empty Login as a last line of
// defense (covered in internal/db/web_store_oauth_test.go), but the
// handler-side checks let us produce a useful error page instead of
// a generic 500 from the database layer.

package web

import (
	"os"
	"strings"
	"testing"
)

const serverSourceFile = "server.go"

// TestGitHubCallbackChecksHTTPStatus pins that the GitHub callback
// rejects a non-200 response from /user. Without this, a transient
// 401/403/5xx on the user-info fetch silently produced a zero-value
// struct and a blank-login user row.
func TestGitHubCallbackChecksHTTPStatus(t *testing.T) {
	body := extractWebFunc(t, "handleGitHubCallback")
	if !strings.Contains(body, "resp.StatusCode") {
		t.Error("handleGitHubCallback must inspect resp.StatusCode from GitHub /user — a non-200 response can deliver an empty body that unmarshals to zero values")
	}
}

// TestGitHubCallbackChecksUnmarshalError pins that json.Unmarshal's
// error is checked. The pre-fix source threw away the error with a
// bare `json.Unmarshal(body, &ghUser)` — malformed JSON silently
// produced a zero-value struct.
func TestGitHubCallbackChecksUnmarshalError(t *testing.T) {
	body := extractWebFunc(t, "handleGitHubCallback")
	// Look for either an explicit Unmarshal error check or a Decoder
	// pattern. Both are acceptable; what's not acceptable is the
	// bare `json.Unmarshal(body, &ghUser)` discard pattern.
	if strings.Contains(body, "json.Unmarshal(body, &ghUser)\n") &&
		!strings.Contains(body, "json.Unmarshal(body, &ghUser); err") &&
		!strings.Contains(body, "json.Unmarshal(body, &ghUser); jerr") {
		// The unmarshal call exists with no error capture — confirm
		// SOMEWHERE the error is checked. The simplest check is for
		// "Unmarshal" combined with "err" in close proximity.
		t.Error("handleGitHubCallback must check the json.Unmarshal error — silently discarding it lets malformed JSON from GitHub /user produce a zero-value user struct")
	}
}

// TestGitHubCallbackRejectsEmptyLogin pins the central invariant:
// no UpsertOAuthUser call until ghUser.Login is verified non-empty.
// Source-contract check: the function body must contain a
// `ghUser.Login == ""` (or equivalent) test BEFORE the call to
// UpsertOAuthUser.
func TestGitHubCallbackRejectsEmptyLogin(t *testing.T) {
	body := extractWebFunc(t, "handleGitHubCallback")
	if body == "" {
		t.Fatal("could not locate handleGitHubCallback function body")
	}
	upsertIdx := strings.Index(body, "UpsertOAuthUser")
	if upsertIdx < 0 {
		t.Fatal("handleGitHubCallback must call UpsertOAuthUser somewhere in its body")
	}
	preUpsert := body[:upsertIdx]
	// Accept any of the common empty-string check patterns for the
	// Login field (== "", strings.TrimSpace, len(...)==0).
	hasCheck := strings.Contains(preUpsert, `ghUser.Login == ""`) ||
		strings.Contains(preUpsert, `ghUser.Login==""`) ||
		(strings.Contains(preUpsert, "ghUser.Login") && strings.Contains(preUpsert, "TrimSpace"))
	if !hasCheck {
		t.Error("handleGitHubCallback must verify ghUser.Login is non-empty BEFORE calling UpsertOAuthUser — an empty Login on /user response is the root cause of the blank-user incident")
	}
}

// TestGitLabCallbackChecksHTTPStatus mirrors the GitHub check for
// the GitLab /api/v4/user fetch. GitLab self-hosted instances behind
// a misbehaving proxy were observed returning 502 with empty bodies.
func TestGitLabCallbackChecksHTTPStatus(t *testing.T) {
	body := extractWebFunc(t, "handleGitLabCallback")
	if !strings.Contains(body, "resp.StatusCode") {
		t.Error("handleGitLabCallback must inspect resp.StatusCode from GitLab /api/v4/user")
	}
}

// TestGitLabCallbackChecksUnmarshalError mirrors the GitHub check.
func TestGitLabCallbackChecksUnmarshalError(t *testing.T) {
	body := extractWebFunc(t, "handleGitLabCallback")
	if strings.Contains(body, "json.Unmarshal(body, &glUser)\n") &&
		!strings.Contains(body, "json.Unmarshal(body, &glUser); err") {
		t.Error("handleGitLabCallback must check the json.Unmarshal error — silently discarding it lets malformed JSON from GitLab /api/v4/user produce a zero-value user struct")
	}
}

// TestGitLabCallbackRejectsEmptyUsername pins the same invariant
// for the GitLab path. GitLab's user object exposes Username (not
// Login).
func TestGitLabCallbackRejectsEmptyUsername(t *testing.T) {
	body := extractWebFunc(t, "handleGitLabCallback")
	if body == "" {
		t.Fatal("could not locate handleGitLabCallback function body")
	}
	upsertIdx := strings.Index(body, "UpsertOAuthUser")
	if upsertIdx < 0 {
		t.Fatal("handleGitLabCallback must call UpsertOAuthUser somewhere in its body")
	}
	preUpsert := body[:upsertIdx]
	hasCheck := strings.Contains(preUpsert, `glUser.Username == ""`) ||
		strings.Contains(preUpsert, `glUser.Username==""`) ||
		(strings.Contains(preUpsert, "glUser.Username") && strings.Contains(preUpsert, "TrimSpace"))
	if !hasCheck {
		t.Error("handleGitLabCallback must verify glUser.Username is non-empty BEFORE calling UpsertOAuthUser")
	}
}

// extractWebFunc reads server.go and returns the body of the named
// method on *Server. Boundary is the next "\nfunc " token.
func extractWebFunc(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(serverSourceFile)
	if err != nil {
		t.Fatalf("read %s: %v", serverSourceFile, err)
	}
	src := string(data)
	marker := "func (s *Server) " + name + "("
	idx := strings.Index(src, marker)
	if idx < 0 {
		return ""
	}
	rest := src[idx:]
	if end := strings.Index(rest[1:], "\nfunc "); end > 0 {
		return rest[:end+1]
	}
	return rest
}
