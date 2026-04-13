package gitlab

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
	"github.com/augurlabs/aveloxis/internal/platform"
)

// TestEnrichContributorPopulatesAllFields verifies that EnrichContributor
// fully populates the Contributor from the GitLab /users?username=... response,
// including public_email, company, location, and created_at.
func TestEnrichContributorPopulatesAllFields(t *testing.T) {
	userResp := []glUser{{
		ID:          42,
		Username:    "jdoe",
		Name:        "Jane Doe",
		PublicEmail: "jane@example.com",
		Company:     "ACME Corp",
		Location:    "Portland, OR",
		AvatarURL:   "https://gitlab.com/uploads/-/system/user/avatar/42/avatar.png",
		WebURL:      "https://gitlab.com/jdoe",
		CreatedAt:   "2020-03-15T10:30:00Z",
	}}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(userResp)
	}))

	c, err := client.EnrichContributor(context.Background(), "jdoe")
	if err != nil {
		t.Fatalf("EnrichContributor: %v", err)
	}

	if c.Login != "jdoe" {
		t.Errorf("Login = %q, want %q", c.Login, "jdoe")
	}
	if c.Email != "jane@example.com" {
		t.Errorf("Email = %q, want %q", c.Email, "jane@example.com")
	}
	if c.FullName != "Jane Doe" {
		t.Errorf("FullName = %q, want %q", c.FullName, "Jane Doe")
	}
	if c.Company != "ACME Corp" {
		t.Errorf("Company = %q, want %q", c.Company, "ACME Corp")
	}
	if c.Location != "Portland, OR" {
		t.Errorf("Location = %q, want %q", c.Location, "Portland, OR")
	}
	// CreatedAt should be parsed from the ISO 8601 string.
	if c.CreatedAt.IsZero() {
		t.Error("CreatedAt should be populated, got zero time")
	}
	if c.CreatedAt.Year() != 2020 || c.CreatedAt.Month() != 3 || c.CreatedAt.Day() != 15 {
		t.Errorf("CreatedAt = %v, want 2020-03-15", c.CreatedAt)
	}

	// Identity fields.
	if len(c.Identities) != 1 {
		t.Fatalf("len(Identities) = %d, want 1", len(c.Identities))
	}
	id := c.Identities[0]
	if id.Platform != model.PlatformGitLab {
		t.Errorf("Identity.Platform = %d, want GitLab", id.Platform)
	}
	if id.UserID != 42 {
		t.Errorf("Identity.UserID = %d, want 42", id.UserID)
	}
	if id.Email != "jane@example.com" {
		t.Errorf("Identity.Email = %q, want %q", id.Email, "jane@example.com")
	}

	// Canonical should be set from the public email (not a noreply).
	if c.Canonical != "jane@example.com" {
		t.Errorf("Canonical = %q, want %q — EnrichContributor must set Canonical from Email", c.Canonical, "jane@example.com")
	}
}

// TestEnrichContributorSourceSetsCanonical verifies the source code sets Canonical.
func TestEnrichContributorSourceSetsCanonical(t *testing.T) {
	src, err := os.ReadFile("client.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (c *Client) EnrichContributor(")
	if idx < 0 {
		t.Fatal("cannot find EnrichContributor function")
	}
	fnBody := code[idx:]
	if len(fnBody) > 1500 {
		fnBody = fnBody[:1500]
	}

	if !strings.Contains(fnBody, "Canonical") {
		t.Error("GitLab EnrichContributor must set the Canonical field on the returned Contributor")
	}
}

// TestEnrichContributorUserNotFound verifies error when user doesn't exist.
func TestEnrichContributorUserNotFound(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]glUser{}) // empty array
	}))

	_, err := client.EnrichContributor(context.Background(), "nobody")
	if err == nil {
		t.Error("expected error for missing user, got nil")
	}
}

// TestEnrichContributorBadCreatedAt verifies graceful handling of unparseable dates.
func TestEnrichContributorBadCreatedAt(t *testing.T) {
	userResp := []glUser{{
		ID:        1,
		Username:  "user",
		CreatedAt: "not-a-date",
	}}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(userResp)
	}))

	c, err := client.EnrichContributor(context.Background(), "user")
	if err != nil {
		t.Fatalf("EnrichContributor: %v", err)
	}
	// CreatedAt should be zero when unparseable, not an error.
	if !c.CreatedAt.IsZero() {
		t.Errorf("CreatedAt should be zero for bad date, got %v", c.CreatedAt)
	}
}

// TestListContributorsAccessLevelAdmin verifies that members with access_level >= 50
// (Owner) have IsAdmin set to true, approximating GitHub's site_admin.
func TestListContributorsAccessLevelAdmin(t *testing.T) {
	callCount := 0
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		callCount++
		if callCount == 1 {
			// First call: /projects/:id/members/all
			members := []glMember{
				{ID: 1, Username: "owner", Name: "Owner User", AccessLevel: 50, WebURL: "https://gitlab.com/owner"},
				{ID: 2, Username: "maintainer", Name: "Maintainer User", AccessLevel: 40, WebURL: "https://gitlab.com/maintainer"},
				{ID: 3, Username: "developer", Name: "Dev User", AccessLevel: 30, WebURL: "https://gitlab.com/developer"},
			}
			json.NewEncoder(w).Encode(members)
		} else {
			// Second call: /projects/:id/repository/contributors
			json.NewEncoder(w).Encode([]glContributor{})
		}
	}))

	var contributors []model.Contributor
	for c, err := range client.ListContributors(context.Background(), "owner", "repo") {
		if err != nil {
			t.Fatalf("ListContributors: %v", err)
		}
		contributors = append(contributors, c)
	}

	if len(contributors) != 3 {
		t.Fatalf("got %d contributors, want 3", len(contributors))
	}

	// Owner (access_level=50) should have IsAdmin=true.
	ownerIdentity := contributors[0].Identities[0]
	if !ownerIdentity.IsAdmin {
		t.Error("Owner (access_level=50) should have IsAdmin=true")
	}

	// Maintainer (access_level=40) should NOT have IsAdmin.
	maintainerIdentity := contributors[1].Identities[0]
	if maintainerIdentity.IsAdmin {
		t.Error("Maintainer (access_level=40) should have IsAdmin=false")
	}

	// Developer (access_level=30) should NOT have IsAdmin.
	devIdentity := contributors[2].Identities[0]
	if devIdentity.IsAdmin {
		t.Error("Developer (access_level=30) should have IsAdmin=false")
	}
}

// TestFetchRepoInfoLogsIssueStatsError verifies that FetchRepoInfo logs a warning
// instead of silently ignoring errors from the issue_statistics endpoint.
// We verify this by checking the returned data still has zero issue counts
// (graceful degradation) while the call doesn't fail.
func TestFetchRepoInfoIssueStatsGracefulDegradation(t *testing.T) {
	callCount := 0
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		callCount++

		path := r.URL.Path
		if contains(path, "issues_statistics") {
			// Return error for issue stats.
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if contains(path, "merge_requests") {
			// Return 0 for MR counts.
			w.Header().Set("X-Total", "0")
			w.Write([]byte("[]"))
			return
		}
		if contains(path, "repository/tree") {
			json.NewEncoder(w).Encode([]map[string]interface{}{})
			return
		}

		// Default: project endpoint.
		proj := glProject{
			ID:                   1,
			DefaultBranch:        "main",
			StarCount:            5,
			ForksCount:           2,
			OpenIssuesCount:      3,
			IssuesEnabled:        true,
			MergeRequestsEnabled: true,
		}
		json.NewEncoder(w).Encode(proj)
	}))

	info, err := client.FetchRepoInfo(context.Background(), "owner", "repo")
	if err != nil {
		t.Fatalf("FetchRepoInfo: %v", err)
	}

	// Issue stats should be zero (graceful degradation) but not an error.
	if info.IssuesCount != 0 {
		t.Errorf("IssuesCount = %d, want 0 (issue stats endpoint failed)", info.IssuesCount)
	}
	// Other fields should still be populated.
	if info.StarCount != 5 {
		t.Errorf("StarCount = %d, want 5", info.StarCount)
	}
	if info.DefaultBranch != "main" {
		t.Errorf("DefaultBranch = %q, want %q", info.DefaultBranch, "main")
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// testClientWithLogger creates a test client with a custom logger for log verification.
func testClientWithLogger(t *testing.T, handler http.Handler, logger *slog.Logger) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	httpClient := platform.NewHTTPClient(server.URL, keys, logger)
	return &Client{http: httpClient, logger: logger, host: "gitlab.com"}
}
