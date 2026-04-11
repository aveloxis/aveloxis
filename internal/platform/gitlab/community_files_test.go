package gitlab

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

func testClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	logger := slog.Default()
	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	httpClient := platform.NewHTTPClient(server.URL, keys, logger)
	return &Client{http: httpClient, logger: logger, host: "gitlab.com"}
}

// TestFetchCommunityFilePresence verifies that the GitLab client correctly
// detects community profile files (CHANGELOG, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY)
// by querying the /repository/tree endpoint.
func TestFetchCommunityFilePresence(t *testing.T) {
	treeResponse := []map[string]interface{}{
		{"name": "README.md", "type": "blob", "path": "README.md"},
		{"name": "CONTRIBUTING.md", "type": "blob", "path": "CONTRIBUTING.md"},
		{"name": "CHANGELOG.md", "type": "blob", "path": "CHANGELOG.md"},
		{"name": "CODE_OF_CONDUCT.md", "type": "blob", "path": "CODE_OF_CONDUCT.md"},
		{"name": "SECURITY.md", "type": "blob", "path": "SECURITY.md"},
		{"name": "src", "type": "tree", "path": "src"},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(treeResponse)
	}))

	files := client.fetchCommunityFiles(context.Background(), "owner%2Frepo")

	if files.Changelog != "present" {
		t.Errorf("Changelog = %q, want %q", files.Changelog, "present")
	}
	if files.Contributing != "present" {
		t.Errorf("Contributing = %q, want %q", files.Contributing, "present")
	}
	if files.CodeOfConduct != "present" {
		t.Errorf("CodeOfConduct = %q, want %q", files.CodeOfConduct, "present")
	}
	if files.Security != "present" {
		t.Errorf("Security = %q, want %q", files.Security, "present")
	}
}

// TestFetchCommunityFileAbsence verifies that missing files return empty strings.
func TestFetchCommunityFileAbsence(t *testing.T) {
	treeResponse := []map[string]interface{}{
		{"name": "README.md", "type": "blob", "path": "README.md"},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(treeResponse)
	}))

	files := client.fetchCommunityFiles(context.Background(), "owner%2Frepo")

	if files.Changelog != "" {
		t.Errorf("Changelog = %q, want empty", files.Changelog)
	}
	if files.Contributing != "" {
		t.Errorf("Contributing = %q, want empty", files.Contributing)
	}
	if files.CodeOfConduct != "" {
		t.Errorf("CodeOfConduct = %q, want empty", files.CodeOfConduct)
	}
	if files.Security != "" {
		t.Errorf("Security = %q, want empty", files.Security)
	}
}

// TestFetchCommunityFileAPIError verifies graceful degradation when the API fails.
func TestFetchCommunityFileAPIError(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Use 404 (non-retryable) to avoid exponential backoff delays in tests.
		w.WriteHeader(http.StatusNotFound)
	}))

	files := client.fetchCommunityFiles(context.Background(), "owner%2Frepo")

	// All should be empty on API failure — graceful degradation.
	if files.Changelog != "" || files.Contributing != "" || files.CodeOfConduct != "" || files.Security != "" {
		t.Error("API error should return empty strings, not panic")
	}
}

// TestFetchCommunityFileCaseInsensitive verifies that file detection is
// case-insensitive (e.g., "contributing.md" matches too).
func TestFetchCommunityFileCaseInsensitive(t *testing.T) {
	treeResponse := []map[string]interface{}{
		{"name": "contributing.md", "type": "blob", "path": "contributing.md"},
		{"name": "Changelog.md", "type": "blob", "path": "Changelog.md"},
		{"name": "security.md", "type": "blob", "path": "security.md"},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(treeResponse)
	}))

	files := client.fetchCommunityFiles(context.Background(), "owner%2Frepo")

	if files.Contributing != "present" {
		t.Errorf("contributing.md (lowercase) not detected")
	}
	if files.Changelog != "present" {
		t.Errorf("Changelog.md (mixed case) not detected")
	}
	if files.Security != "present" {
		t.Errorf("security.md (lowercase) not detected")
	}
}

// TestFetchCommunityFileVariants verifies detection of common filename variants
// (e.g., CHANGES.md, CONTRIBUTING.rst).
func TestFetchCommunityFileVariants(t *testing.T) {
	treeResponse := []map[string]interface{}{
		{"name": "CHANGES.md", "type": "blob", "path": "CHANGES.md"},
		{"name": "CONTRIBUTING.rst", "type": "blob", "path": "CONTRIBUTING.rst"},
	}

	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(treeResponse)
	}))

	files := client.fetchCommunityFiles(context.Background(), "owner%2Frepo")

	if files.Changelog != "present" {
		t.Errorf("CHANGES.md variant not detected as changelog")
	}
	if files.Contributing != "present" {
		t.Errorf("CONTRIBUTING.rst variant not detected")
	}
}

// TestFetchCommunityFileEmptyRepo verifies that an empty repo (no files) returns
// all empty strings.
func TestFetchCommunityFileEmptyRepo(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode([]map[string]interface{}{})
	}))

	files := client.fetchCommunityFiles(context.Background(), "owner%2Frepo")

	if files.Changelog != "" || files.Contributing != "" || files.CodeOfConduct != "" || files.Security != "" {
		t.Error("empty repo should return all empty strings")
	}
}
