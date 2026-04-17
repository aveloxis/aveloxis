package gitlab

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// TestListReleases_404ReturnsErrNotFound — GitLab returns 404 for releases
// on projects that don't exist or aren't accessible. The iterator must
// surface this via errors.Is(err, platform.ErrNotFound) so the collector
// can treat it as "no releases" rather than a fatal collection failure.
func TestListReleases_404ReturnsErrNotFound(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"404 Not Found"}`))
	}))

	var gotErr error
	count := 0
	for _, err := range client.ListReleases(context.Background(), "group", "project") {
		if err != nil {
			gotErr = err
			break
		}
		count++
	}
	if gotErr == nil {
		t.Fatal("expected error for 404 releases response")
	}
	if !errors.Is(gotErr, platform.ErrNotFound) {
		t.Errorf("expected errors.Is(err, platform.ErrNotFound), got %v", gotErr)
	}
	if count != 0 {
		t.Errorf("expected zero releases, got %d", count)
	}
}

// TestListReleases_EmptyArraySucceeds — a GitLab project that has never cut
// a release returns an empty array; this must be treated as success.
func TestListReleases_EmptyArraySucceeds(t *testing.T) {
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))

	count := 0
	for _, err := range client.ListReleases(context.Background(), "group", "project") {
		if err != nil {
			t.Fatalf("unexpected error on empty releases: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 releases, got %d", count)
	}
}

// TestListReleases_ReturnsReleases — sanity: populated array yields entries.
func TestListReleases_ReturnsReleases(t *testing.T) {
	releases := []glRelease{
		{TagName: "v1.0", Name: "v1.0"},
		{TagName: "v2.0", Name: "v2.0"},
	}
	client := testClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(releases)
	}))

	count := 0
	for rel, err := range client.ListReleases(context.Background(), "group", "project") {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if rel.TagName == "" {
			t.Error("expected non-empty tag name")
		}
		count++
	}
	if count != 2 {
		t.Errorf("expected 2 releases, got %d", count)
	}
}
