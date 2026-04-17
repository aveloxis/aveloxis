package github

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/augurlabs/aveloxis/internal/platform"
)

// TestListReleases_404ReturnsErrNotFound verifies that when GitHub returns
// 404 for the releases endpoint (e.g., because the repo slug has been
// mis-stored with a ".git" suffix, or the repo was deleted), the iterator
// yields an error that callers can identify via errors.Is(err, platform.ErrNotFound).
func TestListReleases_404ReturnsErrNotFound(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`{"message":"Not Found"}`))
	}))

	var gotErr error
	count := 0
	for _, err := range client.ListReleases(context.Background(), "o", "r") {
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
		t.Errorf("expected zero releases before error, got %d", count)
	}
}

// TestListReleases_EmptyArraySucceeds verifies that an empty releases list
// (a normal state for repos that never cut a release) yields zero items with
// no error.
func TestListReleases_EmptyArraySucceeds(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`[]`))
	}))

	count := 0
	for _, err := range client.ListReleases(context.Background(), "o", "r") {
		if err != nil {
			t.Fatalf("unexpected error on empty releases: %v", err)
		}
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 releases, got %d", count)
	}
}

// TestListReleases_ReturnsReleases verifies that releases are correctly
// iterated when the endpoint returns a populated array.
func TestListReleases_ReturnsReleases(t *testing.T) {
	releases := []ghRelease{
		{ID: 1, Name: "v1.0.0", TagName: "v1.0.0"},
		{ID: 2, Name: "v2.0.0", TagName: "v2.0.0"},
	}
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(releases)
	}))

	count := 0
	for rel, err := range client.ListReleases(context.Background(), "o", "r") {
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
