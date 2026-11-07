// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// listingClient is a client on an httptest server with one key.
func listingClient(t *testing.T, h http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(srv.URL+"/api/v4", platform.NewKeyPool([]string{"instance-token"}, logger), logger)
}

// Stage 1 of summary/59 (eval-3 target 1; v0.30.0's listing ported): the
// group listing the scheduler's passes run lives on the client, so it uses
// the instance's API URL and keys, escapes the group path, includes
// subgroups, stops at the empty page, and carries the forge's numeric id
// and creation time.
func TestListGroupProjectsPaginatesWithTheInstanceKey(t *testing.T) {
	var tokens []string
	created := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	client := listingClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens = append(tokens, r.Header.Get("PRIVATE-TOKEN"))
		if r.URL.EscapedPath() != "/api/v4/groups/my%20group/projects" {
			t.Errorf("path = %q, want the escaped group projects listing", r.URL.EscapedPath())
		}
		if r.URL.Query().Get("include_subgroups") != "true" {
			t.Error("the listing must include subgroups")
		}
		if r.URL.Query().Get("with_shared") != "false" {
			t.Error("the listing must leave out projects shared into the group (GitHub lists an organization's own repositories; parity)")
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case 1:
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 11, "web_url": "https://gitlab.example.invalid/my-group/a", "name": "a", "path": "a", "path_with_namespace": "my-group/a", "created_at": created, "visibility": "public", "namespace": map[string]any{"full_path": "my-group"}},
				// GitLab's name is the DISPLAY name; the path is the slug every API call addresses (0.29.106 whole-branch review P1).
				// An `internal` project is not public to an unauthenticated probe (0.29.140).
				{"id": 12, "web_url": "https://gitlab.example.invalid/my-group/sub/b", "name": "B Display Name", "path": "b", "path_with_namespace": "my-group/sub/b", "visibility": "internal", "namespace": map[string]any{"full_path": "my-group/sub"}},
			})
		default:
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		}
	}))
	var got []GroupProject
	for p, err := range client.ListGroupProjects(context.Background(), "my group") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, p)
	}
	if len(got) != 2 || got[1].WebURL != "https://gitlab.example.invalid/my-group/sub/b" || got[1].Owner != "my-group/sub" || got[1].Name != "b" || got[1].ForgeID != "12" {
		t.Errorf("ListGroupProjects = %+v; want Name = the project's path (its slug), never its display name", got)
	}
	if !got[0].CreatedAt.Equal(created) || !got[1].CreatedAt.IsZero() {
		t.Errorf("created_at = %v, %v; want the forge's time and zero when absent", got[0].CreatedAt, got[1].CreatedAt)
	}
	if or := got[0].OwnerRepo(); or.URL != got[0].WebURL || or.Owner != "my-group" || or.ForgeID != "11" || !or.CreatedAt.Equal(created) || or.Private {
		t.Errorf("OwnerRepo() = %+v", or)
	}
	if or := got[1].OwnerRepo(); !or.Private {
		t.Errorf("a project whose visibility is not `public` is Private to the scan: %+v", or)
	}
	for _, tok := range tokens {
		if tok != "instance-token" {
			t.Errorf("PRIVATE-TOKEN = %q, want the instance's own key", tok)
		}
	}
	if len(tokens) != 2 {
		t.Errorf("made %d requests, want 2 (stop at the empty page)", len(tokens))
	}
}

func TestListGroupProjectsYieldsTheListingError(t *testing.T) {
	client := listingClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}))
	var errs int
	for _, err := range client.ListGroupProjects(context.Background(), "g") {
		if err != nil {
			errs++
		}
	}
	if errs != 1 {
		t.Errorf("got %d errors, want the listing error yielded once", errs)
	}
}
