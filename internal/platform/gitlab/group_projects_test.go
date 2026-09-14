// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package gitlab

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"testing"
)

// v0.30.0: the group listing the scheduler's legacy GitLab group refresh
// runs lives on the instance's client, so it uses that instance's API URL
// and keys — the scheduler no longer builds a keyed HTTP client of its own.
func TestListGroupProjectsPaginatesWithTheInstanceKey(t *testing.T) {
	var tokens []string
	client := instanceClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tokens = append(tokens, r.Header.Get("PRIVATE-TOKEN"))
		if r.URL.Path != "/api/v4/groups/my group/projects" && r.URL.EscapedPath() != "/api/v4/groups/my%20group/projects" {
			t.Errorf("path = %q, want the escaped group projects listing", r.URL.EscapedPath())
		}
		if r.URL.Query().Get("include_subgroups") != "true" {
			t.Error("the listing must include subgroups")
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case 1:
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 11, "web_url": "https://gitlab.example.invalid/my-group/a", "name": "a", "namespace": map[string]any{"full_path": "my-group"}},
				{"id": 12, "web_url": "https://gitlab.example.invalid/my-group/sub/b", "name": "b", "namespace": map[string]any{"full_path": "my-group/sub"}},
			})
		default:
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		}
	}), "https://gitlab.example.invalid")

	var got []GroupProject
	for p, err := range client.ListGroupProjects(context.Background(), "my group") {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, p)
	}
	if len(got) != 2 || got[1].WebURL != "https://gitlab.example.invalid/my-group/sub/b" || got[1].Owner != "my-group/sub" || got[1].Name != "b" || got[1].ForgeID != "12" {
		t.Errorf("ListGroupProjects = %+v", got)
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
	client := instanceClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusNotFound)
	}), "https://gitlab.example.invalid")
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
