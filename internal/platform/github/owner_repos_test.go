// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func listingClient(t *testing.T, h http.Handler) *Client {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
}

// Stage 1 of summary/59 (eval-3 target 1): one lister per forge. The
// organization listing is paged until empty, the owner is escaped, and
// every item carries the forge's numeric id and creation time.
func TestListOwnerReposPaginatesAnOrganization(t *testing.T) {
	var paths []string
	client := listingClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.EscapedPath()+"?page="+r.URL.Query().Get("page"))
		if r.URL.Query().Get("type") != "all" {
			t.Error("the listing must ask for every repository type (forks included)")
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		w.Header().Set("Content-Type", "application/json")
		switch page {
		case 1:
			_ = json.NewEncoder(w).Encode([]map[string]any{
				{"id": 7, "html_url": "https://github.com/my org/a", "name": "a", "created_at": "2024-05-06T07:08:09Z", "owner": map[string]any{"login": "my org"}},
				// A member token lists the organization's PRIVATE repositories
				// too (type=all); the scan must know (0.29.140: an unauthenticated
				// probe 404s them, so a private listing is no sign of life).
				{"id": 8, "html_url": "https://github.com/my org/p", "name": "p", "private": true, "owner": map[string]any{"login": "my org"}},
			})
		default:
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		}
	}))
	var got []platform.OwnerRepo
	for it, err := range client.ListOwnerRepos(context.Background(), "my org", true) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, it)
	}
	if len(got) != 2 || got[0].ForgeID != "7" || got[0].Owner != "my org" || got[0].Name != "a" || got[0].CreatedAt.Year() != 2024 {
		t.Errorf("ListOwnerRepos = %+v", got)
	}
	if got[0].Private || !got[1].Private {
		t.Errorf("Private must follow the listing's `private` field: %+v", got)
	}
	if len(paths) != 2 || paths[0] != "/orgs/my%20org/repos?page=1" || paths[1] != "/orgs/my%20org/repos?page=2" {
		t.Errorf("paths = %v; want the escaped organization listing, two pages", paths)
	}
}

// N16: the personal-account fallback runs only on a definitive 404 of the
// FIRST organization page; a 404 on a later page, a transport failure or a
// rate-limit refusal is the listing's error, yielded once. Without the
// fallback a 404 is an error too (the legacy group refresh and the CLI).
func TestListOwnerReposFallsBackToAUserOnlyOnAFirstPageNotFound(t *testing.T) {
	personal := func(status map[string]int) (paths []string, items int, err error) {
		client := listingClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.URL.Path + "?page=" + r.URL.Query().Get("page")
			paths = append(paths, key)
			if code, refused := status[key]; refused {
				http.Error(w, "no", code)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			if r.URL.Query().Get("page") == "1" {
				_ = json.NewEncoder(w).Encode([]map[string]any{{"id": 1, "html_url": "u", "name": "n", "owner": map[string]any{"login": "o"}}})
				return
			}
			_ = json.NewEncoder(w).Encode([]map[string]any{})
		}))
		for _, e := range client.ListOwnerRepos(context.Background(), "o", true) {
			if e != nil {
				err = e
				continue
			}
			items++
		}
		return paths, items, err
	}
	paths, items, err := personal(map[string]int{"/orgs/o/repos?page=1": 404})
	if err != nil || items != 1 || len(paths) != 3 || paths[1] != "/users/o/repos?page=1" {
		t.Errorf("first-page 404: paths %v, items %d, err %v; want the user listing", paths, items, err)
	}
	paths, items, err = personal(map[string]int{"/orgs/o/repos?page=2": 404})
	if !errors.Is(err, platform.ErrNotFound) || items != 1 || len(paths) != 2 {
		t.Errorf("second-page 404: paths %v, items %d, err %v; want the error, no fallback", paths, items, err)
	}
	// A 422 is refused without the client's retry backoff (a 5xx would be
	// retried for minutes); it stands for every failure that is not a 404.
	paths, _, err = personal(map[string]int{"/orgs/o/repos?page=1": 422})
	if err == nil || errors.Is(err, platform.ErrNotFound) || len(paths) < 1 || paths[len(paths)-1] != "/orgs/o/repos?page=1" {
		t.Errorf("a 422: paths %v, err %v; want the error, no fallback", paths, err)
	}
	// No fallback asked for: the 404 is the answer.
	client := listingClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", 404) }))
	n := 0
	for _, e := range client.ListOwnerRepos(context.Background(), "o", false) {
		if !errors.Is(e, platform.ErrNotFound) {
			t.Errorf("err = %v", e)
		}
		n++
	}
	if n != 1 {
		t.Errorf("yielded %d times; want the error once", n)
	}
}
