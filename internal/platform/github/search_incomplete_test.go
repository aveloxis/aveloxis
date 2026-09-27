// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestSearchIncompleteResultsIsNotANoHit pins worklist item 16 (the key-pool
// v0.29.55 follow-ups): a search that timed out on GitHub's side answers
// `incomplete_results: true` with zero items — that is not "no such user"
// (SR-16), and callers stamp a no-hit (the search-resolve and sender-resolve
// cooldowns). Zero items with incomplete results is an error that is not a
// definitive answer; a hit is a hit even when the results were incomplete.
func TestSearchIncompleteResultsIsNotANoHit(t *testing.T) {
	calls := map[string]func(*Client) (string, int64, error){
		"SearchUserByEmail": func(c *Client) (string, int64, error) {
			return c.SearchUserByEmail(context.Background(), "someone@example.com")
		},
		"SearchCommitByAuthorEmail": func(c *Client) (string, int64, error) {
			return c.SearchCommitByAuthorEmail(context.Background(), "someone@example.com")
		},
	}
	timedOut := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":0,"incomplete_results":true,"items":[]}`))
	})
	for name, call := range calls {
		t.Run(name+"/timed out, no items", func(t *testing.T) {
			login, id, err := call(testGHClient(t, timedOut))
			if err == nil {
				t.Fatalf("got (%q, %d, nil) — an incomplete search with no items was reported as no hit", login, id)
			}
			if platform.IsDefinitiveAnswer(err) {
				t.Fatalf("err %v classifies as a definitive answer; callers would stamp it", err)
			}
		})
	}
	// A hit is a hit.
	hits := map[string]string{
		"SearchUserByEmail":         `{"total_count":1,"incomplete_results":true,"items":[{"login":"octocat","id":583231}]}`,
		"SearchCommitByAuthorEmail": `{"total_count":1,"incomplete_results":true,"items":[{"author":{"login":"octocat","id":583231}}]}`,
	}
	for name, body := range hits {
		t.Run(name+"/incomplete but a hit", func(t *testing.T) {
			b := body
			login, id, err := calls[name](testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(b)) })))
			if err != nil || login != "octocat" || id != 583231 {
				t.Fatalf("got (%q, %d, %v); want the hit (octocat, 583231, nil)", login, id, err)
			}
		})
	}
	// A complete search with no items is still the no-hit.
	complete := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"total_count":0,"incomplete_results":false,"items":[]}`))
	})
	for name, call := range calls {
		t.Run(name+"/complete, no items", func(t *testing.T) {
			if login, id, err := call(testGHClient(t, complete)); err != nil || login != "" || id != 0 {
				t.Fatalf("got (%q, %d, %v); want the no-hit (\"\", 0, nil)", login, id, err)
			}
		})
	}
}
