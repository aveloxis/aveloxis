// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestRejectedListingDoesNotCompleteTheScan pins worklist item 25: a 400 or
// 422 on a WHOLE-SOURCE listing (the root contents, the releases, the
// packages) is a definitive answer for the client, so the scanner counted
// it as an answer, stored the scan complete with those GitHub-sourced rows
// emptied, and stamped the 180-day cadence. A rejected listing says nothing
// about the repository's distributions: it is a non-answer for the scan
// (the snapshot is kept; the strike/backoff/sideline path applies). A
// rejected fetch of ONE manifest's content stays an answer for that file.
func TestRejectedListingDoesNotCompleteTheScan(t *testing.T) {
	depsServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"versions":[]}`)
	}))
	t.Cleanup(depsServer.Close)
	ecoServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `[]`)
	}))
	t.Cleanup(ecoServer.Close)
	for name, rejected := range map[string]string{
		"releases listing": "/repos/x/y/releases",
		"packages listing": "/users/x/packages",
		"root contents":    "/repos/x/y/contents",
	} {
		t.Run(name, func(t *testing.T) {
			gh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == rejected {
					w.WriteHeader(http.StatusUnprocessableEntity)
					_, _ = io.WriteString(w, `{"message":"Validation Failed"}`)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `[]`)
			}))
			t.Cleanup(gh.Close)
			scanner := buildTestScanner(t, depsServer.URL, ecoServer.URL, gh.URL, true)
			_, _, complete, err := scanner.Scan(context.Background(), 1, "x", "y", "https://github.com/x/y")
			if err == nil || complete {
				t.Errorf("a rejected %s: err=%v complete=%v; want the scan failed (snapshot kept), not complete with the rows emptied", name, err, complete)
			}
		})
	}
}
