// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"sync"
	"testing"
)

// TestListReleasesRequestsTheMeasuredPageSize — 2026-09-23 log review:
// /releases?per_page=100 504ed deterministically on release-heavy
// repositories (canonical/charmcraftcache-hub, kairos-io/kairos; both jobs
// failed after 10 retries). Measured live: page 2 at per_page=100 took 9.4 s
// (charmcraftcache-hub) and 504ed at 11.2 s (kairos) against GitHub's
// ~10 s server limit; at per_page=30 the same pages returned in 4.1 s and
// 2.8 s. Releases page at releasesPerPage; the paginator keeps it on every
// following page.
func TestListReleasesRequestsTheMeasuredPageSize(t *testing.T) {
	var mu sync.Mutex
	var queries []string
	c := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		queries = append(queries, r.URL.Query().Get("per_page"))
		mu.Unlock()
		if r.URL.Query().Get("page") == "" {
			// Echo the request's own per_page, as GitHub does, so the second
			// request shows what the client sent (PR #212 review: a hard-coded
			// 30 made the page-2 check unable to fail).
			w.Header().Set("Link", `<`+r.URL.Path+`?per_page=`+r.URL.Query().Get("per_page")+`&page=2>; rel="next"`)
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	for _, err := range c.ListReleases(context.Background(), "o", "r") {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(queries) == 0 {
		t.Fatal("no request made")
	}
	for i, q := range queries {
		if q != "30" {
			t.Errorf("request %d asked per_page=%q, want 30", i+1, q)
		}
	}
}
