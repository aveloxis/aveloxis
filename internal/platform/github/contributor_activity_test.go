// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

// contributor_activity_test.go — TDD suite for the v0.27.57 batched
// contributionsCollection fetcher. Mirrors the FetchIssueClosers
// pattern: one aliased user() lookup per login, per-path NOT_FOUND
// (deleted/renamed users) degrades to absence from the result map, and
// batching keeps ~100 logins per HTTP round trip.

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

func TestFetchContributorActivityBatchesAndDecodes(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var queries int
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		queries++
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		for _, needle := range []string{"contributionsCollection", "restrictedContributionsCount", "contributionYears", "totalContributions"} {
			if !strings.Contains(req.Query, needle) {
				t.Errorf("query must select %s, got: %.200s", needle, req.Query)
			}
		}
		// u0: fully populated (the sgoggins probe shape, 2026-07-30);
		// u1: deleted/renamed → per-path NOT_FOUND, node null;
		// u2: brand-new account, zero everything.
		fmt.Fprint(w, `{"data":{
			"u0":{"login":"active-user","contributionsCollection":{"restrictedContributionsCount":1895,"contributionYears":[2026,2025,2010],"contributionCalendar":{"totalContributions":3637}}},
			"u1":null,
			"u2":{"login":"new-user","contributionsCollection":{"restrictedContributionsCount":0,"contributionYears":[],"contributionCalendar":{"totalContributions":0}}}
		},"errors":[{"type":"NOT_FOUND","path":["u1"],"message":"Could not resolve to a User with the login of 'gone-user'."}]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	got, err := client.FetchContributorActivity(t.Context(), []string{"active-user", "gone-user", "new-user"})
	if err != nil {
		t.Fatalf("FetchContributorActivity: %v", err)
	}
	if queries != 1 {
		t.Errorf("3 logins must fit in ONE batched query, got %d", queries)
	}
	a, ok := got["active-user"]
	if !ok {
		t.Fatal("active-user missing from result")
	}
	if a.CalendarTotal != 3637 || a.Restricted != 1895 || a.LastContributionYear() != 2026 {
		t.Errorf("active-user decoded wrong: %+v", a)
	}
	if a.PublicContributions() != 1742 {
		t.Errorf("active-user public = %d, want 1742", a.PublicContributions())
	}
	// Deleted user: absent (no data → report no data), NOT an error.
	if _, ok := got["gone-user"]; ok {
		t.Error("gone-user (per-path NOT_FOUND) must be absent from the result map")
	}
	// Zero-everything user still present — absence means "unknown",
	// presence-with-zeros means "confirmed quiet". The distinction is
	// what the activity classes are built on.
	z, ok := got["new-user"]
	if !ok {
		t.Fatal("new-user (zero contributions) must be PRESENT with zeros")
	}
	if z.CalendarTotal != 0 || z.Restricted != 0 || z.LastContributionYear() != 0 {
		t.Errorf("new-user decoded wrong: %+v", z)
	}
}

// More logins than one batch holds must split across multiple queries —
// GitHub aliased-query batches are capped well under the point budget,
// and a single oversized query would be rejected outright.
func TestFetchContributorActivitySplitsLargeBatches(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var queries int
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		queries++
		var req struct {
			Query string `json:"query"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		aliases := strings.Count(req.Query, ": user(login:")
		if aliases > contributorActivityBatchSize {
			t.Errorf("query carries %d aliases, cap is %d", aliases, contributorActivityBatchSize)
		}
		fmt.Fprint(w, `{"data":{}}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	logins := make([]string, contributorActivityBatchSize+1)
	for i := range logins {
		logins[i] = fmt.Sprintf("user%d", i)
	}
	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	if _, err := client.FetchContributorActivity(t.Context(), logins); err != nil {
		t.Fatalf("FetchContributorActivity: %v", err)
	}
	if queries != 2 {
		t.Errorf("batch-size+1 logins must take exactly 2 queries, got %d", queries)
	}
}
