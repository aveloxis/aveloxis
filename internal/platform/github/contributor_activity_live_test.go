// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Live-API canary for the v0.27.57 contributor-activity fetcher —
// added 2026-08-02 while diagnosing a production state where 216,000
// contributors were stamped gh_activity_checked_at with ZERO
// classified rows (every single one fell into the "absent from
// result" mark-only path). The httptest suite passed throughout; this
// canary exercises the REAL GraphQL response shape, per the
// test-against-real-APIs lesson (mock-only suites miss response-shape
// drift).

package github

import (
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// TestFetchContributorActivityFailsOnResourceLimits reproduces the
// 2026-07-30/31 production incident offline: GitHub answers an
// oversized contributionsCollection batch with per-path
// RESOURCE_LIMITS_EXCEEDED on EVERY alias ("Resource limits for this
// query exceeded" — a whole-query condition reported per-path) and
// null data nodes. Pre-fix, the per-path tolerance turned that into
// an empty-but-successful result; the scheduler then marked all 2,500
// claimed contributors "checked, absent" — 216,000 contributors were
// durably stamped with zero classifications before anyone noticed.
// The fetch must FAIL so nothing gets marked.
func TestFetchContributorActivityFailsOnResourceLimits(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mux := http.NewServeMux()
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"u0":null,"u1":null},"errors":[
			{"type":"RESOURCE_LIMITS_EXCEEDED","path":["u0","contributionsCollection"],"message":"Resource limits for this query exceeded."},
			{"type":"RESOURCE_LIMITS_EXCEEDED","path":["u1","contributionsCollection","contributionYears"],"message":"Resource limits for this query exceeded."}
		]}`)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	client := New(srv.URL, platform.NewKeyPool([]string{"t"}, logger), logger)
	_, err := client.FetchContributorActivity(t.Context(), []string{"alice", "bob"})
	if err == nil {
		t.Fatal("RESOURCE_LIMITS_EXCEEDED on every alias must FAIL the fetch — " +
			"an empty-but-successful result gets every claimed contributor " +
			"durably marked 'checked, no data' (the 216K-row production incident)")
	}
	if platform.ClassifyError(err) != platform.ClassTransient {
		t.Errorf("resource-limit failures classify as ClassTransient (subdividable), got %v for %v",
			platform.ClassifyError(err), err)
	}
}

func TestLiveFetchContributorActivity(t *testing.T) {
	if os.Getenv("AVELOXIS_TEST_NETWORK") != "1" {
		t.Skip("network canary: set AVELOXIS_TEST_NETWORK=1 to run")
	}
	tok := os.Getenv("AVELOXIS_TEST_GITHUB_TOKEN")
	if tok == "" {
		t.Skip("set AVELOXIS_TEST_GITHUB_TOKEN to run the activity canary")
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := platform.NewKeyPool([]string{tok}, logger)
	client := New("https://api.github.com", keys, logger)

	got, err := client.FetchContributorActivity(t.Context(),
		[]string{"torvalds", "sgoggins", "this-login-does-not-exist-avx"})
	if err != nil {
		t.Fatalf("FetchContributorActivity: %v", err)
	}
	// The two real accounts MUST be present — absence for a live
	// account is exactly the production failure this canary guards
	// (every contributor absent → mark-only → zero classifications).
	for _, login := range []string{"torvalds", "sgoggins"} {
		act, ok := got[login]
		if !ok {
			t.Fatalf("%s absent from result — the production zero-classification bug shape", login)
		}
		if len(act.ContributionYears) == 0 {
			t.Errorf("%s has no contributionYears — decode drift", login)
		}
		if act.LastContributionYear() < 2020 {
			t.Errorf("%s LastContributionYear = %d — implausibly old", login, act.LastContributionYear())
		}
	}
	if _, ok := got["this-login-does-not-exist-avx"]; ok {
		t.Error("nonexistent login must be absent from the result map")
	}
}
