// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostguard"
	"github.com/aveloxis/aveloxis/internal/manifest"
)

// One analysis waits on the repositories its build files declare for a
// bounded time in all (review round 1, security): the hosts and the number
// of coordinates are the collected repository's choice.
func TestAnAnalysisWaitsOnDeclaredRepositoriesForABoundedTime(t *testing.T) {
	if want := time.Duration(registryBreakerThreshold) * registryHTTPClient.Timeout; declaredFetchBudget != want {
		t.Errorf("declaredFetchBudget = %s, want the breaker's threshold of request timeouts (%s)", declaredFetchBudget, want)
	}
	_, src := pomSourceFixture(t, "declared-budget")
	withMavenRepositories(t)
	withDeclaredFetch(t, true)
	old := declaredFetchBudget
	declaredFetchBudget = 40 * time.Millisecond
	t.Cleanup(func() { declaredFetchBudget = old })

	var asked atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		time.Sleep(60 * time.Millisecond)
		http.Error(w, "busy", http.StatusBadGateway)
	}))
	defer slow.Close()
	// The first request is made (nothing is spent yet) and uses the budget.
	if _, outcome := src.RemotePom("org.remote", "one", "1.0", []string{slow.URL}); outcome != manifest.RemoteFailed {
		t.Errorf("the first coordinate: outcome %v, want failed (unknown)", outcome)
	}
	first := asked.Load()
	if first == 0 {
		t.Fatal("the declared repository was never asked: the test did not run")
	}
	// After it, no declared repository is asked, and what was not asked is
	// UNKNOWN, never "not found".
	for _, artifact := range []string{"two", "three", "four"} {
		if _, outcome := src.RemotePom("org.remote", artifact, "1.0", []string{slow.URL, slow.URL + "/other"}); outcome != manifest.RemoteFailed {
			t.Errorf("%s, past the budget: outcome %v, want failed (unknown)", artifact, outcome)
		}
	}
	if asked.Load() != first {
		t.Errorf("declared repositories were asked %d more times past the budget", asked.Load()-first)
	}
	// Six not made, and the first one cut off when the budget ran out
	// under it (0.29.209: a request ends with the budget).
	if src.budgetCut != 7 {
		t.Errorf("requests not made or cut short = %d, want 7", src.budgetCut)
	}
	// A CONFIGURED repository is not under it.
	central := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, central.srv.URL)
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteFound {
		t.Errorf("a configured repository past the declared budget: outcome %v, want found", outcome)
	}
}

// A coordinate becomes path segments of a URL on a host the build may have
// chosen. What is not a coordinate is not asked for.
func TestOnlyACoordinateIsAskedFor(t *testing.T) {
	_, src := pomSourceFixture(t, "coordinate-grammar")
	repo := newPomRepo(t, map[string]string{"org.remote:parent:5.0": remoteParentPom})
	withMavenRepositories(t, repo.srv.URL)
	for _, c := range [][3]string{
		{"org.remote", "parent", "5.0?x=1"},
		{"org.remote", "parent#frag", "5.0"},
		{"org.remote/../..", "parent", "5.0"},
		{"org.remote", "..", "5.0"},
		{"org.remote", "parent", ".."},
		{"org..remote", "parent", "5.0"},
		{"org.remote", "par ent", "5.0"},
		{"org.remote", "parent", "%2e%2e"},
		{"", "parent", "5.0"},
	} {
		before := repo.asked.Load()
		// Never asked: the reader's answer for a coordinate that is not
		// one, as for a range or a snapshot (round 3).
		if _, outcome := src.RemotePom(c[0], c[1], c[2], nil); outcome != manifest.RemoteNotFound || repo.asked.Load() != before {
			t.Errorf("%q: outcome %v, asked %d times; want not found and nothing asked", c, outcome, repo.asked.Load()-before)
		}
	}
	for _, ok := range [][3]string{{"org.remote", "parent", "5.0"}, {"com.fasterxml.jackson.core", "jackson-databind", "2.15.0-rc1"}, {"g", "a_b", "1.0+build.7"}} {
		if !mavenCoordinateAskable(ok[0], ok[1], ok[2]) {
			t.Errorf("%q is a coordinate and was refused", ok)
		}
	}
	if got := mavenPomURL("https://r.example/m2", "a.b", "c", "1.0"); got != "https://r.example/m2/a/b/c/1.0/c-1.0.pom" {
		t.Errorf("mavenPomURL = %s", got)
	}
}

// A redirect to a host the operator distrusted is not followed.
func TestARedirectToADistrustedHostIsNotFollowed(t *testing.T) {
	ctx := withDistrustedHosts(context.Background(), hostguard.NewSnapshot([]string{"Bad.Example:8443"}))
	hop := func(target string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		if err != nil {
			t.Fatal(err)
		}
		return registryCheckRedirect(req, nil)
	}
	if err := hop("https://bad.example:8443/m2/x.pom"); !errors.Is(err, errRegistryRedirectDistrusted) {
		t.Errorf("a hop to a distrusted host: %v, want refused as distrusted", err)
	}
	if err := hop("https://bad.example:8443/m2/x.pom"); err == nil || strings.Contains(err.Error(), "bad.example") {
		t.Errorf("the refusal names the target: %v", err)
	}
	if err := hop("https://good.example/m2/x.pom"); errors.Is(err, errRegistryRedirectDistrusted) {
		t.Errorf("a hop to another host was refused as distrusted: %v", err)
	}
	if !isRepositoryRefusal(errRegistryRedirectDistrusted) || isRegistryHostFailure(context.Background(), errRegistryRedirectDistrusted) {
		t.Error("a distrusted hop is a refusal, and not a failure of the host that sent it")
	}
}
