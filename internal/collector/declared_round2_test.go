// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostwatch"
	"github.com/aveloxis/aveloxis/internal/manifest"
)

// Review round 2 of the vulnerability program's first part, on the
// declared-host fixes of 0.29.208.

// A lookup asks a bounded number of declared repositories. The time budget
// bounds waiting; this bounds the count, which a refusal at the address
// rule costs no time at all (200 declared repositories and 50 coordinates
// were 10,000 asks with the budget untouched).
func TestALookupAsksABoundedNumberOfDeclaredRepositories(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-count")
	withMavenRepositories(t)
	withDeclaredFetch(t, true)
	var asked atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		asked.Add(1)
		http.NotFound(w, nil)
	}))
	defer srv.Close()
	var declared []string
	for i := 0; i < 3*maxDeclaredRepositoriesAsked; i++ {
		declared = append(declared, fmt.Sprintf("%s/r%d", srv.URL, i))
	}
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", declared); outcome != manifest.RemoteFailed {
		t.Errorf("outcome %v; repositories were left unasked, so the coordinate is not known (failed), never not-found", outcome)
	}
	if n := asked.Load(); n != int64(maxDeclaredRepositoriesAsked) {
		t.Errorf("%d declared repositories asked, want the first %d", n, maxDeclaredRepositoriesAsked)
	}
	if src.declaredNotAsked != 2*maxDeclaredRepositoriesAsked {
		t.Errorf("declaredNotAsked = %d, want %d", src.declaredNotAsked, 2*maxDeclaredRepositoriesAsked)
	}
	// At the bound exactly, every one is asked and "not found" is the answer.
	asked.Store(0)
	if _, outcome := src.RemotePom("org.remote", "other", "5.0", declared[:maxDeclaredRepositoriesAsked]); outcome != manifest.RemoteNotFound {
		t.Errorf("at the bound: outcome %v, want not found", outcome)
	}
}

// One request to a declared repository cannot outlast what is left of the
// analysis's budget: a host that answers 429 with a short Retry-After kept
// one fetch going for its whole retry allowance of round trips, each to the
// client's timeout, before the budget was looked at again.
func TestADeclaredRequestEndsWithTheBudget(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-deadline")
	withMavenRepositories(t)
	withDeclaredFetch(t, true)
	old := declaredFetchBudget
	declaredFetchBudget = 150 * time.Millisecond
	t.Cleanup(func() { declaredFetchBudget = old })
	var asked atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked.Add(1)
		select {
		case <-r.Context().Done():
		case <-time.After(5 * time.Second):
		}
	}))
	defer slow.Close()
	start := time.Now()
	if _, outcome := src.RemotePom("org.remote", "one", "1.0", []string{slow.URL}); outcome != manifest.RemoteFailed {
		t.Errorf("outcome %v, want failed (not known)", outcome)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Errorf("one declared request took %s with a budget of %s", took, declaredFetchBudget)
	}
	if _, outcome := src.RemotePom("org.remote", "two", "1.0", []string{slow.URL}); outcome != manifest.RemoteFailed || asked.Load() != 1 {
		t.Errorf("after the budget: outcome %v, asked %d times; want failed and no second request", outcome, asked.Load())
	}
}

// A connection opened for a CONFIGURED repository is never used for a
// declared address at the same host and port. The first test of this
// (0.29.208) used a listener that closed every connection before TLS, so
// nothing was ever pooled and a shared pool passed it.
func TestADeclaredAddressNeverRidesAConfiguredConnection(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-pool")
	var paths []string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path)
		http.NotFound(w, r)
	}))
	defer srv.Close()
	u, _ := url.Parse(srv.URL)
	// httptest's certificate is valid for example.com: trusted by name
	// here, resolving to the server.
	trustingPolicy(t, "example.com")
	pool := srv.Client().Transport.(*http.Transport).TLSClientConfig.RootCAs
	for _, c := range []*http.Client{registryHTTPClient, declaredHTTPClient} {
		tr := c.Transport.(*http.Transport)
		oldTLS := tr.TLSClientConfig
		tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
		t.Cleanup(func() { tr.TLSClientConfig = oldTLS; tr.CloseIdleConnections() })
	}
	base := "https://example.com:" + u.Port()
	withMavenRepositories(t, base+"/configured")
	oldFetch := declaredRepositoryFetch.Load()
	SetDeclaredRepositoryFetch(true)
	t.Cleanup(func() { SetDeclaredRepositoryFetch(oldFetch) })

	if _, outcome := src.RemotePom("org.remote", "parent", "5.0", nil); outcome != manifest.RemoteNotFound || len(paths) != 1 {
		t.Fatalf("the configured repository: outcome %v, %d requests; want one request that reached the server (the control)", outcome, len(paths))
	}
	// A connection to host:port is now idle in the configured pool.
	withMavenRepositories(t)
	if _, outcome := src.RemotePom("org.remote", "parent", "6.0", []string{base + "/declared"}); outcome != manifest.RemoteRefused {
		t.Errorf("the declared address at the same host and port: outcome %v, want refused", outcome)
	}
	for _, p := range paths {
		if strings.HasPrefix(p, "/declared") {
			t.Fatalf("a request for a declared address reached a host trusted only for the operator's own repository: %v", paths)
		}
	}
}

// The daily check, as the scheduler calls it, dials a declared host under
// no trust.
func TestTheHostProbeDialsADeclaredHostUnderNoTrust(t *testing.T) {
	port, accepted := countingListener(t)
	trustingPolicy(t, "mirror.corp.example")
	withMavenRepositories(t)
	seen := RepositoryHostProbe(context.Background(), "mirror.corp.example:"+port)
	if accepted.Load() != 0 {
		t.Fatalf("the probe connected to a declared host under the operator's trust (%d connections)", accepted.Load())
	}
	refused := false
	for _, o := range seen {
		refused = refused || (o.Kind == hostwatch.KindRefusedAddress && o.State == hostwatch.Signal)
	}
	if !refused {
		t.Errorf("observations %+v; want the refused-address signal", seen)
	}
	// Configured, it is dialed.
	withMavenRepositories(t, "https://mirror.corp.example:"+port+"/m2")
	RepositoryHostProbe(context.Background(), "mirror.corp.example:"+port)
	if accepted.Load() == 0 {
		t.Error("the probe did not dial a configured repository: the control did not run")
	}
}

// A coordinate that cannot be asked for is answered "not asked" (0.29.214;
// it was "not found", which is a repository's answer), counted on its own,
// and said in the summary line. Round 2 called it "refused",
// which every page and document defines as a repository that may not be
// fetched from: no repository refused anything (round 3).
func TestAnUnaskableCoordinateIsCountedAndSaid(t *testing.T) {
	_, src := pomSourceFixture(t, "coordinate-unaskable")
	repo := newPomRepo(t, nil)
	withMavenRepositories(t, repo.srv.URL)
	var logged bytes.Buffer
	src.logger = slog.New(slog.NewTextHandler(&logged, nil))
	if _, outcome := src.RemotePom("org.remote", "parent", "5.0~rc1", nil); outcome != manifest.RemoteNotAsked || repo.asked.Load() != 0 {
		t.Errorf("outcome %v, asked %d; want not asked and nothing asked", outcome, repo.asked.Load())
	}
	if src.unaskable != 1 || src.refused != 0 {
		t.Errorf("unaskable %d, refused %d; want 1 and 0", src.unaskable, src.refused)
	}
	src.logSummary(7)
	if !strings.Contains(logged.String(), "coordinates_not_askable=1") {
		t.Errorf("the summary line does not say it:\n%s", logged.String())
	}
}

// The cap counts the repositories ASKED: entries that are passed over (one
// that is also configured, one that is not https) do not use it up. And the
// warning for a build past the cap is written.
func TestTheCapCountsOnlyRepositoriesAsked(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-count-asked")
	central := newPomRepo(t, nil)
	withMavenRepositories(t, central.srv.URL)
	oldFetch := declaredRepositoryFetch.Load()
	SetDeclaredRepositoryFetch(true)
	t.Cleanup(func() { SetDeclaredRepositoryFetch(oldFetch) })
	// The https check stays as shipped: the plain-http entries are the ones
	// passed over. The one real declared repository is https and loopback,
	// so the policy seam alone is opened.
	oldPolicy := declaredHostPolicy
	declaredHostPolicy = currentOutboundPolicy
	t.Cleanup(func() { declaredHostPolicy = oldPolicy })
	real := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) }))
	defer real.Close()
	var declared []string
	for i := 0; i < maxDeclaredRepositoriesAsked+5; i++ {
		declared = append(declared, central.srv.URL, fmt.Sprintf("http://plain.example/r%d", i))
	}
	declared = append(declared, real.URL)
	before := central.asked.Load()
	src.RemotePom("org.remote", "parent", "5.0", declared)
	if src.declaredNotAsked != 0 {
		t.Errorf("%d repositories left unasked; the passed-over entries used up the cap", src.declaredNotAsked)
	}
	if central.asked.Load()-before != 1 {
		t.Errorf("the configured repository was asked %d times", central.asked.Load()-before)
	}
	// Past the cap, the summary warns.
	var logged bytes.Buffer
	src.logger = slog.New(slog.NewTextHandler(&logged, nil))
	var many []string
	for i := 0; i < maxDeclaredRepositoriesAsked+3; i++ {
		many = append(many, fmt.Sprintf("https://10.0.0.1/r%d", i))
	}
	src.RemotePom("org.remote", "other", "5.0", many)
	src.logSummary(7)
	if src.declaredNotAsked != 3 || !strings.Contains(logged.String(), "left_unconsulted=3") {
		t.Errorf("declaredNotAsked %d; summary:\n%s", src.declaredNotAsked, logged.String())
	}
}

// A request ends with what is LEFT of the budget, not with a budget of its
// own (round 3: a fresh full budget per request passed the first test,
// which starts with nothing spent).
func TestADeclaredRequestEndsWithWhatIsLeftOfTheBudget(t *testing.T) {
	_, src := pomSourceFixture(t, "declared-left")
	withMavenRepositories(t)
	withDeclaredFetch(t, true)
	old := declaredFetchBudget
	declaredFetchBudget = 3 * time.Second
	t.Cleanup(func() { declaredFetchBudget = old })
	src.declaredSpent = declaredFetchBudget - 150*time.Millisecond
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer slow.Close()
	start := time.Now()
	src.RemotePom("org.remote", "one", "1.0", []string{slow.URL})
	if took := time.Since(start); took > 1500*time.Millisecond {
		t.Errorf("with 150ms left of a 3s budget, one request took %s", took)
	}
}
