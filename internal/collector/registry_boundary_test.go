// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aveloxis/aveloxis/internal/outboundtest"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The registry request path under the outbound-address rule (summary/70,
// 4.5, the hardening that comes before any new host is fetched). Before
// 0.29.179 a registry request had no outbound-address check, followed
// redirects wherever they led, and read a body of any size.

// A redirect is followed only to an address the rule permits. The test
// policy resolves *.internal to 10.0.0.5, a private address.
func TestARegistryRedirectIsJudgedByTheOutboundRule(t *testing.T) {
	for name, c := range map[string]struct {
		location string
		want     error
	}{
		"to a private address":      {"http://registry.internal/pkg", platform.ErrPrivateAddress},
		"to the metadata address":   {"http://169.254.169.254/latest/meta-data/", platform.ErrLinkLocalAddress},
		"to a URL with credentials": {"http://user:secret@127.0.0.1/pkg", platform.ErrRedirectTargetUserinfo},
		"to another scheme":         {"ftp://127.0.0.1/pkg", errRegistryRedirectScheme},
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Location", c.location)
			w.WriteHeader(http.StatusFound)
		}))
		_, err := fetchRegistryJSON(context.Background(), srv.URL+"/pkg")
		srv.Close()
		if !errors.Is(err, c.want) {
			t.Errorf("%s: err = %v, want %v", name, err, c.want)
		}
		if err != nil && strings.Contains(err.Error(), "secret") {
			t.Errorf("%s: the error names the redirect target's credentials: %v", name, err)
		}
	}
}

// A redirect to a permitted address is still followed, and a chain ends.
func TestARegistryRedirectChainIsFollowedAndBounded(t *testing.T) {
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, `{"ok":true}`) }))
	defer final.Close()
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Redirect(w, &http.Request{}, final.URL+"/doc", http.StatusMovedPermanently)
	}))
	defer first.Close()
	body, err := fetchRegistryJSON(context.Background(), first.URL+"/pkg")
	if err != nil || string(body) != `{"ok":true}` {
		t.Errorf("a redirect to a permitted host: body %q err %v", body, err)
	}

	var hops atomic.Int64
	var loop *httptest.Server
	loop = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hops.Add(1)
		http.Redirect(w, r, fmt.Sprintf("%s/n%d", loop.URL, hops.Load()), http.StatusFound)
	}))
	defer loop.Close()
	if _, err := fetchRegistryJSON(context.Background(), loop.URL+"/pkg"); !errors.Is(err, errTooManyRedirects) {
		t.Errorf("an endless chain: err = %v, want too many redirects", err)
	}
	if n := hops.Load(); n > int64(maxHops)+1 {
		t.Errorf("an endless chain was followed %d times; the bound is %d", n, maxHops)
	}
}

// The fixed registries are trusted the way a configured forge host is: any
// address but link-local. Every other name is judged by the full rule.
func TestFixedRegistriesAreTrustedByName(t *testing.T) {
	private := func(_ context.Context, _ string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("10.9.8.7")}, nil
	}
	linkLocal := func(_ context.Context, _ string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("169.254.169.254")}, nil
	}
	var strict *platform.OutboundPolicy // the rule with no operator trust at all
	p := registryPolicyFrom(strict)
	for _, host := range fixedRegistryHosts {
		if err := p.WithResolver(private).RefuseHost(context.Background(), host); err != nil {
			t.Errorf("%s resolving to a private address (a mirror on the operator's network): %v", host, err)
		}
		if err := p.WithResolver(linkLocal).RefuseHost(context.Background(), host); !errors.Is(err, platform.ErrLinkLocalAddress) {
			t.Errorf("%s resolving to link-local: err = %v; link-local is never reached, even for a trusted name", host, err)
		}
	}
	if err := p.WithResolver(private).RefuseHost(context.Background(), "repo.attacker.example"); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Errorf("an unlisted host resolving to a private address: err = %v, want refused", err)
	}
	// The operator's own trust is kept.
	if err := registryPolicyFrom(outboundtest.Policy()).RefuseHost(context.Background(), "127.0.0.1"); err != nil {
		t.Errorf("the base policy's trust was lost: %v", err)
	}
}

// Every registry base URL the code ships with names a fixed registry: a new
// resolver cannot be added without its host being listed (and so reviewed).
func TestEveryShippedRegistryBaseIsAFixedRegistry(t *testing.T) {
	listed := map[string]bool{}
	for _, h := range fixedRegistryHosts {
		listed[h] = true
	}
	for name, base := range shippedRegistryBases {
		host, err := platform.RepositoryHost(base)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if !listed[host] {
			t.Errorf("%s ships with %s, whose host %s is not in fixedRegistryHosts", name, base, host)
		}
	}
	if len(shippedRegistryBases) < 11 {
		t.Errorf("only %d shipped bases examined; the eleven registries each have one", len(shippedRegistryBases))
	}
}

// A response body is bounded. The bound is the caller's.
func TestARegistryBodyIsBounded(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, strings.Repeat("x", 4096))
	}))
	defer srv.Close()
	if _, err := fetchRegistryJSON(withRegistryBodyLimit(context.Background(), 1024), srv.URL); !errors.Is(err, errRegistryBodyTooLarge) {
		t.Errorf("a 4096-byte body under a 1024-byte bound: err = %v", err)
	}
	if b, err := fetchRegistryJSON(withRegistryBodyLimit(context.Background(), 4096), srv.URL); err != nil || len(b) != 4096 {
		t.Errorf("a body exactly at the bound: %d bytes, err %v", len(b), err)
	}
	if b, err := fetchRegistryJSON(context.Background(), srv.URL); err != nil || len(b) != 4096 {
		t.Errorf("the default bound: %d bytes, err %v", len(b), err)
	}
}

// A host that keeps failing is rested: after the threshold of consecutive
// transient failures the next request is not sent. A refusal by the server
// that is an answer (404) is not a failure of the host.
func TestARegistryHostThatKeepsFailingIsRested(t *testing.T) {
	var requests atomic.Int64
	status := http.StatusInternalServerError
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(status)
	}))
	defer srv.Close()
	t.Cleanup(resetRegistryBreakers)
	for i := 0; i < registryBreakerThreshold; i++ {
		if _, err := fetchRegistryJSON(context.Background(), srv.URL); err == nil || errors.Is(err, errRegistryHostResting) {
			t.Fatalf("request %d: err = %v; want the server's failure", i, err)
		}
	}
	sent := requests.Load()
	if _, err := fetchRegistryJSON(context.Background(), srv.URL); !errors.Is(err, errRegistryHostResting) {
		t.Errorf("after %d consecutive failures: err = %v, want the host rested", registryBreakerThreshold, err)
	}
	if requests.Load() != sent {
		t.Error("a request was sent to a rested host")
	}
	// A resting host is not an answer about the package: never cached.
	if isCacheableRegistryAnswer(fmt.Errorf("lookup: %w", errRegistryHostResting)) {
		t.Error("a resting host's refusal would be cached as an answer")
	}

	// 404s are answers: any number of them rests nothing.
	resetRegistryBreakers()
	status = http.StatusNotFound
	for i := 0; i < registryBreakerThreshold+5; i++ {
		if _, err := fetchRegistryJSON(context.Background(), srv.URL); errors.Is(err, errRegistryHostResting) {
			t.Fatalf("a run of 404s rested the host at request %d", i)
		}
	}
}

// The path takes a request body (chunk 2's OSV client uses it).
func TestARegistryRequestCarriesABody(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got = r.Method + " " + r.Header.Get("Content-Type") + " " + string(b)
	}))
	defer srv.Close()
	resp, err := doRegistryRequestBody(context.Background(), http.MethodPost, srv.URL, []byte(`{"q":1}`), "Content-Type: application/json")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if got != `POST application/json {"q":1}` {
		t.Errorf("the server received %q", got)
	}
}

// The boundary tripwire, widened to registry requests: every one is sent by
// the one client, whose transport dials under the rule and whose redirects
// are judged, and nothing in the package sends a registry request around it.
func TestEveryRegistryRequestGoesThroughTheBoundary(t *testing.T) {
	if registryHTTPClient.Transport != registryTransport {
		t.Error("the registry client's transport is not the rule's")
	}
	if registryHTTPClient.CheckRedirect == nil {
		t.Error("the registry client follows redirects unjudged")
	}
	if registryTransport.DialContext == nil {
		t.Fatal("the registry transport has no dial of its own")
	}
	// The declared hosts' client (0.29.208): the same redirect rule, a
	// transport and a dial of its own.
	dt, ok := declaredHTTPClient.Transport.(*http.Transport)
	if !ok || dt == registryTransport || dt.DialContext == nil || declaredHTTPClient.CheckRedirect == nil || declaredHTTPClient.Timeout != registryHTTPClient.Timeout {
		t.Error("the declared hosts' client must have its own transport and dial, the redirect rule and the registry timeout")
	}
	send := srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", "func doRegistryRequestBody("))
	if strings.Count(send, "registryClientFor(ctx).Do(") != 1 {
		t.Error("doRegistryRequestBody sends each attempt through registryClientFor(ctx)")
	}
	// Negative needles over the whole package: a client is used by that one
	// function, chosen by that one chooser, and no registry code reaches
	// for net/http's default.
	all := srctest.StripGoComments(srctest.PackageSource(t, "internal/collector", 40))
	if n := strings.Count(all, "registryClientFor("); n != 2 { // its declaration and the one send
		t.Errorf("registryClientFor( appears %d times in the package; want its declaration and the one send", n)
	}
	for _, direct := range []string{"registryHTTPClient.Do(", "declaredHTTPClient.Do(", "declaredHTTPClient.Get(", "declaredHTTPClient.Head(", "declaredHTTPClient.Post("} {
		if strings.Contains(all, direct) {
			t.Errorf("%s sends around the chooser", direct)
		}
	}
	for _, around := range []string{"registryHTTPClient.Get(", "registryHTTPClient.Head(", "registryHTTPClient.Post("} {
		if strings.Contains(all, around) {
			t.Errorf("%s sends a registry request around doRegistryRequestBody (its pacing, its breaker, its by-name check)", around)
		}
	}
}

// The rule is applied twice, and each application is tested alone, because
// each hides the other's absence: the redirect check judges a hop by name
// (the only check a request through a proxy gets, since its dial is the
// proxy's), and the dial pins the address that was checked (what stops a
// name that resolves differently a moment later).
func TestTheRegistryRuleHoldsAtEachLayerAlone(t *testing.T) {
	ctx := context.Background()
	hop := func(raw string) error {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, raw, nil)
		if err != nil {
			t.Fatal(err)
		}
		return registryCheckRedirect(req, nil)
	}
	if err := hop("http://registry.internal/pkg"); !errors.Is(err, platform.ErrPrivateAddress) {
		t.Errorf("the redirect check, a private target: %v", err)
	}
	if err := hop("http://169.254.169.254/x"); !errors.Is(err, platform.ErrLinkLocalAddress) {
		t.Errorf("the redirect check, the metadata address: %v", err)
	}
	if err := hop("http://127.0.0.1/x"); err != nil {
		t.Errorf("the redirect check, a permitted target: %v", err)
	}

	if conn, err := registryDial(ctx, "tcp", "registry.internal:80"); !errors.Is(err, platform.ErrPrivateAddress) {
		if conn != nil {
			conn.Close()
		}
		t.Errorf("the dial, a private target: %v", err)
	}
	if conn, err := registryDial(ctx, "tcp", "169.254.169.254:80"); !errors.Is(err, platform.ErrLinkLocalAddress) {
		if conn != nil {
			conn.Close()
		}
		t.Errorf("the dial, the metadata address: %v", err)
	}
}

// shippedRegistryBases are the base URLs the resolvers ship with, read at
// package initialisation, before any test points one at a local server.
var shippedRegistryBases = map[string]string{
	"npm": npmRegistryBase, "pypi": pypiRegistryBase, "go": goProxyBase, "cargo": cratesRegistryBase,
	"rubygems": rubygemsRegistryBase, "maven": mavenRepositoryBase, "packagist": packagistRegistryBase,
	"hex": hexRegistryBase, "nuget": nugetRegistryBase, "pub": pubDevRegistryBase, "hackage": hackageRegistryBase,
	"cran": cranRegistryBase,
}

// resetRegistryBreakers forgets every host's state (tests).
func resetRegistryBreakers() {
	registryBreakers.Range(func(k, _ any) bool { registryBreakers.Delete(k); return true })
}
