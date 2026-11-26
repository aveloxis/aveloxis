// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostguard"
	"github.com/aveloxis/aveloxis/internal/hostwatch"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// The registry request path's boundary (v0.29.179; summary/70, 4.5). Every
// request a libyear resolver makes goes through doRegistryRequest. Until
// this release that path had no outbound-address check, followed redirects
// wherever they led, and read a response of any size. It is hardened here,
// BEFORE the path is given any host a collected repository can name.
//
//   - The fixed registries are trusted by name, the way a configured forge
//     host is: any address but link-local, so a mirror on the operator's
//     network works. Every other host a request reaches (a redirect's
//     target; later, a repository a build file declares) is judged by the
//     full outbound-address rule.
//   - The dial pins the address that was checked (no DNS rebinding).
//   - Each redirect hop is checked before it is followed: its scheme, no
//     credentials in the URL, the rule by name, and a bound on the chain.
//   - A response body is bounded, by the caller.
//   - A host that keeps failing is rested (hostguard).

// fixedRegistryHosts are the registries the resolvers are written against.
// A new resolver's host is added here, in review
// (TestEveryShippedRegistryBaseIsAFixedRegistry).
var fixedRegistryHosts = []string{
	"registry.npmjs.org",
	"pypi.org",
	"proxy.golang.org",
	"crates.io",
	"rubygems.org",
	"repo1.maven.org",
	"repo.packagist.org",
	"hex.pm",
	"api.nuget.org",
	"pub.dev",
	"hackage.haskell.org",
	"crandb.r-pkg.org",
}

// registryPolicyFrom is the process's outbound-address rule with the fixed
// registries added to the names it trusts.
func registryPolicyFrom(base *platform.OutboundPolicy) *platform.OutboundPolicy {
	return base.WithTrustedHosts(fixedRegistryHosts...)
}

// mavenRepositories is the operator's list (collection.maven_repositories),
// installed by loadConfig. nil, the value before it is installed, means the
// one shipped repository (mavenRepositoryBase), which is also what a test
// points at its own server.
var mavenRepositories atomic.Pointer[[]string]

// SetMavenRepositories installs the configured Maven repositories, in the
// order they are asked. An empty list asks none.
func SetMavenRepositories(bases []string) {
	list := append([]string{}, bases...)
	mavenRepositories.Store(&list)
}

// mavenRepositoryBases is the list the Maven resolver asks.
func mavenRepositoryBases() []string {
	if p := mavenRepositories.Load(); p != nil {
		return *p
	}
	return []string{mavenRepositoryBase}
}

// registryPolicyEntry is the derived policy with the rule and the list it
// was built from.
type registryPolicyEntry struct {
	base    *platform.OutboundPolicy
	repos   *[]string
	derived *platform.OutboundPolicy
}

// registryPolicyCache holds the last derived policy, so it is built once
// per installed rule and list, and not per request.
var registryPolicyCache atomic.Pointer[registryPolicyEntry]

// registryPolicy is the rule every registry request is made under: the
// process's, with the fixed registries and the operator's configured Maven
// repositories trusted by name.
func registryPolicy() *platform.OutboundPolicy {
	base, repos := currentOutboundPolicy(), mavenRepositories.Load()
	if c := registryPolicyCache.Load(); c != nil && c.base == base && c.repos == repos {
		return c.derived
	}
	derived := registryPolicyFrom(base)
	if repos != nil {
		var hosts []string
		for _, raw := range *repos {
			if h, err := platform.RepositoryHost(raw); err == nil {
				hosts = append(hosts, h)
			}
		}
		derived = derived.WithTrustedHosts(hosts...)
	}
	registryPolicyCache.Store(&registryPolicyEntry{base: base, repos: repos, derived: derived})
	return derived
}

// declaredHostPolicy is the rule for a repository a collected build file
// DECLARES: the process's with none of its trust (platform's Strict). The
// operator's trusted names and ranges exist so that what THEY configured
// can be reached; a pom's author is not the operator, and a declared host
// dialed under that trust would be a way to reach an internal service. A
// seam for the tests whose declared repositories are loopback servers;
// its default is pinned (TestTheDeclaredHostPolicyIsStrictByDefault).
var declaredHostPolicy = func() *platform.OutboundPolicy { return currentOutboundPolicy().Strict() }

type registryDeclaredHostKey struct{}

// withDeclaredRepositoryHost marks the requests made under ctx as ones to a
// host a collected repository chose: https on every hop, judged by
// declaredHostPolicy at the dial and at every redirect, and sent on a
// transport of their own.
func withDeclaredRepositoryHost(ctx context.Context) context.Context {
	return context.WithValue(withRegistryHTTPSOnly(ctx), registryDeclaredHostKey{}, true)
}

func registryDeclaredHost(ctx context.Context) bool {
	on, _ := ctx.Value(registryDeclaredHostKey{}).(bool)
	return on
}

// registryPolicyFor is the rule for one request: THE place that chooses
// between the registries' rule and the declared hosts'.
func registryPolicyFor(ctx context.Context) *platform.OutboundPolicy {
	if registryDeclaredHost(ctx) {
		return declaredHostPolicy()
	}
	return registryPolicy()
}

// registryClientFor is the client for one request. A declared host's
// requests have their own transport, so that a connection made under the
// registries' rule (to a configured repository) is never reused for an
// address a build file chose at the same host and port.
func registryClientFor(ctx context.Context) *http.Client {
	if registryDeclaredHost(ctx) {
		return declaredHTTPClient
	}
	return registryHTTPClient
}

// declaredDial is a declared host's connection: declaredHostPolicy, always.
func declaredDial(ctx context.Context, network, address string) (net.Conn, error) {
	if host, _, err := net.SplitHostPort(address); err == nil {
		if _, ok := proxyHosts[strings.ToLower(host)]; ok {
			var d net.Dialer
			return d.DialContext(ctx, network, address)
		}
	}
	return declaredHostPolicy().DialContext(ctx, network, address)
}

// registryDial is a registry request's connection under the rule: the
// address checked is the address dialed. A dial to a configured proxy is
// the proxy's, made as is (probeDial's rule; the target is then checked by
// name before the hop).
func registryDial(ctx context.Context, network, address string) (net.Conn, error) {
	if host, _, err := net.SplitHostPort(address); err == nil {
		if _, ok := proxyHosts[strings.ToLower(host)]; ok {
			var d net.Dialer
			return d.DialContext(ctx, network, address)
		}
	}
	return registryPolicy().DialContext(ctx, network, address)
}

// RepositoryHostProbe looks at one package repository host under the rule
// a fetch from it is made under (0.29.189). A configured repository's is
// the registries' rule: it is trusted by name, so a private mirror is not
// reported as pointing inward. Any other host is one a collected build
// file declared, and is looked at under declaredHostPolicy, with none of
// the operator's trust (0.29.208): the check must not dial what a fetch
// may not. Link-local is refused for every host.
func RepositoryHostProbe(ctx context.Context, host string) []hostwatch.Observation {
	return hostwatch.Prober{Policy: repositoryHostProbePolicy(host)}.Probe(ctx, host)
}

func repositoryHostProbePolicy(host string) *platform.OutboundPolicy {
	host = hostguard.Normalize(host)
	for _, base := range mavenRepositoryBases() {
		if hostguard.OfURL(base) == host {
			return registryPolicy()
		}
	}
	return declaredHostPolicy()
}

var (
	errRegistryRedirectScheme = errors.New("registry redirect target: not an http or https URL")
	// errRegistryRedirectDistrusted: the hop's host is on the distrusted
	// list. A refusal, like the address rule's.
	errRegistryRedirectDistrusted = errors.New("registry redirect target: its host is distrusted")
	// errRegistryBodyTooLarge: the response is longer than the caller's
	// bound. A lookup failure, not an answer about the package.
	errRegistryBodyTooLarge = errors.New("registry response is larger than its bound")
	// errRegistryHostResting: the host has failed registryBreakerThreshold
	// times in a row and is not asked until its rest ends. A lookup
	// failure, never cached and never an answer.
	errRegistryHostResting = errors.New("registry host is resting after repeated failures")
)

// registryCheckRedirect judges one redirect hop before it is followed. No
// error it returns names the target (a Location can carry credentials).
func registryCheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= maxHops {
		return errTooManyRedirects
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return errRegistryRedirectScheme
	}
	// A request to a host a collected repository named is https on every
	// hop (0.29.184).
	if registryHTTPSOnly(req.Context()) && req.URL.Scheme != "https" {
		return errRegistryRedirectScheme
	}
	if platform.RefuseURLUserinfo(req.URL.String()) != nil {
		return platform.ErrRedirectTargetUserinfo
	}
	// A hop to a host the operator has distrusted is not followed: the
	// decision covers a host however it is reached.
	if !hostguard.MayUse(distrustedHostsFrom(req.Context()), req.URL.Host) {
		return errRegistryRedirectDistrusted
	}
	if err := registryPolicyFor(req.Context()).RefuseHost(req.Context(), req.URL.Host); err != nil {
		if errors.Is(err, platform.ErrLinkLocalAddress) {
			return platform.ErrLinkLocalAddress
		}
		if errors.Is(err, platform.ErrPrivateAddress) {
			return platform.ErrPrivateAddress
		}
		return errors.New("registry redirect target: its host could not be resolved")
	}
	return nil
}

// registryTransport is http.DefaultTransport's settings with the rule's
// dial.
var registryTransport = func() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = registryDial
	t.ForceAttemptHTTP2 = true // a custom DialContext would otherwise drop HTTP/2
	return t
}()

// declaredHTTPClient is registryHTTPClient for a declared host: the same
// timeout and redirect rule, its own transport and dial.
var declaredHTTPClient = func() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DialContext = declaredDial
	t.ForceAttemptHTTP2 = true
	return &http.Client{Timeout: registryHTTPClient.Timeout, Transport: t, CheckRedirect: registryCheckRedirect}
}()

// defaultRegistryBodyLimit bounds a registry document read whole into
// memory. PROVISIONAL (summary/71, section 8): an npm package document
// lists every version ever published and the largest run to hundreds of
// megabytes; the bound is set above those, and the size of every body is
// already logged (platform.NoteResponseSize), which is what a tighter value
// should be set from.
const defaultRegistryBodyLimit int64 = 1 << 30

type registryHTTPSOnlyKey struct{}

// withRegistryHTTPSOnly marks the requests made under ctx as ones that must
// stay on https through every redirect.
func withRegistryHTTPSOnly(ctx context.Context) context.Context {
	return context.WithValue(ctx, registryHTTPSOnlyKey{}, true)
}

func registryHTTPSOnly(ctx context.Context) bool {
	on, _ := ctx.Value(registryHTTPSOnlyKey{}).(bool)
	return on
}

type registryBodyLimitKey struct{}

// withRegistryBodyLimit sets the bound for the registry bodies read under
// ctx.
func withRegistryBodyLimit(ctx context.Context, n int64) context.Context {
	return context.WithValue(ctx, registryBodyLimitKey{}, n)
}

func registryBodyLimit(ctx context.Context) int64 {
	if n, ok := ctx.Value(registryBodyLimitKey{}).(int64); ok && n > 0 {
		return n
	}
	return defaultRegistryBodyLimit
}

// The breaker, one per host. The threshold is the house value for a host
// breaker (the mailing-list sources'). The rest is two request timeouts:
// long enough that a host answering nothing is not hammered by seventy
// workers, short enough that a registry's brief outage does not leave an
// hour of analyses without libyear (the mailing-list pause, an hour, fits a
// monthly archive and not a per-dependency lookup).
const registryBreakerThreshold = mailinglist.DefaultCircuitThreshold

var registryBreakerRest = 2 * registryHTTPClient.Timeout

var registryBreakers sync.Map // host -> *hostguard.Breaker

func registryBreakerFor(host string) *hostguard.Breaker {
	if b, ok := registryBreakers.Load(host); ok {
		return b.(*hostguard.Breaker)
	}
	b, _ := registryBreakers.LoadOrStore(host, hostguard.NewBreaker(registryBreakerThreshold, registryBreakerRest, time.Now))
	return b.(*hostguard.Breaker)
}

// isRegistryHostFailure reports whether a request's outcome is a failure OF
// THE HOST: a transport error, or a 5xx. An answer (a 2xx, a 404, a 429 the
// retry loop paces) is not, and neither is this side's refusal to send, or
// a cancelled request.
func isRegistryHostFailure(ctx context.Context, err error) bool {
	if err == nil || ctx.Err() != nil {
		return false
	}
	if errors.Is(err, platform.ErrPrivateAddress) || errors.Is(err, platform.ErrLinkLocalAddress) ||
		errors.Is(err, platform.ErrRedirectTargetUserinfo) || errors.Is(err, errRegistryRedirectScheme) ||
		errors.Is(err, errRegistryRedirectDistrusted) || errors.Is(err, errTooManyRedirects) {
		return false
	}
	var se *registryStatusError
	if errors.As(err, &se) {
		return se.status >= 500
	}
	return true
}

// registryHostResting is the error for a host whose breaker is open.
func registryHostResting(host string) error {
	return fmt.Errorf("%w: %s", errRegistryHostResting, host)
}
