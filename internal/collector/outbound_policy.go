// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"net"
	"net/url"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/aveloxis/aveloxis/internal/platform"
	"golang.org/x/net/http/httpproxy"
)

// The process's outbound-address rule (0.29.132; platform.OutboundPolicy):
// every boundary in this package that reaches a repository's host from the
// serve host — the redirect probe's hops, the two git clones, the scorecard
// run — consults it first (TestEveryRepositoryHostBoundaryConsultsTheOutboundPolicy).
// The zero value (nil) is the strict rule: it trusts nothing, so a command
// that never set it cannot reach a private host (SR-18). loadConfig sets it
// from the configuration for every command (TestLoadConfigWiresTheOutboundPolicy).
var outboundPolicy atomic.Pointer[platform.OutboundPolicy]

// SetOutboundPolicy installs the process's rule; nil restores the strict one.
func SetOutboundPolicy(p *platform.OutboundPolicy) { outboundPolicy.Store(p) }

// currentOutboundPolicy is the rule every boundary consults.
func currentOutboundPolicy() *platform.OutboundPolicy { return outboundPolicy.Load() }

// RefuseRepositoryHost is the rule for a caller outside this package that
// reaches a repository's host itself (import-from-augur's existence HEAD).
func RefuseRepositoryHost(ctx context.Context, rawURL string) error {
	return currentOutboundPolicy().Refuse(ctx, rawURL)
}

// proxyHosts are the hosts of the proxies the environment names
// (HTTP_PROXY, HTTPS_PROXY): a probe through a proxy dials the proxy, whose
// address the rule must not judge in the target's place (it is the
// operator's egress, often private); the target is still checked by name
// before the hop. The proxy resolves the target itself, so the address the
// rule checked is not the one the proxy reaches — the stated residual
// (SECURITY.md; Copilot's fourteenth review of PR #246, declined: the
// proxy's resolution is under the operator's control, and routing to the
// checked address through the proxy would break hosts that reach the
// internet only through one). Read once: the environment does not change
// under a process.
var proxyHosts = func() map[string]struct{} {
	out := map[string]struct{}{}
	cfg := httpproxy.FromEnvironment()
	for _, raw := range []string{cfg.HTTPProxy, cfg.HTTPSProxy} {
		if raw == "" {
			continue
		}
		if !strings.Contains(raw, "://") {
			raw = "http://" + raw
		}
		if u, err := url.Parse(raw); err == nil && u.Hostname() != "" {
			out[strings.ToLower(u.Hostname())] = struct{}{}
		}
	}
	return out
}()

// ProxyHosts are the proxy hosts the environment names, for the log line
// that states the effective trust: an operator reads from it whether the
// proxy residual (SECURITY.md) is live on this host.
func ProxyHosts() []string {
	out := make([]string, 0, len(proxyHosts))
	for h := range proxyHosts {
		out = append(out, h)
	}
	sort.Strings(out)
	return out
}

// probeDial is the redirect probe's connection under the rule: the address
// checked is the address dialed (platform.OutboundPolicy.DialContext), so a
// DNS answer that changes between the check by name and the connection
// cannot route the probe elsewhere. A dial to a configured proxy is the
// proxy's, made as is.
func probeDial(ctx context.Context, network, address string) (net.Conn, error) {
	if host, _, err := net.SplitHostPort(address); err == nil {
		if _, ok := proxyHosts[strings.ToLower(host)]; ok {
			var d net.Dialer
			return d.DialContext(ctx, network, address)
		}
	}
	return currentOutboundPolicy().DialContext(ctx, network, address)
}
