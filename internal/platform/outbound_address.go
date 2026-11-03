// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"sort"
	"strings"
)

// The outbound-address rule (0.29.132; summary/64 §4, the SSRF class the
// operator took on 2026-11-03). Every repository URL becomes a HEAD probe
// from the serve host, redirects are followed, and the final URL is cloned,
// so a URL that names the host's own services, the cloud metadata endpoint
// or a private network made the collector a proxy into it — what leaked was
// reachability, never content, and a non-administrator's URL waits for
// approval, but an administrator's add ran at once. The rule: a host whose
// address is loopback, link-local, multicast, unspecified, private (RFC
// 1918, ULA), carrier-grade NAT (RFC 6598) or another IANA special-purpose
// range (specialUse), or an IPv6 transition address embedding such an IPv4
// address, or a numeric host that is not a dotted-decimal address, is
// refused, after resolution, unless the operator trusts it — the host of a configured forge instance
// (github.base_url, gitlab.base_url, web.gitlab_base_url: a self-hosted
// GitLab or GitHub Enterprise on a private network works untouched) or an
// entry of collection.private_network_hosts (a name, or a CIDR for a
// standalone git host). Link-local is never allowed: no repository host
// lives there, and the cloud metadata service does. ONE rule (SR-17),
// consulted at every boundary that reaches a repository's host from the
// serve host: the redirect probe (each hop), the two git clones, the
// scorecard run — pinned by the collector's boundary test.
//
// The check is by name before the connection; git and scorecard are
// subprocesses, so a DNS answer that changes between the check and their
// connect (rebinding) is a residual the rule cannot close — the probe's
// own connection pins the address it checked (DialContext) when no proxy
// is configured. Refused at collection, not at the add: the add runs in
// the web or api process, which need not share the serve host's network,
// and the serve host is where the risk is; the refusal is logged with the
// key to set, and the repository is never marked gone for it (SR-7).

// ErrPrivateAddress is the rule's refusal. Its text names the key.
var ErrPrivateAddress = errors.New("the host resolves to a private or special-use address the collector does not reach (a self-hosted forge is trusted by its configured base_url; a private git host must be listed in collection.private_network_hosts)")

// ErrLinkLocalAddress is the refusal no allowlist lifts.
var ErrLinkLocalAddress = fmt.Errorf("link-local addresses are never reached (no repository host lives there; the cloud metadata service does): %w", ErrPrivateAddress)

// ErrNumericHost refuses a host that is a number in some notation but not
// a dotted-decimal or IPv6 address: parsers disagree on it (L10 round 1 of
// 0.29.132: the system resolver read 0177.0.0.1 as 177.0.0.1, public, while
// git's libcurl read it as octal, 127.0.0.1), so the check and the
// subprocess would reach different hosts. Only the forms netip parses are
// one address everywhere; the rest are refused as written.
var ErrNumericHost = fmt.Errorf("a numeric host that is not a dotted-decimal or IPv6 address is refused (parsers disagree on octal, hexadecimal and integer forms): %w", ErrPrivateAddress)

// specialUse are the IANA special-purpose ranges netip's predicates do not
// cover (loopback, unspecified, multicast, link-local and private are
// predicates): RFC 6598's shared address space, "this network", IETF
// protocol assignments, the benchmarking range, the reserved class E block
// and limited broadcast.
var specialUse = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),
	netip.MustParsePrefix("0.0.0.0/8"),
	netip.MustParsePrefix("192.0.0.0/24"),
	netip.MustParsePrefix("198.18.0.0/15"),
	netip.MustParsePrefix("240.0.0.0/4"),
	netip.MustParsePrefix("255.255.255.255/32"),
}

// IPv6 ranges that carry an IPv4 address inside them: the embedded address
// is what the connection reaches, so it is the one classified.
var (
	nat64      = netip.MustParsePrefix("64:ff9b::/96")   // RFC 6052, the last four bytes
	nat64Local = netip.MustParsePrefix("64:ff9b:1::/48") // RFC 8215, the last four bytes
	sixToFour  = netip.MustParsePrefix("2002::/16")      // RFC 3056, bytes 2–5
	teredo     = netip.MustParsePrefix("2001::/32")      // RFC 4380, the last four bytes inverted
)

// strictDialer is the connection a nil (strict) policy makes.
var strictDialer net.Dialer

// OutboundPolicy is the rule with the operator's trust. The zero value
// trusts nothing (strict); NewOutboundPolicy builds one from an allowlist.
type OutboundPolicy struct {
	hosts  map[string]struct{} // trusted names, lowercase, no port
	nets   []netip.Prefix      // trusted address ranges
	lookup func(ctx context.Context, host string) ([]netip.Addr, error)
	dialer net.Dialer
}

// NewOutboundPolicy builds the rule with the operator's trust: each entry is
// a host name (trusted as written, case-insensitively, with no port) or an
// address range in CIDR form (a bare address is its own /32 or /128). A
// link-local range is refused: the rule never lifts it. Entries are
// normalised; the order does not matter.
func NewOutboundPolicy(allow []string) (*OutboundPolicy, error) {
	p := &OutboundPolicy{hosts: map[string]struct{}{}}
	for _, raw := range allow {
		e := strings.TrimSpace(raw)
		if e == "" {
			continue
		}
		if prefix, err := parseAllowedPrefix(e); err == nil {
			if prefix.Addr().IsLinkLocalUnicast() || prefix.Addr().IsLinkLocalMulticast() {
				return nil, fmt.Errorf("%q: %w", raw, ErrLinkLocalAddress)
			}
			p.nets = append(p.nets, prefix)
			continue
		}
		host := strings.TrimSuffix(strings.ToLower(e), ".")
		// A wildcard or an empty name would be stored and never match: said
		// now, not discovered as silent non-trust (L10 round 2).
		if host == "" || strings.ContainsAny(host, "*/:@ ") || strings.Contains(host, "..") {
			return nil, fmt.Errorf("%q is neither a host name nor an address range in CIDR form (a name is exact; no wildcards)", raw)
		}
		p.hosts[host] = struct{}{}
	}
	sort.Slice(p.nets, func(i, j int) bool { return p.nets[i].String() < p.nets[j].String() })
	return p, nil
}

func parseAllowedPrefix(e string) (netip.Prefix, error) {
	if prefix, err := netip.ParsePrefix(e); err == nil {
		return prefix.Masked(), nil
	}
	addr, err := netip.ParseAddr(e)
	if err != nil {
		return netip.Prefix{}, err
	}
	return netip.PrefixFrom(addr, addr.BitLen()), nil
}

// WithResolver answers a copy of the policy that resolves names through
// lookup instead of the system resolver (a caller with its own resolver;
// the collector's probe test drives a resolver that fails once).
func (p *OutboundPolicy) WithResolver(lookup func(ctx context.Context, host string) ([]netip.Addr, error)) *OutboundPolicy {
	if p == nil {
		p = &OutboundPolicy{hosts: map[string]struct{}{}}
	}
	c := *p
	c.lookup = lookup
	return &c
}

// Allow reports the policy's trust as the normalised entries it holds, for
// the log line that states the effective value.
func (p *OutboundPolicy) Allow() []string {
	if p == nil {
		return nil
	}
	out := make([]string, 0, len(p.hosts)+len(p.nets))
	for h := range p.hosts {
		out = append(out, h)
	}
	sort.Strings(out)
	for _, n := range p.nets {
		out = append(out, n.String())
	}
	return out
}

// ErrNoHost is RepositoryHost's answer for a URL with no host: a local
// path, or a URL whose authority is empty.
var ErrNoHost = errors.New("repository URL has no host")

// Refuse answers nil when rawURL's host may be reached, ErrPrivateAddress
// (or ErrLinkLocalAddress) when not, and the resolver's error when the host
// cannot be resolved (a host that does not resolve cannot be cloned either;
// the caller reports it as a failure, not a refusal). The host is read from
// an http, https, git or ssh URL, or an SCP-style clone URL (user@host:path).
// A URL with no host (a local path) reaches nothing over the network, so
// the rule has nothing to judge: nil — which URL forms a clone may use at
// all is the scheme validation's decision (validateGitURL in the
// collector), not this rule's.
func (p *OutboundPolicy) Refuse(ctx context.Context, rawURL string) error {
	host, err := RepositoryHost(rawURL)
	if errors.Is(err, ErrNoHost) {
		return nil
	}
	if err != nil {
		return err
	}
	return p.RefuseHost(ctx, host)
}

// RepositoryHost is the host part of a repository or clone URL, lowercase,
// without a port: an http, https, git or ssh URL's, or an SCP-style clone
// URL's (git@host:path). An error names a URL with no host.
func RepositoryHost(rawURL string) (string, error) {
	raw := strings.TrimSpace(rawURL)
	if strings.Contains(raw, "://") {
		u, err := url.Parse(raw)
		if err != nil {
			return "", fmt.Errorf("repository URL: %w", err)
		}
		if u.Hostname() == "" {
			return "", fmt.Errorf("%w: %q", ErrNoHost, RedactURLUserinfo(raw))
		}
		return strings.ToLower(strings.TrimSuffix(u.Hostname(), ".")), nil
	}
	// SCP-style: [user@]host:path, no scheme.
	rest := raw
	if at := strings.IndexByte(rest, '@'); at >= 0 {
		rest = rest[at+1:]
	}
	colon := strings.IndexByte(rest, ':')
	if colon <= 0 {
		return "", fmt.Errorf("%w: %q", ErrNoHost, RedactURLUserinfo(raw))
	}
	return strings.ToLower(strings.TrimSuffix(rest[:colon], ".")), nil
}

// RefuseHost is Refuse for a bare host (a name or an address, with or
// without a port). A trusted name is not resolved: the operator trusts the
// name as written. Otherwise every address the name resolves to is checked
// — a name that resolves to one private address among public ones is
// refused, since the connection may land on either.
func (p *OutboundPolicy) RefuseHost(ctx context.Context, host string) error {
	name := strings.ToLower(strings.TrimSuffix(host, "."))
	if h, _, err := net.SplitHostPort(name); err == nil {
		name = h
	}
	name = strings.Trim(name, "[]")
	if p != nil {
		if _, ok := p.hosts[name]; ok {
			return nil
		}
	}
	addrs, err := p.resolve(ctx, name)
	if err != nil {
		return fmt.Errorf("resolving %s: %w", name, err)
	}
	for _, a := range addrs {
		if err := p.refuseAddr(a); err != nil {
			return fmt.Errorf("%s (%s): %w", name, a, err)
		}
	}
	return nil
}

func (p *OutboundPolicy) resolve(ctx context.Context, name string) ([]netip.Addr, error) {
	if a, err := netip.ParseAddr(name); err == nil {
		return []netip.Addr{a}, nil
	}
	if numericHost(name) {
		return nil, ErrNumericHost
	}
	if p != nil && p.lookup != nil {
		return p.lookup(ctx, name)
	}
	return net.DefaultResolver.LookupNetIP(ctx, "ip", name)
}

// numericHost reports a host that is not a netip address but reads as a
// number: its last label is all digits or 0x-prefixed hexadecimal (the
// WHATWG "ends in a number" rule, which is what browsers and libcurl treat
// as an IPv4 address in octal, hexadecimal or integer form).
func numericHost(name string) bool {
	last := name[strings.LastIndexByte(name, '.')+1:]
	if last == "" {
		return false
	}
	if strings.HasPrefix(last, "0x") || strings.HasPrefix(last, "0X") {
		return len(last) > 2 && strings.Trim(last[2:], "0123456789abcdefABCDEF") == ""
	}
	return strings.Trim(last, "0123456789") == ""
}

// dial is the connection this policy makes; a nil policy's is the strict
// dialer (L10 round 1 of 0.29.132: the nil policy dereferenced itself here).
func (p *OutboundPolicy) dial() *net.Dialer {
	if p == nil {
		return &strictDialer
	}
	return &p.dialer
}

// embeddedIPv4 is the IPv4 address an IPv6 transition address carries,
// when it carries one.
func embeddedIPv4(a netip.Addr) (netip.Addr, bool) {
	if !a.Is6() || a.Is4In6() {
		return netip.Addr{}, false
	}
	b := a.As16()
	switch {
	case nat64.Contains(a) || nat64Local.Contains(a):
		return netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}), true
	case sixToFour.Contains(a):
		return netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]}), true
	case teredo.Contains(a):
		return netip.AddrFrom4([4]byte{^b[12], ^b[13], ^b[14], ^b[15]}), true
	}
	return netip.Addr{}, false
}

// refuseAddr classifies one address: link-local is refused outright; the
// other special-use kinds are refused unless a trusted range holds the
// address; everything else is reachable.
func (p *OutboundPolicy) refuseAddr(a netip.Addr) error {
	// The zone is a scope, not an address: dropped before any range check
	// (netip's Contains answers false for every zoned address, so a zoned
	// NAT64 or 6to4 spelling of a loopback host passed; L10 round 2).
	a = a.Unmap().WithZone("")
	if inner, ok := embeddedIPv4(a); ok {
		// The transition address reaches its embedded IPv4 host: classify
		// that one, and a trusted range may hold either spelling.
		if err := p.refuseAddr(inner); err == nil || !p.trusted(a) {
			return err
		}
		return nil
	}
	if a.IsLinkLocalUnicast() || a.IsLinkLocalMulticast() {
		return ErrLinkLocalAddress
	}
	special := a.IsLoopback() || a.IsUnspecified() || a.IsMulticast() || a.IsPrivate()
	for _, n := range specialUse {
		if n.Contains(a) {
			special = true
		}
	}
	if !special {
		return nil
	}
	if p.trusted(a) {
		return nil
	}
	return ErrPrivateAddress
}

// trusted reports whether a trusted range holds the address.
func (p *OutboundPolicy) trusted(a netip.Addr) bool {
	if p == nil {
		return false
	}
	for _, n := range p.nets {
		if n.Contains(a) {
			return true
		}
	}
	return false
}

// DialContext is a net.Dialer's DialContext under the rule: the address is
// resolved, each candidate is checked, and the connection is made to a
// permitted candidate — so what was checked is what is dialed (no DNS
// rebinding between the two). A trusted name dials as it is.
func (p *OutboundPolicy) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	name := strings.ToLower(strings.TrimSuffix(host, "."))
	if p != nil {
		if _, ok := p.hosts[name]; ok {
			return p.dial().DialContext(ctx, network, address)
		}
	}
	addrs, err := p.resolve(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("resolving %s: %w", name, err)
	}
	for _, a := range addrs {
		if err := p.refuseAddr(a); err != nil {
			return nil, fmt.Errorf("%s (%s): %w", name, a, err)
		}
	}
	var last error
	for _, a := range addrs {
		conn, derr := p.dial().DialContext(ctx, network, net.JoinHostPort(a.Unmap().WithZone("").String(), port))
		if derr == nil {
			return conn, nil
		}
		last = derr
	}
	if last == nil {
		last = fmt.Errorf("resolving %s: no address", name)
	}
	return nil, last
}
