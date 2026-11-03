// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
)

// A resolver the test controls: names map to addresses; an unknown name
// fails as the real resolver would.
func fakeLookup(table map[string][]string) func(context.Context, string) ([]netip.Addr, error) {
	return func(_ context.Context, host string) ([]netip.Addr, error) {
		raw, ok := table[host]
		if !ok {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		var out []netip.Addr
		for _, r := range raw {
			out = append(out, netip.MustParseAddr(r))
		}
		return out, nil
	}
}

var names = map[string][]string{
	"forge.example":   {"203.0.113.10"},
	"both.example":    {"203.0.113.11", "10.0.0.9"}, // one public, one private: the connection may land on either
	"gitlab.internal": {"10.1.2.3"},
	"git.lan":         {"192.168.7.7"},
	"ula.example":     {"fd00::1"},
	"cgnat.example":   {"100.64.3.4"},
	"meta.example":    {"169.254.169.254"},
	"v6ll.example":    {"fe80::1"},
	"mapped.example":  {"::ffff:10.9.9.9"},
	"localhost":       {"127.0.0.1", "::1"},
	"six.example":     {"2001:db8::7"},
}

func policy(t *testing.T, allow ...string) *OutboundPolicy {
	t.Helper()
	p, err := NewOutboundPolicy(allow)
	if err != nil {
		t.Fatal(err)
	}
	p.lookup = fakeLookup(names)
	return p
}

func TestOutboundPolicyRefusesSpecialUseAddressesAfterResolution(t *testing.T) {
	p := policy(t)
	ctx := context.Background()
	for _, c := range []struct {
		url  string
		want error // nil = reachable
	}{
		{"https://forge.example/o/r", nil},
		{"https://six.example/o/r", nil},
		{"http://203.0.113.5/o/r.git", nil},
		{"https://both.example/o/r", ErrPrivateAddress},
		{"https://gitlab.internal/g/p", ErrPrivateAddress},
		{"git://git.lan/repo.git", ErrPrivateAddress},
		{"ssh://git@git.lan/repo.git", ErrPrivateAddress},
		{"git@git.lan:repo.git", ErrPrivateAddress},
		{"https://ula.example/o/r", ErrPrivateAddress},
		{"https://cgnat.example/o/r", ErrPrivateAddress},
		{"https://mapped.example/o/r", ErrPrivateAddress},
		{"http://localhost:5555/", ErrPrivateAddress},
		{"http://127.0.0.1:8383/api/v1/health", ErrPrivateAddress},
		{"http://[::1]:8082/", ErrPrivateAddress},
		{"http://0.0.0.0/", ErrPrivateAddress},
		{"http://10.255.0.1/", ErrPrivateAddress},
		{"http://172.16.4.4/", ErrPrivateAddress},
		{"http://169.254.169.254/latest/meta-data/", ErrLinkLocalAddress},
		{"https://meta.example/", ErrLinkLocalAddress},
		{"https://v6ll.example/", ErrLinkLocalAddress},
		{"http://[fe80::1%25eth0]/", ErrLinkLocalAddress},
	} {
		err := p.Refuse(ctx, c.url)
		if c.want == nil && err != nil {
			t.Errorf("%s: reachable, got %v", c.url, err)
		}
		if c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s: want %v, got %v", c.url, c.want, err)
		}
	}
}

func TestOutboundPolicyLinkLocalIsRefusedBeforePrivate(t *testing.T) {
	// ErrLinkLocalAddress wraps ErrPrivateAddress: every arm that refuses the
	// class still fires, and a caller can tell the one no allowlist lifts.
	if !errors.Is(ErrLinkLocalAddress, ErrPrivateAddress) {
		t.Fatal("ErrLinkLocalAddress must wrap ErrPrivateAddress")
	}
}

func TestOutboundPolicyTrustsConfiguredHostsAndRanges(t *testing.T) {
	ctx := context.Background()
	p := policy(t, "GitLab.Internal", "192.168.0.0/16", "fd00::/8", "100.64.3.4")
	for _, u := range []string{
		"https://gitlab.internal/g/p", // trusted by name, not resolved
		"https://GITLAB.INTERNAL./g/p",
		"git://git.lan/repo.git", // 192.168.7.7 inside the range
		"https://ula.example/o/r",
		"https://cgnat.example/o/r", // the bare address is its own /32
	} {
		if err := p.Refuse(ctx, u); err != nil {
			t.Errorf("%s: trusted, got %v", u, err)
		}
	}
	for _, u := range []string{
		"https://both.example/o/r", // 10.0.0.9 is in no trusted range
		"http://localhost:5555/",
		"http://169.254.169.254/", // never
	} {
		if err := p.Refuse(ctx, u); !errors.Is(err, ErrPrivateAddress) {
			t.Errorf("%s: want a refusal, got %v", u, err)
		}
	}
	if got, want := strings.Join(p.Allow(), " "), "gitlab.internal 100.64.3.4/32 192.168.0.0/16 fd00::/8"; got != want {
		t.Errorf("Allow() = %q, want %q", got, want)
	}
}

func TestOutboundPolicyRefusesALinkLocalAllowlistEntry(t *testing.T) {
	for _, e := range []string{"169.254.0.0/16", "169.254.169.254", "fe80::/10"} {
		if _, err := NewOutboundPolicy([]string{e}); !errors.Is(err, ErrLinkLocalAddress) {
			t.Errorf("%q: an allowlist never lifts link-local, got %v", e, err)
		}
	}
	for _, e := range []string{"not a host", "host:8080", "http://x", "a..b", "10.0.0.0/33"} {
		if _, err := NewOutboundPolicy([]string{e}); err == nil {
			t.Errorf("%q: neither a host name nor a CIDR, want an error", e)
		}
	}
	if p, err := NewOutboundPolicy([]string{" ", ""}); err != nil || len(p.Allow()) != 0 {
		t.Errorf("blank entries are skipped, got %v %v", p.Allow(), err)
	}
}

func TestOutboundPolicyZeroValueIsStrict(t *testing.T) {
	var p *OutboundPolicy // nil: trusts nothing, resolves with the system resolver
	if err := p.RefuseHost(context.Background(), "127.0.0.1"); !errors.Is(err, ErrPrivateAddress) {
		t.Errorf("a nil policy must refuse loopback, got %v", err)
	}
	if err := p.RefuseHost(context.Background(), "203.0.113.1:443"); err != nil {
		t.Errorf("a nil policy reaches a public address, got %v", err)
	}
}

func TestOutboundPolicyReportsAnUnresolvableHostAsItsResolverError(t *testing.T) {
	p := policy(t)
	err := p.Refuse(context.Background(), "https://nowhere.example/o/r")
	var dnsErr *net.DNSError
	if !errors.As(err, &dnsErr) || errors.Is(err, ErrPrivateAddress) {
		t.Errorf("an unresolvable host is the resolver's error, not a refusal: %v", err)
	}
}

func TestRepositoryHostReadsEveryCloneURLShape(t *testing.T) {
	for _, c := range []struct{ in, want string }{
		{"https://GitHub.com/o/r", "github.com"},
		{"http://host.example:8080/o/r.git", "host.example"},
		{"git://git.lan/repo.git", "git.lan"},
		{"ssh://git@git.lan:2222/repo.git", "git.lan"},
		{"git@git.lan:org/repo.git", "git.lan"},
		{"user@Host.Example:path", "host.example"},
		{"http://[::1]:8082/x", "::1"},
	} {
		got, err := RepositoryHost(c.in)
		if err != nil || got != c.want {
			t.Errorf("RepositoryHost(%q) = %q, %v; want %q", c.in, got, err, c.want)
		}
	}
	for _, bad := range []string{"", "/local/path", "https:///o/r", "repo.git", "http://"} {
		if _, err := RepositoryHost(bad); err == nil {
			t.Errorf("RepositoryHost(%q): want an error (no host)", bad)
		}
	}
}

// The probe's connection pins what it checked: a dial through the policy
// resolves the name, refuses a special-use answer, and connects to the
// checked address — here a loopback listener, refused by a strict policy
// and reached once loopback is trusted.
func TestOutboundPolicyDialContextPinsTheCheckedAddress(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			c.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	addr := net.JoinHostPort("local.test", strconv.Itoa(port))
	table := map[string][]string{"local.test": {"127.0.0.1"}}
	strict := policy(t)
	strict.lookup = fakeLookup(table)
	if _, err := strict.DialContext(context.Background(), "tcp", addr); !errors.Is(err, ErrPrivateAddress) {
		t.Errorf("a strict policy must refuse the loopback answer, got %v", err)
	}
	trusted := policy(t, "127.0.0.0/8")
	trusted.lookup = fakeLookup(table)
	conn, err := trusted.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		t.Fatalf("loopback trusted by range: %v", err)
	}
	conn.Close()
	byName := policy(t, "local.test")
	byName.lookup = fakeLookup(map[string][]string{}) // a trusted name is not resolved by the policy
	if _, err := byName.DialContext(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port))); err != nil {
		// the literal address is not the trusted name: resolved (literal) and refused as loopback
		if !errors.Is(err, ErrPrivateAddress) {
			t.Errorf("got %v", err)
		}
	}
}
