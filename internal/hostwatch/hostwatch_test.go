// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostwatch

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// policyFor trusts loopback (where the test's server is) and resolves by
// the table; a name not in it is "no such host", and "timeout.invalid" is a
// resolver failure.
func policyFor(t *testing.T, table map[string]string) *platform.OutboundPolicy {
	t.Helper()
	p, err := platform.NewOutboundPolicy([]string{"127.0.0.0/8"})
	if err != nil {
		t.Fatal(err)
	}
	return p.WithResolver(func(_ context.Context, host string) ([]netip.Addr, error) {
		if host == "timeout.invalid" {
			return nil, &net.DNSError{Err: "i/o timeout", Name: host, IsTimeout: true}
		}
		a, ok := table[host]
		if !ok {
			return nil, &net.DNSError{Err: "no such host", Name: host, IsNotFound: true}
		}
		return []netip.Addr{netip.MustParseAddr(a)}, nil
	})
}

func states(obs []Observation) map[Kind]State {
	m := map[Kind]State{}
	for _, o := range obs {
		m[o.Kind] = o.State
	}
	return m
}

func want(t *testing.T, what string, obs []Observation, unresolvable, refused, cert State) {
	t.Helper()
	if len(obs) != len(ProbeKinds) {
		t.Fatalf("%s: %d observations; every probe reports all %d kinds", what, len(obs), len(ProbeKinds))
	}
	got := states(obs)
	if got[KindUnresolvable] != unresolvable || got[KindRefusedAddress] != refused || got[KindCertificate] != cert {
		t.Errorf("%s: unresolvable=%s refused_address=%s certificate=%s; want %s %s %s\n%+v", what,
			got[KindUnresolvable], got[KindRefusedAddress], got[KindCertificate], unresolvable, refused, cert, obs)
	}
}

func TestProbeTellsSignalFromClearFromUnknown(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.Config.ErrorLog = log.New(io.Discard, "", 0) // the refused handshakes below are the test's own
	srv.StartTLS()
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(srv.Listener.Addr().String())
	roots := x509.NewCertPool()
	roots.AddCert(srv.Certificate())
	// A listener that accepts and says something that is not TLS.
	plain, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	go func() {
		for {
			c, err := plain.Accept()
			if err != nil {
				return
			}
			_, _ = c.Write([]byte("HTTP/1.1 400 Bad Request\r\n\r\n"))
			_ = c.Close()
		}
	}()
	_, plainPort, _ := net.SplitHostPort(plain.Addr().String())
	// A port nothing listens on.
	closed, _ := net.Listen("tcp", "127.0.0.1:0")
	_, closedPort, _ := net.SplitHostPort(closed.Addr().String())
	_ = closed.Close()

	pol := policyFor(t, map[string]string{
		"example.com":       "127.0.0.1", // httptest's certificate names example.com
		"other.invalid":     "127.0.0.1", // and not this name
		"inward.invalid":    "10.1.2.3",
		"metadata.invalid":  "169.254.169.254",
		"untrusted.invalid": "127.0.0.1",
	})
	ctx := context.Background()
	p := Prober{Policy: pol, Roots: roots}

	want(t, "a healthy host", p.Probe(ctx, "example.com:"+port), Clear, Clear, Clear)
	// The certificate is valid, for another name.
	want(t, "a certificate for another name", p.Probe(ctx, "other.invalid:"+port), Clear, Clear, Signal)
	// The certificate's issuer is not trusted.
	want(t, "an untrusted issuer", Prober{Policy: pol, Roots: x509.NewCertPool()}.Probe(ctx, "example.com:"+port), Clear, Clear, Signal)
	// The name is gone. Its address and certificate are then not known.
	want(t, "a name that no longer resolves", p.Probe(ctx, "gone.invalid"), Signal, Unknown, Unknown)
	// The name points inward. It is not dialed.
	want(t, "a name resolving to a private address", p.Probe(ctx, "inward.invalid"), Clear, Signal, Unknown)
	want(t, "a name resolving to the metadata address", p.Probe(ctx, "metadata.invalid"), Clear, Signal, Unknown)
	// The unknown arms: none of these is a signal, and none is clear.
	want(t, "a resolver failure", p.Probe(ctx, "timeout.invalid"), Unknown, Unknown, Unknown)
	want(t, "nothing listening", p.Probe(ctx, "example.com:"+closedPort), Clear, Clear, Unknown)
	want(t, "a port that does not speak TLS", p.Probe(ctx, "example.com:"+plainPort), Clear, Clear, Unknown)

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	for _, o := range p.Probe(cancelled, "example.com:"+port) {
		if o.State == Signal {
			t.Errorf("a cancelled probe reported a signal: %+v", o)
		}
	}
}

// A detail comes partly from outside: it is bounded and has no control
// characters.
func TestDetailIsBounded(t *testing.T) {
	d := detail(errors.New("a\nb\x00c\x1b[31m" + strings.Repeat("é", 2*MaxDetailRunes)))
	if n := len([]rune(d)); n != MaxDetailRunes {
		t.Errorf("detail is %d runes; want %d", n, MaxDetailRunes)
	}
	if strings.ContainsAny(d, "\n\x00\x1b") {
		t.Errorf("detail kept a control character: %q", d[:20])
	}
}

// An IPv6 literal with no port is dialed at [addr]:443, not [[addr]]:443
// (round 2: the certificate check could never run for such a host).
func TestProbeDialsAnIPv6LiteralOnTheDefaultPort(t *testing.T) {
	// The address must be trusted, or the rule refuses it before any dial.
	policy, err := platform.NewOutboundPolicy([]string{"::1/128"})
	if err != nil {
		t.Fatal(err)
	}
	seen := Prober{Policy: policy}.Probe(context.Background(), "[::1]")
	dialed := false
	for _, o := range seen {
		if strings.Contains(o.Detail, "missing port") || strings.Contains(o.Detail, "[[") {
			t.Errorf("the probe built an address that cannot be dialed: %+v", o)
		}
		// Nothing listens on [::1]:443 here: the dial itself is what
		// answers, refused or (on a host with no IPv6) unreachable.
		dialed = dialed || (o.Kind == KindCertificate && o.Detail != "")
	}
	if !dialed {
		t.Errorf("the probe never reached its dial: %+v", seen)
	}
}
