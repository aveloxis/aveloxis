// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package hostwatch looks at a package repository host and says what it
// saw (v0.29.189; summary/70, 4.5, D14). It decides nothing and changes
// nothing (SR-7): an observation is a line on the administrator's list, and
// the administrator distrusts a host or does not.
//
// Every check has three answers (SR-16). Signal: the thing was seen. Clear:
// it was looked for and is not there. Unknown: the check could not tell (a
// timeout, a refused connection, a resolver failure). Unknown is never
// "clear" and never a signal.
package hostwatch

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aveloxis/aveloxis/internal/platform"
)

// Kind names one thing that is watched for.
type Kind string

const (
	// KindUnresolvable: the name no longer resolves.
	KindUnresolvable Kind = "unresolvable"
	// KindRefusedAddress: the name resolves to an address the
	// outbound-address rule refuses, which is what a taken-over name
	// pointed inward looks like.
	KindRefusedAddress Kind = "refused_address"
	// KindCertificate: the host's certificate is not valid for it.
	KindCertificate Kind = "certificate"
)

// ProbeKinds are the kinds Probe reports, always all of them and in this
// order.
var ProbeKinds = []Kind{KindUnresolvable, KindRefusedAddress, KindCertificate}

// State is a check's answer.
type State string

// The three answers.
const (
	Signal  State = "signal"
	Clear   State = "clear"
	Unknown State = "unknown"
)

// Observation is one check's answer about one host.
type Observation struct {
	Kind   Kind
	State  State
	Detail string // why, for a signal or an unknown; bounded (MaxDetailRunes)
}

// MaxDetailRunes bounds an observation's detail, which is partly an error's
// text from outside.
const MaxDetailRunes = 300

// ProbeTimeout bounds one host's probe: the resolution, the connection and
// the handshake together. It is net/http's own default bound on a TLS
// handshake (http.DefaultTransport.TLSHandshakeTimeout), which a host that
// serves poms meets.
const ProbeTimeout = 10 * time.Second

// Prober probes hosts under the outbound-address rule.
type Prober struct {
	// Policy is the rule (nil: strict). Its own resolver and dialer are
	// used, so what was judged is what is dialed.
	Policy *platform.OutboundPolicy
	// Roots are the certificate authorities trusted; nil is the system's.
	Roots *x509.CertPool
}

func detail(err error) string {
	s := strings.ToValidUTF8(err.Error(), "?")
	s = strings.Map(func(r rune) rune {
		if r < ' ' || r == 0x7f {
			return ' '
		}
		return r
	}, s)
	if utf8.RuneCountInString(s) > MaxDetailRunes {
		s = string([]rune(s)[:MaxDetailRunes])
	}
	return s
}

// Probe resolves host, judges its addresses, and completes a TLS handshake
// with it on its port (443 when the host names none). It reports every kind
// of ProbeKinds.
func (p Prober) Probe(ctx context.Context, host string) []Observation {
	ctx, cancel := context.WithTimeout(ctx, ProbeTimeout)
	defer cancel()
	unresolvable := Observation{Kind: KindUnresolvable, State: Clear}
	refused := Observation{Kind: KindRefusedAddress, State: Clear}
	cert := Observation{Kind: KindCertificate, State: Unknown}
	out := func() []Observation { return []Observation{unresolvable, refused, cert} }

	if err := p.Policy.RefuseHost(ctx, host); err != nil {
		var dns *net.DNSError
		switch {
		// The rule's one refusal: link-local and numeric-host refusals
		// wrap it.
		case errors.Is(err, platform.ErrPrivateAddress):
			refused.State, refused.Detail = Signal, detail(err)
			cert.Detail = "not dialed: the address is refused"
		case errors.As(err, &dns) && dns.IsNotFound:
			unresolvable.State, unresolvable.Detail = Signal, detail(err)
			refused.State, refused.Detail = Unknown, "the name does not resolve"
			cert.Detail = "the name does not resolve"
		default:
			// A resolver timeout or failure says nothing about the host.
			d := detail(err)
			unresolvable.State, unresolvable.Detail = Unknown, d
			refused.State, refused.Detail = Unknown, d
			cert.Detail = d
		}
		return out()
	}

	address := host
	var name string
	if h, _, err := net.SplitHostPort(host); err == nil {
		name = h
	} else {
		// No port. An IPv6 literal is spelled in brackets with none
		// ("[2001:db8::1]"); JoinHostPort adds its own, and bracketing
		// twice gave an address nothing can dial (round 2).
		name = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
		address = net.JoinHostPort(name, "443")
	}
	conn, err := p.Policy.DialContext(ctx, "tcp", address)
	if err != nil {
		cert.Detail = detail(err) // down, filtered, or refused between the two looks: not known
		return out()
	}
	defer conn.Close()
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	}
	tc := tls.Client(conn, &tls.Config{ServerName: strings.Trim(name, "[]"), RootCAs: p.Roots, MinVersion: tls.VersionTLS12})
	err = tc.HandshakeContext(ctx)
	var verify *tls.CertificateVerificationError
	switch {
	case err == nil:
		cert.State = Clear
	case errors.As(err, &verify):
		cert.State, cert.Detail = Signal, detail(err)
	default:
		cert.Detail = detail(err) // not TLS on this port, a reset, a timeout: not known
	}
	return out()
}
