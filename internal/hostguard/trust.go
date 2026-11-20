// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostguard

import (
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"

	"golang.org/x/net/idna"
)

// Distrusted is the list of package repository hosts an operator has decided
// not to use (v0.29.186; summary/70, 4.5, D14). It is read through this
// interface so the package stays a leaf; the store's rows are one
// implementation, a Snapshot another.
type Distrusted interface {
	IsDistrusted(host string) bool
}

// MayUse is THE decision: may this host be fetched from, and may an answer
// it gave earlier be served? Both, or neither: a distrusted host's stored
// answers are no more to be used than new ones. Every place that would do
// either asks here, at the moment of use, before any cache is consulted.
//
// A nil list distrusts nothing. An empty host is never usable: there is
// nothing to decide about.
func MayUse(list Distrusted, host string) bool {
	host = Normalize(host)
	if host == "" {
		return false
	}
	return list == nil || !list.IsDistrusted(host)
}

// defaultPort is the port of an https request that names none. Every fetch
// from a declared repository is https, and so is every shipped one, so a
// host written with it and without it is the same host.
const defaultPort = "443"

// Normalize is the one spelling of a host for this decision, however it was
// written: lower case, no surrounding space, no trailing dot on the name,
// an international name in the ASCII form that is dialed, and the default
// port left off. Any other port is part of it: another port is another
// service, so a distrust of a host covers that host on ONE port. What cannot be read as a name is kept as written, lower case.
func Normalize(host string) string {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	name, port := host, ""
	if h, p, err := net.SplitHostPort(host); err == nil {
		name, port = h, p
	} else if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		name = host[1 : len(host)-1]
	}
	name = strings.TrimSuffix(name, ".")
	if name == "" {
		return ""
	}
	// Spellings that dial the same service are one spelling (0.29.209):
	// an address in its canonical text ([0:0:0:0:0:0:0:1] is [::1]), and a
	// port as a number (":0443" dials 443). Otherwise a distrust is
	// sidestepped by writing the host the other way.
	if addr, err := netip.ParseAddr(name); err == nil {
		// Unmap: [::ffff:192.0.2.1] dials 192.0.2.1 and is that host.
		name = addr.WithZone("").Unmap().String()
	} else if !strings.Contains(name, ":") {
		if ascii, err := idna.Lookup.ToASCII(name); err == nil && ascii != "" {
			name = ascii
		}
	}
	if n, err := strconv.ParseUint(port, 10, 16); err == nil {
		port = strconv.FormatUint(n, 10)
	}
	if port == defaultPort {
		port = ""
	}
	switch {
	case port != "":
		return net.JoinHostPort(name, port)
	case strings.Contains(name, ":"):
		return "[" + name + "]"
	}
	return name
}

// OfURL is the host of a repository URL in that one spelling; empty when
// the text is not a URL with a host. THE way to get a host from a URL for
// anything this package decides: the configuration's list, the console,
// the daily check, the store and the fetch path all use it, so that
// distrusting a host means the host that is fetched from.
func OfURL(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return Normalize(u.Host)
}

// Snapshot is the distrusted list as read once: for one repository's
// analysis or scan, which is the stated bound on how soon a distrust made
// elsewhere takes effect.
type Snapshot map[string]struct{}

// NewSnapshot builds one from host names.
func NewSnapshot(hosts []string) Snapshot {
	s := make(Snapshot, len(hosts))
	for _, h := range hosts {
		if h = Normalize(h); h != "" {
			s[h] = struct{}{}
		}
	}
	return s
}

// IsDistrusted implements Distrusted.
func (s Snapshot) IsDistrusted(host string) bool {
	_, ok := s[Normalize(host)]
	return ok
}

// Allowed filters a list of repository base URLs to those whose host may be
// used, in order: the effective list. hostOf extracts a base's host.
func Allowed(list Distrusted, bases []string, hostOf func(string) string) []string {
	out := make([]string, 0, len(bases))
	for _, b := range bases {
		if MayUse(list, hostOf(b)) {
			out = append(out, b)
		}
	}
	return out
}
