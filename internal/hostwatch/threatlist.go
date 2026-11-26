// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostwatch

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strings"
)

// KindThreatList means the host, or a domain above it, is on a public threat
// list (v0.29.191; summary/70, 4.5, D14). The list is downloaded whole and
// matched here, so no repository host is disclosed to its publisher. It is a
// hint and never a verdict: the plan's sample of 82 hosts had one hit, a
// shared cloud-storage endpoint.
const KindThreatList Kind = "threat_list"

// ThreatCandidates are the names under which host would be listed: the host
// itself, without its port, and each domain above it down to the one a
// registry holds. A list entry for evil.example covers repo.evil.example; an
// entry for other.evil.example does not; nothing above the registrable
// domain is a candidate, so a listed suffix shared by strangers is not a
// match.
func ThreatCandidates(host string) []string {
	name := strings.ToLower(host)
	if h, _, err := net.SplitHostPort(name); err == nil {
		name = h
	}
	name = strings.TrimSuffix(strings.Trim(name, "[]"), ".")
	if name == "" {
		return nil
	}
	out := []string{name}
	if _, err := netip.ParseAddr(name); err == nil {
		return out
	}
	stop, ok := RegistrableDomain(name)
	if !ok {
		return out
	}
	for name != stop {
		_, rest, cut := strings.Cut(name, ".")
		if !cut {
			break
		}
		name = rest
		out = append(out, name)
	}
	return out
}

// threatEntry reads one line of a list: a host name, or a hosts-file line
// (an address, then the name). Blank lines and # comments are "".
func threatEntry(line string) string {
	if i := strings.IndexByte(line, '#'); i >= 0 {
		line = line[:i]
	}
	f := strings.Fields(line)
	switch len(f) {
	case 1:
	case 2:
		if _, err := netip.ParseAddr(f[0]); err != nil {
			return ""
		}
		f = f[1:]
	default:
		return ""
	}
	e := strings.TrimSuffix(strings.ToLower(f[0]), ".")
	for _, r := range e {
		// A host name's characters (an address's colon included). A line of
		// markup or a URL is not an entry.
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_' || r == ':') {
			return ""
		}
	}
	// An entry is an address, or a name: labels separated by dots, the
	// last a top-level domain (letters, or an xn-- label). Anything else
	// (a word, `v1.2`, `error:`, a timestamp) is not told apart from an
	// error body, and a document of those must not read as a list that
	// names nobody (0.29.208, tightened in 0.29.209).
	if addr, err := netip.ParseAddr(e); err == nil {
		// In the text a host is spelled in (hostguard.Normalize), so an
		// address written out in full matches the host that has it.
		return addr.Unmap().String()
	}
	labels := strings.Split(e, ".")
	if len(labels) < 2 || strings.Contains(e, ":") {
		return ""
	}
	for _, l := range labels {
		if l == "" {
			return ""
		}
	}
	tld := labels[len(labels)-1]
	if !strings.HasPrefix(tld, "xn--") {
		for _, r := range tld {
			if r < 'a' || r > 'z' {
				return ""
			}
		}
	}
	return e
}

// ThreatMatches is one reading of the list against a set of hosts.
type ThreatMatches struct {
	hit     map[string]string // host -> the entry that matched
	entries int
}

// Entries is the number of entries the list held.
func (m *ThreatMatches) Entries() int { return m.entries }

// MatchThreatList reads the list from r and remembers which of hosts it
// names. The list is streamed: only the hosts' candidates are held. A list
// with no entry is an error (an empty or wrong document is not "nothing is
// listed").
func MatchThreatList(r io.Reader, hosts []string) (*ThreatMatches, error) {
	want := map[string][]string{} // candidate -> the hosts it stands for
	for _, h := range hosts {
		for _, c := range ThreatCandidates(h) {
			want[c] = append(want[c], h)
		}
	}
	m := &ThreatMatches{hit: map[string]string{}}
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		e := threatEntry(sc.Text())
		if e == "" {
			continue
		}
		m.entries++
		for _, h := range want[e] {
			if _, already := m.hit[h]; !already {
				m.hit[h] = e
			}
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("the threat list could not be read to its end: %w", err)
	}
	if m.entries == 0 {
		return nil, errors.New("the threat list holds no entry")
	}
	return m, nil
}

// Check is the list's answer for one of the hosts it was matched against.
func (m *ThreatMatches) Check(host string) Observation {
	if e, ok := m.hit[host]; ok {
		return Observation{Kind: KindThreatList, State: Signal, Detail: detail(fmt.Errorf("listed as %s (a hint, not a verdict: lists name shared hosting too)", e))}
	}
	return Observation{Kind: KindThreatList, State: Clear}
}

// ThreatListUnknown is the answer for every host when the list could not be
// obtained or read.
func ThreatListUnknown(err error) Observation {
	return Observation{Kind: KindThreatList, State: Unknown, Detail: detail(err)}
}
