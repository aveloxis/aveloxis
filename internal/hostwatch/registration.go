// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostwatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/publicsuffix"
)

// KindRegisteredLater: the host's domain was registered after Aveloxis
// first fetched from the host, the direct sign of a name that lapsed and was
// registered by someone else (v0.29.190; summary/70, 4.5, D14).
const KindRegisteredLater Kind = "registered_later"

// RDAPBootstrapURL is IANA's bootstrap file for domain registration data
// (RFC 9224): for each top-level domain, the registry's RDAP server. The
// servers contacted are therefore not a fixed list, and a domain is
// disclosed only to the registry that already holds it.
const RDAPBootstrapURL = "https://data.iana.org/rdap/dns.json"

// Registration checks registration dates through RDAP (RFC 9082, 9083).
type Registration struct {
	// Get fetches one URL's body. It is the caller's request path: the
	// outbound-address rule on every hop, https only, a bounded body.
	Get func(ctx context.Context, rawURL string) ([]byte, error)
	// NotFound reports Get's "the server has no such object" (404).
	NotFound func(error) bool
	// BootstrapURL is RDAPBootstrapURL unless a test sets it.
	BootstrapURL string

	servers map[string]string      // top-level domain -> RDAP base URL; nil until loaded
	seen    map[string]Observation // registrable domain -> this pass's answer, less its detail's comparison
	dates   map[string]time.Time
}

// RegistrableDomain is the domain a registry holds for host (its public
// suffix plus one label), and false for an address, a single label or a name
// that is itself a public suffix.
func RegistrableDomain(host string) (string, bool) {
	name := strings.ToLower(host)
	if h, _, err := net.SplitHostPort(name); err == nil {
		name = h
	}
	name = strings.TrimSuffix(strings.Trim(name, "[]"), ".")
	if _, err := netip.ParseAddr(name); err == nil || !strings.Contains(name, ".") {
		return "", false
	}
	d, err := publicsuffix.EffectiveTLDPlusOne(name)
	if err != nil {
		return "", false
	}
	return d, true
}

// parseBootstrap reads the bootstrap file into top-level domain -> https
// base URL. An entry with no https server is left out.
func parseBootstrap(body []byte) (map[string]string, error) {
	var doc struct {
		Services [][][]string `json:"services"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, fmt.Errorf("the registration-data bootstrap file is not readable: %w", err)
	}
	out := map[string]string{}
	for _, svc := range doc.Services {
		if len(svc) != 2 {
			continue
		}
		base := ""
		for _, u := range svc[1] {
			if p, err := url.Parse(u); err == nil && p.Scheme == "https" && p.Host != "" && p.User == nil {
				base = strings.TrimSuffix(u, "/") + "/"
				break
			}
		}
		if base == "" {
			continue
		}
		for _, tld := range svc[0] {
			out[strings.ToLower(strings.TrimPrefix(tld, "."))] = base
		}
	}
	if len(out) == 0 {
		return nil, errors.New("the registration-data bootstrap file names no server")
	}
	return out, nil
}

// registrationDate reads the registration event's date from an RDAP domain
// object, and false when it has none.
func registrationDate(body []byte) (time.Time, bool, error) {
	var doc struct {
		Events []struct {
			Action string `json:"eventAction"`
			Date   string `json:"eventDate"`
		} `json:"events"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		return time.Time{}, false, err
	}
	for _, e := range doc.Events {
		if strings.EqualFold(strings.TrimSpace(e.Action), "registration") {
			t, err := time.Parse(time.RFC3339, strings.TrimSpace(e.Date))
			if err != nil {
				return time.Time{}, false, fmt.Errorf("registration date %q: %w", e.Date, err)
			}
			return t, true, nil
		}
	}
	return time.Time{}, false, nil
}

func (r *Registration) unknown(why string) Observation {
	return Observation{Kind: KindRegisteredLater, State: Unknown, Detail: detail(errors.New(why))}
}

// Check says whether host's domain was registered after firstFetched. Every
// way of not knowing is Unknown: an address or a name no registry holds, a
// top-level domain whose registry publishes no registration data, a server
// that does not answer or has no such domain, an object with no
// registration date. A domain is asked once per Registration value, however
// many hosts share it.
func (r *Registration) Check(ctx context.Context, host string, firstFetched time.Time) Observation {
	domain, ok := RegistrableDomain(host)
	if !ok {
		return r.unknown("not a name a registry holds")
	}
	registered, known := r.dates[domain]
	if !known {
		if o, failed := r.seen[domain]; failed {
			return o
		}
		var o Observation
		registered, o = r.lookup(ctx, domain)
		if o.State == Unknown {
			if ctx.Err() == nil { // an answer cut short by shutdown is not this pass's answer
				if r.seen == nil {
					r.seen = map[string]Observation{}
				}
				r.seen[domain] = o
			}
			return o
		}
		if r.dates == nil {
			r.dates = map[string]time.Time{}
		}
		r.dates[domain] = registered
	}
	if registered.After(firstFetched) {
		return Observation{Kind: KindRegisteredLater, State: Signal, Detail: fmt.Sprintf("%s was registered on %s, after the first fetch from this host on %s",
			domain, registered.UTC().Format("2006-01-02"), firstFetched.UTC().Format("2006-01-02"))}
	}
	return Observation{Kind: KindRegisteredLater, State: Clear}
}

// lookup asks the registry for domain's registration date. Its Observation
// is Unknown with the reason, or the zero value with the date.
func (r *Registration) lookup(ctx context.Context, domain string) (time.Time, Observation) {
	if r.servers == nil {
		src := r.BootstrapURL
		if src == "" {
			src = RDAPBootstrapURL
		}
		body, err := r.Get(ctx, src)
		if err != nil {
			return time.Time{}, r.unknown("the registration-data bootstrap file could not be fetched: " + err.Error())
		}
		servers, err := parseBootstrap(body)
		if err != nil {
			return time.Time{}, r.unknown(err.Error())
		}
		r.servers = servers
	}
	tld := domain[strings.LastIndexByte(domain, '.')+1:]
	base, ok := r.servers[tld]
	if !ok {
		return time.Time{}, r.unknown("the registry of ." + tld + " publishes no registration data")
	}
	body, err := r.Get(ctx, base+"domain/"+url.PathEscape(domain))
	if err != nil {
		if r.NotFound != nil && r.NotFound(err) {
			return time.Time{}, r.unknown("the registry has no record of " + domain)
		}
		return time.Time{}, r.unknown("the registry did not answer for " + domain + ": " + err.Error())
	}
	at, has, err := registrationDate(body)
	if err != nil {
		return time.Time{}, r.unknown("the registry's answer for " + domain + " is not readable: " + err.Error())
	}
	if !has {
		return time.Time{}, r.unknown("the registry's record of " + domain + " carries no registration date")
	}
	return at, Observation{}
}
