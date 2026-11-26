// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package hostwatch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRegistrableDomain(t *testing.T) {
	for host, want := range map[string]string{
		"repo.example.com":           "example.com",
		"Repo.Example.COM.:8443":     "example.com",
		"maven.pkg.example.co.uk":    "example.co.uk",
		"example.com":                "example.com",
		"127.0.0.1":                  "",
		"[2001:db8::1]:443":          "",
		"localhost":                  "",
		"co.uk":                      "",
		"":                           "",
		"repo.jenkins-ci.org:443":    "jenkins-ci.org",
		"oss.sonatype.org":           "sonatype.org",
		"something.s3.amazonaws.com": "something.s3.amazonaws.com",
	} {
		got, ok := RegistrableDomain(host)
		if got != want || ok != (want != "") {
			t.Errorf("RegistrableDomain(%q) = %q, %v; want %q", host, got, ok, want)
		}
	}
}

var errNotFound = errors.New("404")

const bootstrapDoc = `{"services":[
 [["com","net"],["http://insecure.example/","https://rdap.registry.example/v1"]],
 [["org"],["https://rdap.org-registry.example/"]],
 [["plain"],["http://only-plain.example/"]]
]}`

func rdap(date string) string {
	return `{"objectClassName":"domain","events":[{"eventAction":"last changed","eventDate":"2026-06-01T00:00:00Z"},{"eventAction":"registration","eventDate":"` + date + `"}]}`
}

func TestRegistrationCheck(t *testing.T) {
	first := time.Date(2025, 3, 1, 0, 0, 0, 0, time.UTC)
	var asked []string
	answers := map[string]any{
		"https://bootstrap.example/dns.json":                      bootstrapDoc,
		"https://rdap.registry.example/v1/domain/lapsed.com":      rdap("2026-02-02T10:00:00Z"),
		"https://rdap.registry.example/v1/domain/old.com":         rdap("2009-05-05T00:00:00Z"),
		"https://rdap.registry.example/v1/domain/gone.com":        errNotFound,
		"https://rdap.registry.example/v1/domain/down.com":        errors.New("timeout"),
		"https://rdap.registry.example/v1/domain/nodate.com":      `{"events":[{"eventAction":"expiration","eventDate":"2030-01-01T00:00:00Z"}]}`,
		"https://rdap.registry.example/v1/domain/garbage.com":     `<html>`,
		"https://rdap.registry.example/v1/domain/baddate.com":     rdap("yesterday"),
		"https://rdap.org-registry.example/domain/jenkins-ci.org": rdap("2011-01-01T00:00:00Z"),
	}
	r := &Registration{BootstrapURL: "https://bootstrap.example/dns.json", NotFound: func(err error) bool { return errors.Is(err, errNotFound) },
		Get: func(_ context.Context, u string) ([]byte, error) {
			asked = append(asked, u)
			switch a := answers[u].(type) {
			case string:
				return []byte(a), nil
			case error:
				return nil, a
			}
			return nil, errors.New("unexpected request " + u)
		}}
	ctx := context.Background()
	check := func(host string, want State, detailHas string) {
		t.Helper()
		o := r.Check(ctx, host, first)
		if o.Kind != KindRegisteredLater || o.State != want || !strings.Contains(o.Detail, detailHas) {
			t.Errorf("%s: %+v; want %s with %q", host, o, want, detailHas)
		}
	}
	// Registered after the first fetch: the name changed hands.
	check("repo.lapsed.com", Signal, "lapsed.com was registered on 2026-02-02, after the first fetch from this host on 2025-03-01")
	check("repo.old.com:8443", Clear, "")
	check("repo.jenkins-ci.org", Clear, "")
	// Every way of not knowing is "not known": never clear, never a signal.
	check("repo.gone.com", Unknown, "no record of gone.com")
	check("repo.down.com", Unknown, "did not answer")
	check("repo.nodate.com", Unknown, "no registration date")
	check("repo.garbage.com", Unknown, "not readable")
	check("repo.baddate.com", Unknown, "not readable")
	check("repo.example.dev", Unknown, "the registry of .dev publishes no registration data")
	check("repo.example.plain", Unknown, "publishes no registration data") // its only server is not https
	check("10.0.0.7", Unknown, "not a name a registry holds")

	// The bootstrap file once, and each domain once, however many hosts
	// share it: the https server, never the plain one.
	check("maven.lapsed.com", Signal, "lapsed.com")
	check("other.down.com", Unknown, "did not answer")
	count := map[string]int{}
	for _, u := range asked {
		count[u]++
		if strings.HasPrefix(u, "http://") {
			t.Errorf("asked a plain-http server: %s", u)
		}
	}
	for u, n := range count {
		if n != 1 {
			t.Errorf("%s was asked %d times in one pass", u, n)
		}
	}
	// A registration at the very moment of the first fetch is not "after".
	if o := r.Check(ctx, "repo.lapsed.com", time.Date(2026, 2, 2, 10, 0, 0, 0, time.UTC)); o.State != Clear {
		t.Errorf("registered at the instant of the first fetch: %+v", o)
	}
}

// Without the bootstrap file nothing is known, and no registry is asked.
func TestRegistrationCheckWithoutTheBootstrapFile(t *testing.T) {
	for name, body := range map[string]any{"unfetchable": errors.New("refused"), "unreadable": "nope", "empty": `{"services":[]}`} {
		n := 0
		r := &Registration{Get: func(_ context.Context, u string) ([]byte, error) {
			n++
			if u != RDAPBootstrapURL {
				t.Errorf("%s: asked %s", name, u)
			}
			if err, ok := body.(error); ok {
				return nil, err
			}
			return []byte(body.(string)), nil
		}}
		if o := r.Check(context.Background(), "repo.example.com", time.Now()); o.State != Unknown || n != 1 {
			t.Errorf("%s: %+v after %d requests; want unknown after the one", name, o, n)
		}
	}
}

// An answer cut short by shutdown is not kept as the pass's answer.
func TestRegistrationCheckDoesNotKeepAShutdownAnswer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &Registration{BootstrapURL: "b", Get: func(_ context.Context, u string) ([]byte, error) {
		if u == "b" {
			return []byte(bootstrapDoc), nil
		}
		cancel()
		return nil, context.Canceled
	}}
	if o := r.Check(ctx, "repo.x.com", time.Now()); o.State != Unknown {
		t.Fatalf("%+v", o)
	}
	if len(r.seen) != 0 {
		t.Errorf("a shutdown answer was kept: %+v", r.seen)
	}
}
