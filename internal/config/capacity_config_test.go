// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/capacity"
)

// v0.29.89 (operator, 2026-10-10; summary/53 §9): each quota may have an
// aveloxis.json line in "capacity" — WEB (the admin Capacity page decides),
// DEFAULT (the shipped value, enforced), SHADOW (shipped, observed only) or
// OFF. A missing line means WEB; an unknown quota or word is refused at load.
func TestCapacityWordsAreReadAndValidated(t *testing.T) {
	write := func(body string) string {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		return p
	}
	cfg, err := Load(write(`{"capacity": {"requests_per_hour": "SHADOW", "repos_per_account": " default ", "signups_per_address_per_day": "off", "requests_per_day": "WEB"}}`))
	if err != nil {
		t.Fatal(err)
	}
	got := cfg.CapacitySources()
	want := map[string]capacity.Source{
		capacity.QuotaRequestsPerHour:         capacity.SourceShadow,
		capacity.QuotaReposPerAccount:         capacity.SourceDefault,
		capacity.QuotaSignupsPerAddressPerDay: capacity.SourceOff,
		capacity.QuotaRequestsPerDay:          capacity.SourceWeb,
		capacity.QuotaTokenRequestsPerHour:    capacity.SourceWeb, // no line: WEB
		capacity.QuotaTokenRequestsPerDay:     capacity.SourceWeb,
		capacity.QuotaRepoLinksPerDay:         capacity.SourceWeb,
	}
	if len(got) != len(want) {
		t.Fatalf("sources = %v; want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q; want %q", k, got[k], v)
		}
	}
	none, err := Load(write(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range capacity.QuotaNames() {
		if src := none.CapacitySources()[name]; src != capacity.SourceWeb {
			t.Errorf("no capacity section: %s = %q; want WEB", name, src)
		}
	}
	for _, bad := range []struct{ json, names string }{
		{`{"capacity": {"requests_per_minute": "WEB"}}`, "requests_per_minute"},
		{`{"capacity": {"requests_per_hour": "ENFORCE"}}`, "requests_per_hour"},
		{`{"capacity": {"requests_per_hour": ""}}`, "requests_per_hour"},
	} {
		if _, err := Load(write(bad.json)); err == nil || !strings.Contains(err.Error(), bad.names) || !strings.Contains(err.Error(), "capacity.") {
			t.Errorf("%s: Load = %v; want a refusal naming capacity.%s", bad.json, err, bad.names)
		}
	}
}

// web.trusted_proxy follows api.trusted_proxy's rule (one validator).
func TestWebTrustedProxyIsValidated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "aveloxis.json")
	for _, c := range []struct {
		value string
		ok    bool
	}{{"127.0.0.1", true}, {"::1", true}, {"", true}, {" 127.0.0.1", false}, {"localhost", false}, {"127.0.0.0/8", false}, {"::ffff:127.0.0.1", false}} {
		if err := os.WriteFile(p, []byte(`{"web": {"trusted_proxy": "`+c.value+`"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(p)
		if c.ok && (err != nil || cfg.Web.TrustedProxy != c.value) {
			t.Errorf("%q: %v", c.value, err)
		}
		if !c.ok && (err == nil || !strings.Contains(err.Error(), "web.trusted_proxy")) {
			t.Errorf("%q = %v; want a refusal naming web.trusted_proxy", c.value, err)
		}
	}
}

// web.signup_escrow_recipient must be an age public key, refused at load
// otherwise (v0.29.89).
func TestSignupEscrowRecipientIsValidated(t *testing.T) {
	p := filepath.Join(t.TempDir(), "aveloxis.json")
	for _, c := range []struct {
		value string
		ok    bool
	}{
		{"", true},
		{"age1ql3z7hjy54pw3hyww5ayyfg7zqgvc7w3j2elw8zmrj2kg5sfn9aqmcac8p", true},
		{"ssh-ed25519 AAAA", false},
		{"age1notakey", false},
	} {
		if err := os.WriteFile(p, []byte(`{"web": {"signup_escrow_recipient": "`+c.value+`"}}`), 0o600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(p)
		if c.ok != (err == nil) || (!c.ok && !strings.Contains(err.Error(), "web.signup_escrow_recipient")) {
			t.Errorf("%q: Load = %v; want ok=%v", c.value, err, c.ok)
		}
	}
}
