package db

import (
	"context"
	"testing"
)

func TestExtractDomain(t *testing.T) {
	tests := []struct {
		email string
		want  string
	}{
		{"user@example.com", "example.com"},
		{"user@UPPER.COM", "upper.com"},
		{"user@sub.domain.org", "sub.domain.org"},
		{"user@", ""},
		{"noatsign", ""},
		{"", ""},
		{"@domain.com", "domain.com"},
		{"user@a", "a"},
	}

	for _, tt := range tests {
		got := extractDomain(tt.email)
		if got != tt.want {
			t.Errorf("extractDomain(%q) = %q, want %q", tt.email, got, tt.want)
		}
	}
}

func TestAffiliationResolver_ResolveWithPreloadedCache(t *testing.T) {
	// Test the in-memory resolution logic without a database.
	r := &AffiliationResolver{
		cache: map[string]string{
			"redhat.com":   "Red Hat",
			"google.com":   "Google",
			"mozilla.org":  "Mozilla",
		},
		loaded: true,
	}

	tests := []struct {
		email string
		want  string
	}{
		{"dev@redhat.com", "Red Hat"},
		{"eng@google.com", "Google"},
		{"hacker@mozilla.org", "Mozilla"},
		{"user@unknown.com", ""},
		{"", ""},
		// Parent domain matching: mail.google.com -> google.com
		{"user@mail.google.com", "Google"},
		{"user@corp.redhat.com", "Red Hat"},
		// No match for bare TLDs.
		{"user@com", ""},
	}

	for _, tt := range tests {
		got := r.Resolve(context.TODO(), tt.email)
		if got != tt.want {
			t.Errorf("Resolve(%q) = %q, want %q", tt.email, got, tt.want)
		}
	}
}
