package db

import (
	"testing"
)

// TestExtractDomain verifies email domain extraction (already exists, testing edge cases).
func TestExtractDomainEdgeCases(t *testing.T) {
	tests := []struct {
		email string
		want  string
	}{
		{"user@example.com", "example.com"},
		{"user@mail.example.com", "mail.example.com"},
		{"user@EXAMPLE.COM", "example.com"},
		{"", ""},
		{"no-at-sign", ""},
		{"@", ""},
		{"user@", ""},
	}
	for _, tt := range tests {
		got := extractDomain(tt.email)
		if got != tt.want {
			t.Errorf("extractDomain(%q) = %q, want %q", tt.email, got, tt.want)
		}
	}
}

// TestNormalizeCompany verifies company name normalization.
func TestNormalizeCompany(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Red Hat", "Red Hat"},
		{"@microsoft", "Microsoft"},
		{"@esnet @chaoss", "Esnet"},
		{"  Red Hat, Inc.  ", "Red Hat, Inc."},
		{"", ""},
		{"None", ""},
		{"none", ""},
		{"N/A", ""},
	}
	for _, tt := range tests {
		got := normalizeCompany(tt.input)
		if got != tt.want {
			t.Errorf("normalizeCompany(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// TestBuildAffiliationMap verifies building domain->company mapping from contributor data.
func TestBuildAffiliationMap(t *testing.T) {
	rows := []affiliationCandidate{
		{Email: "jberkus@redhat.com", Company: "Red Hat"},
		{Email: "jstrunk@redhat.com", Company: "Red Hat"},
		{Email: "suuus@github.com", Company: "@microsoft"},
		{Email: "user@gmail.com", Company: "Google"},
		{Email: "alice@example.com", Company: ""},
		{Email: "", Company: "Unknown Corp"},
	}

	m := buildAffiliationMap(rows)

	// redhat.com should map to "Red Hat" (two contributors agree).
	if aff, ok := m["redhat.com"]; !ok || aff != "Red Hat" {
		t.Errorf("redhat.com = %q, want %q", aff, "Red Hat")
	}

	// github.com should map to "Microsoft" (normalized from @microsoft).
	if aff, ok := m["github.com"]; !ok || aff != "Microsoft" {
		t.Errorf("github.com = %q, want %q", aff, "Microsoft")
	}

	// gmail.com should be excluded — it's a public email provider.
	if _, ok := m["gmail.com"]; ok {
		t.Error("gmail.com should be excluded as public provider")
	}

	// example.com should not be present — no company data.
	if _, ok := m["example.com"]; ok {
		t.Error("example.com should not appear — contributor has no company")
	}
}

// TestPublicEmailDomains verifies that common free email providers are excluded.
func TestPublicEmailDomains(t *testing.T) {
	domains := []string{
		"gmail.com", "yahoo.com", "hotmail.com", "outlook.com",
		"protonmail.com", "qq.com", "163.com", "126.com",
	}
	for _, d := range domains {
		if !isPublicEmailDomain(d) {
			t.Errorf("isPublicEmailDomain(%q) = false, want true", d)
		}
	}

	corporateDomains := []string{
		"redhat.com", "microsoft.com", "google.com", "bitergia.com",
	}
	for _, d := range corporateDomains {
		if isPublicEmailDomain(d) {
			t.Errorf("isPublicEmailDomain(%q) = true, want false", d)
		}
	}
}

// TestBuildAffiliationMapDomainConsensus verifies that when multiple
// contributors with the same domain disagree on company, the most common wins.
func TestBuildAffiliationMapDomainConsensus(t *testing.T) {
	rows := []affiliationCandidate{
		{Email: "a@corp.com", Company: "Corp Inc"},
		{Email: "b@corp.com", Company: "Corp Inc"},
		{Email: "c@corp.com", Company: "Other Corp"},
	}

	m := buildAffiliationMap(rows)

	if aff, ok := m["corp.com"]; !ok || aff != "Corp Inc" {
		t.Errorf("corp.com = %q, want %q (majority rule)", aff, "Corp Inc")
	}
}
