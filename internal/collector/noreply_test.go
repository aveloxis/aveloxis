package collector

import "testing"

func TestParseNoreplyEmail_WithID(t *testing.T) {
	info := ParseNoreplyEmail("12345+octocat@users.noreply.github.com")
	if info == nil {
		t.Fatal("expected non-nil result")
	}
	if info.Login != "octocat" {
		t.Errorf("Login = %q, want %q", info.Login, "octocat")
	}
	if info.UserID != 12345 {
		t.Errorf("UserID = %d, want 12345", info.UserID)
	}
	if !info.HasID {
		t.Error("HasID should be true")
	}
}

func TestParseNoreplyEmail_WithoutID(t *testing.T) {
	info := ParseNoreplyEmail("octocat@users.noreply.github.com")
	if info == nil {
		t.Fatal("expected non-nil result")
	}
	if info.Login != "octocat" {
		t.Errorf("Login = %q, want %q", info.Login, "octocat")
	}
	if info.UserID != 0 {
		t.Errorf("UserID = %d, want 0", info.UserID)
	}
	if info.HasID {
		t.Error("HasID should be false")
	}
}

func TestParseNoreplyEmail_NotNoreply(t *testing.T) {
	tests := []string{
		"user@example.com",
		"user@github.com",
		"",
		"not-an-email",
		"@users.noreply.github.com",   // no login
		"user@noreply.github.com",      // wrong subdomain
	}
	for _, email := range tests {
		if info := ParseNoreplyEmail(email); info != nil {
			t.Errorf("ParseNoreplyEmail(%q) should be nil, got %+v", email, info)
		}
	}
}

func TestParseNoreplyEmail_LargeUserID(t *testing.T) {
	info := ParseNoreplyEmail("29740296+chaossbot@users.noreply.github.com")
	if info == nil {
		t.Fatal("expected non-nil")
	}
	if info.UserID != 29740296 {
		t.Errorf("UserID = %d, want 29740296", info.UserID)
	}
	if info.Login != "chaossbot" {
		t.Errorf("Login = %q, want %q", info.Login, "chaossbot")
	}
}

func TestParseNoreplyEmail_Whitespace(t *testing.T) {
	info := ParseNoreplyEmail("  12345+user@users.noreply.github.com  ")
	if info == nil {
		t.Fatal("expected non-nil after trimming whitespace")
	}
	if info.Login != "user" {
		t.Errorf("Login = %q, want %q", info.Login, "user")
	}
}

func TestIsNoreplyEmail(t *testing.T) {
	if !IsNoreplyEmail("user@users.noreply.github.com") {
		t.Error("should recognize noreply email")
	}
	if !IsNoreplyEmail("12345+user@USERS.NOREPLY.GITHUB.COM") {
		t.Error("should be case-insensitive")
	}
	if IsNoreplyEmail("user@example.com") {
		t.Error("should not match regular email")
	}
}

func TestIsBotEmail(t *testing.T) {
	tests := []struct {
		email string
		want  bool
	}{
		{"dependabot[bot]@users.noreply.github.com", true},
		{"noreply@github.com", true},
		{"actions@github.com", true},
		{"user@users.noreply.github.com", false}, // noreply but the GitHub kind
		{"user@example.com", false},
		{"12345+user@users.noreply.github.com", false},
	}
	for _, tt := range tests {
		got := IsBotEmail(tt.email)
		if got != tt.want {
			t.Errorf("IsBotEmail(%q) = %v, want %v", tt.email, got, tt.want)
		}
	}
}
