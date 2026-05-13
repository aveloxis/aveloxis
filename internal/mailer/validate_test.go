package mailer

import (
	"strings"
	"testing"
)

// v0.20.14 mailer validation: production diagnostic on 2026-05-13
// — an operator hit `535 5.7.8 Username and Password not accepted`
// on first signup. Root cause was two config mistakes the mailer
// didn't catch at load time:
//   1. gmail_user was set to the bare domain "aveloxis.io"
//      (Gmail SMTP needs the full email address).
//   2. gmail_app_password was set to a regular password containing
//      special characters and uppercase letters — Google App
//      Passwords are exactly 16 lowercase ASCII letters.
//
// The first signal of failure was a confusing SMTP error in the
// production log after a new user signed up. ValidateConfig
// catches both shapes at config-load time so operators see the
// problem in `aveloxis web` startup logs instead.

func TestValidateConfig_AcceptsValidEmailAndAppPassword(t *testing.T) {
	cfg := Config{
		GmailUser:        "ops@example.com",
		GmailAppPassword: "abcd efgh ijkl mnop", // Google's display format with spaces
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Errorf("ValidateConfig must accept a full email and 16 lowercase letters (with or without display-format spaces). Got: %v", err)
	}
}

func TestValidateConfig_AcceptsAppPasswordWithoutSpaces(t *testing.T) {
	cfg := Config{
		GmailUser:        "ops@example.com",
		GmailAppPassword: "abcdefghijklmnop", // same password, spaces stripped
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Errorf("ValidateConfig must accept App Passwords without the display-format spaces. Got: %v", err)
	}
}

func TestValidateConfig_AcceptsEmptyConfigAsDisabled(t *testing.T) {
	// Empty user means the mailer is disabled — that's a valid
	// state, not a validation failure. The rest of the app must
	// continue to work without email.
	cfg := Config{
		GmailUser:        "",
		GmailAppPassword: "",
	}
	if err := ValidateConfig(cfg); err != nil {
		t.Errorf("ValidateConfig must treat a fully-empty mail block as 'mailer disabled' rather than a validation failure. Got: %v", err)
	}
}

func TestValidateConfig_RejectsBareDomainAsUser(t *testing.T) {
	// The exact production mistake from 2026-05-13: gmail_user
	// set to the domain instead of the full email address.
	cfg := Config{
		GmailUser:        "aveloxis.io",
		GmailAppPassword: "abcdefghijklmnop",
	}
	err := ValidateConfig(cfg)
	if err == nil {
		t.Fatal("ValidateConfig must reject gmail_user='aveloxis.io' (no @ — that's a domain, not an email). Without this check, operators learn about the misconfiguration only when the first user signs up and hits a confusing SMTP 535 error.")
	}
	if !strings.Contains(err.Error(), "@") && !strings.Contains(err.Error(), "email") {
		t.Errorf("validation error must mention the missing @ or 'email' so the operator knows what to fix. Got: %v", err)
	}
}

func TestValidateConfig_RejectsWrongLengthPassword(t *testing.T) {
	// Production case: a regular Gmail password rather than an
	// App Password.
	cfg := Config{
		GmailUser:        "ops@example.com",
		GmailAppPassword: "}r?73%uP;RYA~A^",
	}
	err := ValidateConfig(cfg)
	if err == nil {
		t.Fatal("ValidateConfig must reject a 15-character password with special characters. App Passwords are exactly 16 lowercase ASCII letters.")
	}
}

func TestValidateConfig_RejectsNonLowercaseLetterPassword(t *testing.T) {
	// 16 characters but with uppercase / digits / specials —
	// not an App Password.
	cfg := Config{
		GmailUser:        "ops@example.com",
		GmailAppPassword: "ABCDEFGHIJKLMNOP",
	}
	err := ValidateConfig(cfg)
	if err == nil {
		t.Fatal("ValidateConfig must reject an all-uppercase 16-character password — App Passwords contain only lowercase ASCII letters.")
	}
}

func TestValidateConfig_RejectsPartialConfigOnlyUser(t *testing.T) {
	// One field set, the other not — almost certainly a mistake.
	cfg := Config{
		GmailUser:        "ops@example.com",
		GmailAppPassword: "",
	}
	err := ValidateConfig(cfg)
	if err == nil {
		t.Error("ValidateConfig must reject a partial config (gmail_user set but gmail_app_password empty). Either both are set (mailer enabled) or both are empty (mailer disabled).")
	}
}

func TestValidateConfig_RejectsPartialConfigOnlyPassword(t *testing.T) {
	cfg := Config{
		GmailUser:        "",
		GmailAppPassword: "abcdefghijklmnop",
	}
	err := ValidateConfig(cfg)
	if err == nil {
		t.Error("ValidateConfig must reject a partial config (gmail_app_password set but gmail_user empty). Either both are set or both are empty.")
	}
}

// TestNormalizedAppPassword pins that the Send path strips the
// display-format spaces before passing to PlainAuth. Operators
// copy-paste from Google's UI which inserts spaces; the actual
// auth token is the 16 contiguous characters.
func TestNormalizedAppPassword_StripsSpaces(t *testing.T) {
	got := normalizeAppPassword("abcd efgh ijkl mnop")
	want := "abcdefghijklmnop"
	if got != want {
		t.Errorf("normalizeAppPassword(%q) = %q, want %q — Gmail's UI displays App Passwords with spaces every 4 chars but the auth token is the contiguous 16 chars. Without stripping, smtp.PlainAuth sends the spaced version and Gmail rejects with 535.", "abcd efgh ijkl mnop", got, want)
	}
}
