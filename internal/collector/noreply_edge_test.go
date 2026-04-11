package collector

import (
	"testing"
)

// ============================================================
// ParseNoreplyEmail edge cases (beyond noreply_test.go coverage)
// ============================================================

func TestParseNoreplyEmail_Empty(t *testing.T) {
	if info := ParseNoreplyEmail(""); info != nil {
		t.Error("empty should return nil")
	}
}

func TestParseNoreplyEmail_HyphenInLogin(t *testing.T) {
	info := ParseNoreplyEmail("my-user-name@users.noreply.github.com")
	if info == nil {
		t.Fatal("hyphens in login should be valid")
	}
	if info.Login != "my-user-name" {
		t.Errorf("Login = %q", info.Login)
	}
}

func TestParseNoreplyEmail_DotInLogin(t *testing.T) {
	info := ParseNoreplyEmail("user.name@users.noreply.github.com")
	if info == nil {
		t.Fatal("dots in login should be valid")
	}
	if info.Login != "user.name" {
		t.Errorf("Login = %q", info.Login)
	}
}

func TestParseNoreplyEmail_UnderscoreInLogin(t *testing.T) {
	info := ParseNoreplyEmail("user_name@users.noreply.github.com")
	if info == nil {
		t.Fatal("underscores should be valid")
	}
	if info.Login != "user_name" {
		t.Errorf("Login = %q", info.Login)
	}
}

func TestParseNoreplyEmail_InvalidDomain(t *testing.T) {
	if info := ParseNoreplyEmail("user@users.noreply.gitlab.com"); info != nil {
		t.Error("gitlab noreply should not match")
	}
}

func TestParseNoreplyEmail_NoAtSign(t *testing.T) {
	if info := ParseNoreplyEmail("not-an-email"); info != nil {
		t.Error("should return nil for non-email")
	}
}

// ============================================================
// IsNoreplyEmail edge cases (beyond noreply_test.go coverage)
// ============================================================

func TestIsNoreplyEmail_CaseMixed(t *testing.T) {
	if !IsNoreplyEmail("user@Users.Noreply.GitHub.com") {
		t.Error("should be case insensitive")
	}
}

func TestIsNoreplyEmail_EmptyString(t *testing.T) {
	if IsNoreplyEmail("") {
		t.Error("empty is not noreply")
	}
}

func TestIsNoreplyEmail_WhitespaceAround(t *testing.T) {
	if !IsNoreplyEmail("  user@users.noreply.github.com  ") {
		t.Error("should trim whitespace")
	}
}

// ============================================================
// IsBotEmail edge cases (beyond noreply_test.go coverage)
// ============================================================

func TestIsBotEmail_BotBracketCase(t *testing.T) {
	if !IsBotEmail("Dependabot[BOT]@users.noreply.github.com") {
		t.Error("[BOT] (uppercase) should be detected")
	}
}

func TestIsBotEmail_GenericNoreply(t *testing.T) {
	if !IsBotEmail("noreply@company.com") {
		t.Error("generic noreply should be bot")
	}
}

func TestIsBotEmail_GitHubSystemEmail(t *testing.T) {
	if !IsBotEmail("actions@github.com") {
		t.Error("actions@github.com should be bot")
	}
}

func TestIsBotEmail_UserNoreplyNotBot(t *testing.T) {
	// GitHub user noreply emails are real users, not bots.
	if IsBotEmail("user@users.noreply.github.com") {
		t.Error("GitHub user noreply should NOT be bot")
	}
}

func TestIsBotEmail_RegularEmail(t *testing.T) {
	if IsBotEmail("dev@example.com") {
		t.Error("regular email is not bot")
	}
}

func TestIsBotEmail_EmptyString(t *testing.T) {
	if IsBotEmail("") {
		t.Error("empty is not bot")
	}
}
