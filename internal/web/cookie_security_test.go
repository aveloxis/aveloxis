package web

import (
	"testing"

	"github.com/augurlabs/aveloxis/internal/config"
)

// TestSessionCookie_SecureByDefault verifies that in production mode (default),
// cookies set the Secure attribute.
func TestSessionCookie_SecureByDefault(t *testing.T) {
	cfg := config.WebConfig{} // default: SecureCookies not set
	s := &Server{cfg: cfg}
	cookie := s.sessionCookie("token123")
	if !cookie.Secure {
		t.Error("cookies should be Secure by default (production mode)")
	}
	if !cookie.HttpOnly {
		t.Error("cookies should always be HttpOnly")
	}
	if cookie.Name != "aveloxis_session" {
		t.Errorf("cookie name = %q, want aveloxis_session", cookie.Name)
	}
	if cookie.Value != "token123" {
		t.Errorf("cookie value = %q, want token123", cookie.Value)
	}
}

// TestSessionCookie_InsecureForDev verifies that with dev_mode=true,
// cookies do NOT set the Secure attribute (allowing HTTP for local dev).
func TestSessionCookie_InsecureForDev(t *testing.T) {
	cfg := config.WebConfig{DevMode: true}
	s := &Server{cfg: cfg}
	cookie := s.sessionCookie("token123")
	if cookie.Secure {
		t.Error("cookies should NOT be Secure in dev mode (HTTP dev)")
	}
	if !cookie.HttpOnly {
		t.Error("cookies should always be HttpOnly, even in dev mode")
	}
}

// TestOAuthStateCookie_AlwaysHttpOnly verifies the OAuth state cookie.
func TestOAuthStateCookie_AlwaysHttpOnly(t *testing.T) {
	cfg := config.WebConfig{}
	s := &Server{cfg: cfg}
	cookie := s.oauthStateCookie("state123")
	if !cookie.HttpOnly {
		t.Error("OAuth state cookie should be HttpOnly")
	}
	if !cookie.Secure {
		t.Error("OAuth state cookie should be Secure in production")
	}
}

// TestLogoutCookie_HttpOnlySet verifies the logout (expire) cookie.
func TestLogoutCookie_HttpOnlySet(t *testing.T) {
	cfg := config.WebConfig{}
	s := &Server{cfg: cfg}
	cookie := s.expireCookie("aveloxis_session")
	if !cookie.HttpOnly {
		t.Error("logout cookie should set HttpOnly")
	}
	if cookie.MaxAge != -1 {
		t.Errorf("logout cookie MaxAge = %d, want -1", cookie.MaxAge)
	}
}
