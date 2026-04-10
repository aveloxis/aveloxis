package github

import (
	"testing"
)

func TestGhUserToRef_AllFields(t *testing.T) {
	u := ghUser{
		ID:        12345,
		Login:     "octocat",
		Name:      "The Octocat",
		Email:     "octocat@github.com",
		AvatarURL: "https://avatars.githubusercontent.com/u/12345",
		HTMLURL:   "https://github.com/octocat",
		NodeID:    "MDQ6VXNlcjEyMzQ1",
		Type:      "User",
	}

	ref := ghUserToRef(u)

	if ref.PlatformID != 12345 {
		t.Errorf("PlatformID = %d, want 12345", ref.PlatformID)
	}
	if ref.Login != "octocat" {
		t.Errorf("Login = %q, want %q", ref.Login, "octocat")
	}
	if ref.Name != "The Octocat" {
		t.Errorf("Name = %q, want %q", ref.Name, "The Octocat")
	}
	if ref.Email != "octocat@github.com" {
		t.Errorf("Email = %q, want %q", ref.Email, "octocat@github.com")
	}
	if ref.AvatarURL != "https://avatars.githubusercontent.com/u/12345" {
		t.Errorf("AvatarURL = %q, want correct URL", ref.AvatarURL)
	}
	if ref.URL != "https://github.com/octocat" {
		t.Errorf("URL = %q, want %q", ref.URL, "https://github.com/octocat")
	}
	if ref.NodeID != "MDQ6VXNlcjEyMzQ1" {
		t.Errorf("NodeID = %q, want %q", ref.NodeID, "MDQ6VXNlcjEyMzQ1")
	}
	if ref.Type != "User" {
		t.Errorf("Type = %q, want %q", ref.Type, "User")
	}
}

func TestGhUserToRef_ZeroUser(t *testing.T) {
	var u ghUser
	ref := ghUserToRef(u)

	if ref.PlatformID != 0 {
		t.Errorf("PlatformID = %d, want 0", ref.PlatformID)
	}
	if ref.Login != "" {
		t.Errorf("Login = %q, want empty", ref.Login)
	}
	if !ref.IsZero() {
		t.Error("expected zero ghUser to produce a zero-ish UserRef")
	}
}
