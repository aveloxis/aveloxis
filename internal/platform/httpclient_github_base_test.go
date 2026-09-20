// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"io"
	"log/slog"
	"testing"
)

// v0.29.57, Copilot review 5261384568: seven commands built their GitHub
// client as github.New(cfg.GitHub.BaseURL, …) with the RAW field, which is
// empty when unset, so every request went out as "/repos/…" with no host.
// The accessor that defaults the base existed, but normalising at the
// callers is a sweep that stops one site short every time (seven reviews
// in a row). The constructor every GitHub client passes through normalises
// instead, so no caller CAN build a hostless GitHub client.
func TestNewHTTPClientDefaultsAnEmptyGitHubBaseToPublic(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := NewKeyPool([]string{"k"}, logger)
	for _, base := range []string{"", "https://api.github.com/"} {
		c := NewHTTPClient(base, keys, logger, AuthGitHub)
		if c.baseURL != PublicGitHubAPIBase {
			t.Errorf("NewHTTPClient(%q, AuthGitHub).baseURL = %q, want %q", base, c.baseURL, PublicGitHubAPIBase)
		}
	}
	// A configured host is kept verbatim (trailing slash aside).
	if c := NewHTTPClient("https://ghe.example.invalid/api/v3/", keys, logger, AuthGitHub); c.baseURL != "https://ghe.example.invalid/api/v3" {
		t.Errorf("configured base rewritten: %q", c.baseURL)
	}
	// The GitHub default is GitHub's: a GitLab client with no base must not
	// acquire api.github.com.
	if c := NewHTTPClient("", keys, logger, AuthGitLab); c.baseURL == PublicGitHubAPIBase {
		t.Error("a GitLab client's empty base became public GitHub")
	}
}
