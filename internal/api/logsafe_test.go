// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"strings"
	"testing"
)

func TestLogSafeStripsNewlines(t *testing.T) {
	got := logSafe("legit\nFAKE level=ERROR forged line\rmore")
	if strings.ContainsAny(got, "\n\r") {
		t.Errorf("logSafe must strip newlines/carriage returns, got %q", got)
	}
	if !strings.Contains(got, "legit") || !strings.Contains(got, "forged line") {
		t.Errorf("logSafe should keep the printable content, got %q", got)
	}
}

func TestLogSafeStripsControlChars(t *testing.T) {
	got := logSafe("a\x1b[31mred\x00b\x7fc")
	for _, r := range got {
		if r < 0x20 || r == 0x7f {
			t.Errorf("logSafe must drop control characters, got %q", got)
		}
	}
	if got != "a[31mredbc" {
		t.Errorf("unexpected scrub result %q", got)
	}
}

func TestLogSafeTruncates(t *testing.T) {
	got := logSafe(strings.Repeat("x", 500))
	if len(got) > 220 {
		t.Errorf("logSafe must bound length (flood protection), got %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "...(truncated)") {
		t.Errorf("truncated output should say so, got suffix %q", got[len(got)-20:])
	}
}

func TestLogSafePassesCleanStringsThrough(t *testing.T) {
	if got := logSafe("org:github.com/chaoss"); got != "org:github.com/chaoss" {
		t.Errorf("clean input must pass through unchanged, got %q", got)
	}
}

// TestUserInputLogSitesAreSanitized pins that the API log sites CodeQL
// flagged (user-provided metric / entity label flowing into slog) route
// through logSafe. New handler logging of request-derived strings
// should follow the same pattern.
func TestUserInputLogSitesAreSanitized(t *testing.T) {
	src := mustReadFile(t, "analytics.go")
	for _, needle := range []string{
		`"metric", logSafe(metric), "entity", logSafe(e.Label), "error", logSafe(err.Error())`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("analytics.go log sites must sanitize user-derived values via logSafe; missing %q", needle)
		}
	}
	if strings.Contains(src, `"metric", metric, "entity", e.Label`) {
		t.Error("analytics.go still logs raw user-derived metric/entity values — wrap them in logSafe")
	}
}
