// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestTokenHashCarriesNoTokenCharacters — CodeQL alert 201 (PR #220,
// go/clear-text-logging): the key pool named a key in its logs by the first
// 8 characters of the token, 4 of them secret on a classic "ghp_" token,
// and every fine-grained token read "github_p..." (whole-branch review C2).
// A key is now named by its public type and a SHA-256 fingerprint: no
// character of the secret reaches a log, and fine-grained tokens differ.
func TestTokenHashCarriesNoTokenCharacters(t *testing.T) {
	fp := func(tok string) string {
		sum := sha256.Sum256([]byte(tok))
		return hex.EncodeToString(sum[:])[:tokenHashHexLen]
	}
	for _, tc := range []struct{ tok, kind string }{
		{"ghp_ZqXwRtYuKmNbVcLpQsWeDfGhJkLzXcVbNmQw", "ghp_"},
		{"gho_ZqXwRtYuKmNbVcLpQsWeDfGhJkLzXcVbNmQw", "gho_"},
		{"github_pat_11ZQXWRTY0ZqXwRtYuKmNbVcLpQsWeDfGhJkLzXcVbNm", "github_pat_"},
		{"glpat-ZqXwRtYuKmNbVcLpQsWe", "glpat-"},
		{"ZqXwRtYuKmNbVcLpQsWeDfGhJkLzXcVbNmQwZqXw", ""}, // no recognised type
		{"ghp_", ""}, // a bare type is not a token of that type
		{"ab", ""},   // shorter than any prefix slice
	} {
		got := TokenHash(tc.tok)
		if want := tc.kind + "#" + fp(tc.tok); got != want {
			t.Errorf("TokenHash(%q) = %q, want %q", tc.tok, got, want)
		}
		secret := strings.TrimPrefix(tc.tok, tc.kind)
		for i := 0; i+4 <= len(secret); i++ {
			if strings.Contains(got, secret[i:i+4]) {
				t.Errorf("TokenHash(%q) = %q carries token characters %q", tc.tok, got, secret[i:i+4])
			}
		}
	}
	if got := TokenHash(""); got != "" {
		t.Errorf("TokenHash(\"\") = %q, want empty (no key)", got)
	}
	a, b := "github_pat_11AAAAAAA0one", "github_pat_11AAAAAAA0two"
	if TokenHash(a) == TokenHash(b) {
		t.Errorf("two fine-grained tokens share a name (%s): the logs cannot tell them apart", TokenHash(a))
	}
}

// TestCheckKeysMarkersMatchTokenHash — scripts/check-keys.sh names keys
// like the logs (TestCheckKeysNamesKeysLikeTheLogs runs its mask()); its
// copy of the type markers must stay the same list, in the same order.
func TestCheckKeysMarkersMatchTokenHash(t *testing.T) {
	src := srctest.Read(t, "scripts/check-keys.sh")
	const open = "local -a prefixes=("
	i := strings.Index(src, open)
	if i < 0 {
		t.Fatal("check-keys.sh mask() has no prefixes array")
	}
	rest := src[i+len(open):]
	j := strings.Index(rest, ")")
	if j < 0 {
		t.Fatal("the prefixes array is not closed")
	}
	got := strings.Fields(rest[:j])
	if strings.Join(got, " ") != strings.Join(tokenTypePrefixes, " ") {
		t.Errorf("check-keys.sh markers %v, want tokenTypePrefixes %v", got, tokenTypePrefixes)
	}
}
