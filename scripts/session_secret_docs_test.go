// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// web.session_secret is reserved: nothing reads config.WebConfig.SessionSecret
// outside the loader. PR #226 Copilot review 5462937284: configuration.md and
// the field comment still said it signs cookies and keeps sessions across
// restarts while web-gui.md, commands.md and deployment.md said it is unread.

// sessionSecretClaim reports a sentence that mentions the setting and claims
// it signs cookies or keeps sessions across restarts, unless the sentence
// itself says the setting is reserved or plays no part (the honest wording).
var (
	sessionSecretMention = regexp.MustCompile(`(?i)session_secret|SessionSecret`)
	sessionSecretPower   = regexp.MustCompile(`(?i)\bsign(s|ed|ing)?\b[^.]*\bcookies?\b|\bcookies?\b[^.]*\bsign(s|ed|ing)?\b|\b(survive|survives|persist|persists|keep|keeps)\b[^.]*\brestarts?\b`)
	sessionSecretHonest  = regexp.MustCompile(`(?i)\breserved\b|\bnot read\b|\bnot signed\b|\bno part\b|\bunread\b`)
)

func sessionSecretClaims(text string) []string {
	var out []string
	for _, sentence := range regexp.MustCompile(`[.!?](\s|$)|\n\n|\|\s*\n`).Split(text, -1) {
		if sessionSecretMention.MatchString(sentence) && sessionSecretPower.MatchString(sentence) && !sessionSecretHonest.MatchString(sentence) {
			out = append(out, strings.TrimSpace(sentence))
		}
	}
	return out
}

// The judge on a corpus with the prose forms the first version missed (a
// bullet, positive "keeps sessions" wording) and the honest current texts.
func TestSessionSecretClaimJudge(t *testing.T) {
	for _, bad := range []string{
		"| `web.session_secret` | string | (none) | Secret used to sign session cookies.",
		"- **`web.session_secret`** signs the session cookies.",
		"Set `web.session_secret` so the web GUI keeps sessions across restarts.",
		"SessionSecret is used to sign session cookies (generate a random string).",
		"Without `session_secret`, sessions don't survive restarts.",
	} {
		if len(sessionSecretClaims(bad)) == 0 {
			t.Errorf("not caught: %q", bad)
		}
	}
	for _, ok := range []string{
		"The session cookie carries a random token; it is not signed, so `web.session_secret` plays no part today (the API's Bearer tokens survive restarts).",
		"- **`web.session_secret`** is accepted and reserved; web sessions are random tokens held in the process.",
		"| `web.session_secret` | string | (none) | Reserved: accepted by the loader and not read today. Web sessions are random tokens held in the web process, so a web restart signs everyone out whatever this is set to; the API's Bearer tokens are stored in the database and survive restarts.",
		`"session_secret": "change-me-to-a-random-string",`,
	} {
		if got := sessionSecretClaims(ok); len(got) != 0 {
			t.Errorf("honest text flagged: %q", got)
		}
	}
}

// While no code reads the field, no page, the README or the field comment
// may claim it signs cookies or keeps sessions across restarts. When code
// starts reading it, this FAILS (a skip reads green in CI): rewrite every
// "reserved, not read today" text to say what it does, then retire this.
func TestSessionSecretIsDocumentedAsReserved(t *testing.T) {
	root := srctest.Root(t)
	readers := 0
	for _, dir := range []string{"internal", "cmd"} {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(p string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(p, ".go") || strings.HasSuffix(p, "_test.go") || strings.HasSuffix(p, filepath.Join("config", "config.go")) {
				return nil
			}
			b, _ := os.ReadFile(p)
			readers += strings.Count(srctest.StripGoComments(string(b)), ".SessionSecret")
			return nil
		})
	}
	if readers > 0 {
		t.Fatal("code now reads web.session_secret: rewrite the docs that call it reserved / not read today (configuration.md, web-gui.md, commands.md, deployment.md, the WebConfig field comment), then retire this test")
	}
	files := []string{"internal/config/config.go", "README.md"}
	for _, dir := range []string{"docs/getting-started", "docs/guide", "docs/architecture"} {
		ms, _ := filepath.Glob(filepath.Join(root, dir, "*.md"))
		for _, m := range ms {
			rel, _ := filepath.Rel(root, m)
			files = append(files, rel)
		}
	}
	if len(files) < 20 {
		t.Fatalf("scanned %d files; the doc globs found too few", len(files))
	}
	for _, f := range files {
		for _, claim := range sessionSecretClaims(srctest.Read(t, f)) {
			t.Errorf("%s claims web.session_secret does something, but nothing reads it: %q", f, claim)
		}
	}
}
