// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

// TestSanitizeBodyValue covers the shapes CodeQL alert 16 is about: an
// untrusted label trying to become structure, terminal control, or an
// invisible reordering of what the reader sees.
func TestSanitizeBodyValue(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"plain", "augurlabs", "augurlabs"},
		{"forged sign-off and link", "evil\n\n\u2014 Aveloxis\n\nClick: http://evil", "evil \u2014 Aveloxis Click: http://evil"},
		{"CRLF header attempt", "a\r\nBcc: x@y", "a Bcc: x@y"},
		{"ANSI escape", "gr\x1b[2Joup", "gr[2Joup"},
		{"NUL and DEL", "a\x00b\x7fc", "abc"},
		{"C1 control", "a\u0085b", "ab"},
		{"bidi override", "safe\u202Etxt.exe", "safetxt.exe"},
		{"bidi isolate", "a\u2066b\u2069c", "abc"},
		{"zero width", "goo\u200bgle", "google"},
		{"BOM", "\ufeffname", "name"},
		{"tabs collapse", "a\t\tb", "a b"},
		{"unicode kept", "Gr\u00fc\u00dfe-\u9879\u76ee", "Gr\u00fc\u00dfe-\u9879\u76ee"},
		{"empty", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeBodyValue(tc.in); got != tc.want {
				t.Errorf("sanitizeBodyValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSanitizeBodyValueCaps(t *testing.T) {
	for _, tc := range []struct{ name, in string }{
		{"ascii", strings.Repeat("x", bodyValueMax+50)},
		// The cap used to slice BYTES: cutting at 300 bytes through a
		// multi-byte rune left an orphaned lead byte, i.e. invalid UTF-8 in
		// a body declared charset=UTF-8. An all-ASCII fixture could never
		// catch it, which is why the first version of this test did not.
		{"multi-byte at the boundary", strings.Repeat("x", bodyValueMax-1) + strings.Repeat("\u9879\u76ee", 10)},
		{"all multi-byte", strings.Repeat("\u9879", bodyValueMax+50)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeBodyValue(tc.in)
			if !utf8.ValidString(got) {
				t.Errorf("sanitizeBodyValue produced invalid UTF-8: % x", got)
			}
			if n := len([]rune(got)); n != bodyValueMax+1 { // +1 for the ellipsis
				t.Errorf("len = %d runes, want %d plus an ellipsis", n, bodyValueMax)
			}
			if !strings.HasSuffix(got, "\u2026") {
				t.Error("an over-long value must be visibly truncated")
			}
		})
	}
}

// TestSanitizeBodyValueDropsFormatRunesByCategory covers what an enumerated
// list missed: nine bidi/format runes survived the first version, and the
// line separators were neutralized only as a side effect of strings.Fields,
// which a refactor could have undone silently.
func TestSanitizeBodyValueDropsFormatRunesByCategory(t *testing.T) {
	for _, tc := range []struct{ name, in, want string }{
		{"LRM", "a\u200eb", "ab"},
		{"RLM", "a\u200fb", "ab"},
		{"ALM", "a\u061cb", "ab"},
		{"soft hyphen", "a\u00adb", "ab"},
		{"invisible times", "a\u2062b", "ab"},
		{"interlinear annotation", "a\ufff9b\ufffbc", "abc"},
		{"tag block (ASCII smuggling)", "a\U000e0041b", "ab"},
		// U+FE0F is Mn (a nonspacing mark), not Cf, so it is KEPT on
		// purpose: it only selects emoji presentation, cannot forge
		// structure, and dropping it would mangle legitimate names.
		{"variation selector is kept", "a\ufe0fb", "a\ufe0fb"},
		{"line separator", "a\u2028b", "a b"},
		{"paragraph separator", "a\u2029b", "a b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sanitizeBodyValue(tc.in); got != tc.want {
				t.Errorf("sanitizeBodyValue(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestSanitizeHeaderSharesTheBodyFilter: subjects carry the same untrusted
// names as bodies, so they must get the same scrubbing. Before this, the
// header filter dropped only C0 and DEL, letting C1, bidi overrides and
// zero-width runes into the Subject: line.
func TestSanitizeHeaderSharesTheBodyFilter(t *testing.T) {
	for _, in := range []string{"a\u202Eb", "a\u0085b", "a\u200bb", "a\u2028b", "a\r\nBcc: x@y"} {
		if got, want := sanitizeHeader(in), sanitizeBodyValue(in); got != want {
			t.Errorf("sanitizeHeader(%q) = %q but sanitizeBodyValue gives %q — they must share one normalizer", in, got, want)
		}
	}
}

// TestSanitizeSampleKeepsListStructure: the entries are attacker-supplied,
// the line breaks between them are the template's.
func TestSanitizeSampleKeepsListStructure(t *testing.T) {
	got := sanitizeSample([]string{"https://github.com/a/b", "evil\nInjected: yes", "", "https://github.com/c/d"})
	want := "https://github.com/a/b\nevil Injected: yes\nhttps://github.com/c/d"
	if got != want {
		t.Errorf("sanitizeSample = %q, want %q", got, want)
	}
	if strings.Count(got, "\n") != 2 {
		t.Errorf("got %d line breaks, want 2 — one per surviving entry", strings.Count(got, "\n"))
	}
}

// TestEveryUntrustedBodyInterpolationIsSanitized is the tripwire. Every
// Send* builder interpolates caller-supplied strings into a body, and each
// must pass through sanitizeBodyValue (or sanitizeSample). Forgetting is
// exactly how CodeQL alert 16 came about.
//
// It parses the file (AST) rather than matching line shapes. The first
// version matched three shapes and was blind to two real sites — the
// multi-line raw-string verdicts in SendAddRequestDecided, whose argument
// line begins with prose — and to the whole SendVulnerabilityDigest
// builder, which uses fmt.Fprintf. Verified: unwrapping either verdict's
// groupName left it PASSING.
//
// The untrusted set is DERIVED from each Send* function's own string
// parameters, not hand-listed, so a new builder taking `orgName` or
// `userName` is covered the day it is written.
func TestEveryUntrustedBodyInterpolationIsSanitized(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "mailer.go", nil, 0)
	if err != nil {
		t.Fatalf("parse mailer.go: %v", err)
	}

	// Values the package derives itself, or that come from operator config
	// rather than from a user: not attacker-controlled.
	trusted := map[string]bool{
		"to": true, "toEmail": true, "subject": true, "body": true,
		"what": true, "verdict": true, "link": true, "siteURL": true,
		"confirmURL": true, "kind": true, "summary": true,
	}

	sanitizers := map[string]bool{"sanitizeBodyValue": true, "sanitizeSample": true, "sanitizeHeader": true, "scrubUntrusted": true}

	checked := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || !strings.HasPrefix(fn.Name.Name, "Send") || fn.Recv == nil {
			continue
		}
		// String parameters of this builder = its untrusted inputs.
		untrusted := map[string]bool{}
		for _, p := range fn.Type.Params.List {
			if id, ok := p.Type.(*ast.Ident); !ok || id.Name != "string" {
				continue
			}
			for _, n := range p.Names {
				if !trusted[n.Name] {
					untrusted[n.Name] = true
				}
			}
		}
		if len(untrusted) == 0 {
			continue
		}
		ast.Inspect(fn, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "Sprintf" && sel.Sel.Name != "Fprintf" {
				return true
			}
			for _, arg := range call.Args {
				id, ok := arg.(*ast.Ident) // a BARE identifier argument
				if !ok || !untrusted[id.Name] {
					continue
				}
				checked++
				t.Errorf("%s interpolates %s unsanitized at %s — wrap it in sanitizeBodyValue (CodeQL alert 16: a forged sign-off and link inside a group name reads as if Aveloxis sent it)",
					fn.Name.Name, id.Name, fset.Position(id.Pos()))
			}
			return true
		})
		checked++
	}
	// Denominator guard: count builders EXAMINED, so the pin cannot pass by
	// having quietly stopped finding any.
	if checked < 5 {
		t.Fatalf("examined only %d Send* builders — the scan is not reaching them", checked)
	}
	_ = sanitizers
}

// TestSendUsesTheSanitizedRecipientForTheEnvelope: the envelope address and
// the To: header must carry the same scrubbed value, not one of each.
func TestSendUsesTheSanitizedRecipientForTheEnvelope(t *testing.T) {
	src, err := os.ReadFile("mailer.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "[]string{sanitizeHeader(to)}") {
		t.Error("smtp.SendMail must receive the sanitized recipient, matching the To: header")
	}
}
