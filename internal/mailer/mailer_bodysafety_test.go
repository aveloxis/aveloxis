// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package mailer

import (
	"go/ast"
	"go/parser"
	"go/token"
	"net/mail"
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

// TestSendRejectsUnparseableRecipients: `to` arrives straight from a web
// form, where the only check is a stray "@". Parsing is what makes the
// header and envelope an addr-spec by construction. A bad address must be
// SKIPPED with a WARN, not error — it must not break account creation or
// group approval, which is the same contract the empty-recipient case has.
func TestSendRejectsUnparseableRecipients(t *testing.T) {
	for _, tc := range []struct {
		name string
		to   string
		send bool
	}{
		{"plain address", "user@example.com", true},
		{"display name form", "Real Name <user@example.com>", true},
		{"header injection attempt", "user@example.com\r\nBcc: victim@example.com", false},
		// A TRAILING newline is stripped by TrimSpace before parsing, so
		// this is a valid address — the dangerous shape is an INNER CR/LF,
		// covered above, which ParseAddress rejects.
		{"trailing newline is trimmed", "user@example.com\n", true},
		{"no at sign", "not-an-address", false},
		{"two addresses", "a@example.com, b@example.com", false},
		{"empty", "", false},
		{"spaces", "   ", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			addr, err := mail.ParseAddress(strings.TrimSpace(tc.to))
			if tc.send {
				if err != nil {
					t.Fatalf("ParseAddress(%q) = %v, want it accepted", tc.to, err)
				}
				if strings.ContainsAny(addr.Address, "\r\n ") {
					t.Errorf("parsed address %q still contains CR/LF or a space", addr.Address)
				}
				return
			}
			if err == nil && strings.TrimSpace(tc.to) != "" {
				t.Errorf("ParseAddress(%q) = %q with no error — an unparseable recipient must not reach the header or envelope", tc.to, addr.Address)
			}
		})
	}
}

// TestEveryUntrustedBodyInterpolationIsSanitized is the tripwire. Every
// Send* builder interpolates caller-supplied data into a message, and every
// untrusted value must reach fmt through a sanitizer. Forgetting is exactly
// how CodeQL alert 16 came about.
//
// It traces argument EXPRESSIONS recursively, not bare identifiers. Two
// earlier versions were escapable, both mutation-proved:
//   - matching three line shapes missed the multi-line raw-string verdicts
//     in SendAddRequestDecided and all of SendVulnerabilityDigest;
//   - accepting only a bare `ident` missed any wrapper — `strings.ToUpper(
//     requesterLogin)` — and every field of a non-string parameter, which
//     is precisely how the digest's `it.Summary` reaches fmt.
//
// So: a call to a sanitizer makes its subtree safe; anything else is
// descended into; an untrusted leaf reached without passing through one is
// a failure. Locals are tracked too, since builders assemble values in
// steps (`summary := sanitizeBodyValue(it.Summary)` is safe; a local
// assigned from an unsanitized untrusted value is not).
//
// The untrusted set is DERIVED from each builder's own parameters, so a new
// builder taking `orgName` is covered the day it is written.
func TestEveryUntrustedBodyInterpolationIsSanitized(t *testing.T) {
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "mailer.go", nil, 0)
	if err != nil {
		t.Fatalf("parse mailer.go: %v", err)
	}

	sanitizers := map[string]bool{
		"sanitizeBodyValue": true, "sanitizeSample": true,
		"sanitizeHeader": true, "scrubUntrusted": true,
	}
	// Values this package assembles or takes from operator config.
	// `body` is the assembled message: its parts are sanitized
	// individually by the builders, and scrubbing it here would collapse
	// the templates' own line breaks. `confirmURL` is NOT exempt — it is
	// built from a site URL that used to come from the request Host.
	trusted := map[string]bool{"to": true, "toEmail": true, "subject": true, "body": true}

	// untrustedType: the shapes that can carry attacker text.
	untrustedType := func(e ast.Expr) bool {
		switch typ := e.(type) {
		case *ast.Ident:
			return typ.Name == "string"
		case *ast.ArrayType: // []string, []VulnDigestItem
			_, isIdent := typ.Elt.(*ast.Ident)
			return isIdent
		}
		return false
	}

	builders := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Recv == nil || !strings.HasPrefix(fn.Name.Name, "Send") {
			continue
		}
		untrusted := map[string]bool{}
		for _, p := range fn.Type.Params.List {
			if !untrustedType(p.Type) {
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
		builders++

		// tainted reports whether expr reaches an untrusted leaf without
		// passing through a sanitizer.
		var tainted func(ast.Expr) (string, bool)
		tainted = func(expr ast.Expr) (string, bool) {
			switch e := expr.(type) {
			case *ast.CallExpr:
				if id, ok := e.Fun.(*ast.Ident); ok {
					if sanitizers[id.Name] {
						return "", false // sanitized subtree
					}
					// len/cap of untrusted data yield an int, which cannot
					// carry text into the message.
					if id.Name == "len" || id.Name == "cap" {
						return "", false
					}
				}
				for _, a := range e.Args {
					if name, bad := tainted(a); bad {
						return name, true
					}
				}
			case *ast.Ident:
				if untrusted[e.Name] {
					return e.Name, true
				}
			case *ast.SelectorExpr: // it.Summary, where `it` ranges over items
				return tainted(e.X)
			case *ast.IndexExpr:
				return tainted(e.X)
			case *ast.SliceExpr:
				return tainted(e.X)
			case *ast.BinaryExpr:
				if name, bad := tainted(e.X); bad {
					return name, true
				}
				return tainted(e.Y)
			case *ast.ParenExpr:
				return tainted(e.X)
			case *ast.StarExpr:
				return tainted(e.X)
			case *ast.UnaryExpr:
				return tainted(e.X)
			}
			return "", false
		}

		// Walk the body in order so locals and range vars pick up taint
		// before the fmt call that uses them.
		ast.Inspect(fn, func(n ast.Node) bool {
			switch stmt := n.(type) {
			case *ast.RangeStmt: // for _, it := range items
				if _, bad := tainted(stmt.X); bad {
					if id, ok := stmt.Value.(*ast.Ident); ok && id.Name != "_" {
						untrusted[id.Name] = true
					}
				}
			case *ast.AssignStmt:
				for i, rhs := range stmt.Rhs {
					if i >= len(stmt.Lhs) {
						break
					}
					id, ok := stmt.Lhs[i].(*ast.Ident)
					if !ok || id.Name == "_" {
						continue
					}
					if _, bad := tainted(rhs); bad {
						untrusted[id.Name] = true
					} else {
						delete(untrusted, id.Name) // reassigned from a safe value
					}
				}
			case *ast.CallExpr:
				sel, ok := stmt.Fun.(*ast.SelectorExpr)
				if !ok || (sel.Sel.Name != "Sprintf" && sel.Sel.Name != "Fprintf") {
					return true
				}
				for _, arg := range stmt.Args {
					if name, bad := tainted(arg); bad {
						t.Errorf("%s interpolates %s unsanitized at %s — wrap it in sanitizeBodyValue (CodeQL alert 16: a forged sign-off and link inside a group name reads as if Aveloxis sent it)",
							fn.Name.Name, name, fset.Position(arg.Pos()))
					}
				}
			}
			return true
		})
	}
	// Denominator guard: count builders EXAMINED, so the pin cannot pass by
	// having quietly stopped finding any.
	if builders < 5 {
		t.Fatalf("examined only %d Send* builders — the scan is not reaching them", builders)
	}
}

// TestSendUsesTheParsedRecipientForTheEnvelope: the envelope address and
// the To: header must carry the SAME parsed address, not one of each and
// not the raw form value.
func TestSendUsesTheParsedRecipientForTheEnvelope(t *testing.T) {
	src, err := os.ReadFile("mailer.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "[]string{recipient}") {
		t.Error("smtp.SendMail must receive the parsed recipient, the same value the To: header carries")
	}
	if !strings.Contains(string(src), "mail.ParseAddress(") {
		t.Error("the recipient must be parsed into an addr-spec, not merely scrubbed — it arrives straight from a web form (CodeQL alert 197)")
	}
}
