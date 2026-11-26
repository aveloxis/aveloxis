// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package textclean

import "testing"

func TestSanitize(t *testing.T) {
	cases := []struct {
		name, in, want string
	}{
		{"empty", "", ""},
		{"plain ASCII", "left-pad", "left-pad"},
		{"tab newline and carriage return are kept", "a\tb\nc\rd", "a\tb\nc\rd"},
		{"null byte removed", "fo\x00o", "foo"},
		{"only null bytes", "\x00\x00", ""},
		{"C0 control removed", "a\x01b\x1fc", "abc"},
		{"DEL removed", "a\x7fb", "ab"},
		{"C1 control removed", "a\u0085b\u009fc", "abc"},
		{"invalid UTF-8 byte replaced", "a\xffb", "a�b"},
		{"truncated multi-byte sequence replaced", "caf\xc3", "caf�"},
		{"valid non-ASCII unchanged", "ruamel.yaml-日本語-café", "ruamel.yaml-日本語-café"},
		{"an existing replacement character is kept", "a�b", "a�b"},
		{"null byte beside non-ASCII", "caf\x00é", "café"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Sanitize(c.in); got != c.want {
				t.Errorf("Sanitize(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

// TestSanitizeIsIdempotent: cleaning a cleaned string changes nothing, so a
// value that passed through once is stable wherever it is cleaned again.
func TestSanitizeIsIdempotent(t *testing.T) {
	for _, in := range []string{"fo\x00o", "a\xffb", "a\u0085b", "日本語", "plain", "\x01\x02\x03"} {
		once := Sanitize(in)
		if twice := Sanitize(once); twice != once {
			t.Errorf("Sanitize is not idempotent for %q: %q then %q", in, once, twice)
		}
		if WouldChange(once) {
			t.Errorf("WouldChange(Sanitize(%q)) is true; a cleaned value must be stable", in)
		}
	}
}

// TestWouldChange is the identity question the vulnerability program asks of a
// package name (summary/70, 4.7): refuse a name that storing would alter, and
// never refuse one merely for not being ASCII.
func TestWouldChange(t *testing.T) {
	changed := []string{"fo\x00o", "a\x01b", "a\x7fb", "a\u0085b", "a\xffb", "caf\xc3"}
	for _, s := range changed {
		if !WouldChange(s) {
			t.Errorf("WouldChange(%q) = false, want true", s)
		}
	}
	unchanged := []string{
		"", "left-pad", "@scope/name", "a\tb\nc\rd",
		// Valid non-ASCII: a name, a directory, a lockfile entry. The fast
		// path answers "look properly" for each of these; the answer here must
		// still be "unchanged".
		"café", "日本語", "ruamel.yaml-ü", "src/données/pom.xml", "пакет",
		"a�b",
	}
	for _, s := range unchanged {
		if WouldChange(s) {
			t.Errorf("WouldChange(%q) = true, want false: valid text must not be refused", s)
		}
	}
}

// TestMayNeedSanitizingIsOnlyAFastPath pins the difference the package comment
// warns about, so nobody swaps the two: the fast path says "look" for valid
// non-ASCII text, and WouldChange says "unchanged".
func TestMayNeedSanitizingIsOnlyAFastPath(t *testing.T) {
	for _, s := range []string{"café", "日本語", "пакет"} {
		if !mayNeedSanitizing(s) {
			t.Errorf("mayNeedSanitizing(%q) = false; the fast path must send non-ASCII text to the full check", s)
		}
		if WouldChange(s) {
			t.Errorf("WouldChange(%q) = true; valid non-ASCII text is unchanged", s)
		}
	}
	for _, s := range []string{"", "plain-ascii_1.2", "a\tb\nc\rd"} {
		if mayNeedSanitizing(s) {
			t.Errorf("mayNeedSanitizing(%q) = true; clean ASCII must take the fast path", s)
		}
	}
}
