// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package textclean is the one sanitizer for text Aveloxis stores, and the one
// answer to "would storing this change it?".
//
// PostgreSQL's TEXT type cannot hold a null byte, and rejects a byte sequence
// that is not valid UTF-8:
//
//	ERROR: invalid byte sequence for encoding "UTF8": 0x00
//
// Forge APIs, manifests, lockfiles and registry answers all carry such bytes
// now and then. Sanitize removes what cannot be stored. It lived in
// internal/db as SanitizeText (which still exists and calls this); it is a
// package of its own, importing nothing from Aveloxis, so that the units of
// the vulnerability program (summary/70, section 3) can use it without
// importing the database package.
package textclean

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Sanitize cleans a string for storage in a PostgreSQL TEXT column:
//
//  1. a null byte is removed;
//  2. a byte that is not valid UTF-8 becomes U+FFFD, the replacement character;
//  3. C0 and C1 control characters are removed, except tab, newline and
//     carriage return.
//
// A string that needs none of that is returned as it is. In particular valid
// non-ASCII text is unchanged.
func Sanitize(s string) string {
	if s == "" {
		return s
	}

	// Fast path: nothing in the string can need cleaning.
	if !mayNeedSanitizing(s) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])

		if r == utf8.RuneError && size <= 1 {
			// Invalid UTF-8 byte: replaced with U+FFFD.
			b.WriteRune('�')
			i++
			continue
		}

		if r == 0 {
			// Null byte: dropped.
			i++
			continue
		}

		// C0 control characters, except tab, newline and carriage return.
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			i += size
			continue
		}

		// DEL and the C1 control characters (0x7F-0x9F).
		if r == 0x7F || (r >= 0x80 && r <= 0x9F) {
			i += size
			continue
		}

		// Any other control character Unicode knows.
		if unicode.Is(unicode.Cc, r) && r != '\t' && r != '\n' && r != '\r' {
			i += size
			continue
		}

		b.WriteRune(r)
		i += size
	}

	return b.String()
}

// WouldChange reports whether Sanitize would return something other than s:
// whether s, stored, would not be s. It is exactly Sanitize(s) != s. A caller
// that must never repair a value (a package's name is its identity) asks this
// and refuses the value when it is true.
//
// It is not mayNeedSanitizing, which is true for every string with a byte
// above 0x7F, valid or not: that is only Sanitize's cheap first look.
func WouldChange(s string) bool {
	return Sanitize(s) != s
}

// mayNeedSanitizing is Sanitize's fast path: false means the string is plain
// ASCII with no control character and no null byte, so nothing can change.
// True means only "look properly": every non-ASCII string answers true, valid
// or not. Never use it to decide that a value is dirty.
func mayNeedSanitizing(s string) bool {
	for i := 0; i < len(s); i++ {
		b := s[i]
		if b == 0 {
			return true // null byte
		}
		if b < 0x20 && b != '\t' && b != '\n' && b != '\r' {
			return true // control character
		}
		if b == 0x7F {
			return true // DEL
		}
		if b >= 0x80 {
			return true // not ASCII: decode it to find out
		}
	}
	return false
}
