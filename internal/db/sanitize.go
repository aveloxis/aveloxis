// Package db — sanitize.go provides text sanitization for data going into PostgreSQL.
//
// PostgreSQL's TEXT type cannot store null bytes (\x00). GitHub and GitLab API
// responses sometimes contain them in issue bodies, PR descriptions, commit
// messages, and comments — especially from bot-generated content, copy-pasted
// binary data, or malformed Unicode. Without sanitization these cause:
//   ERROR: invalid byte sequence for encoding "UTF8": 0x00
//
// This module mirrors Augur's remove_null_characters_from_string() and the
// encode('UTF-8', errors='backslashreplace').decode('UTF-8', errors='ignore')
// pattern from augur/application/db/data_parse.py.
package db

import (
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// NullTime converts a time.Time to a *time.Time, returning nil for zero values.
// This prevents Go's zero time (year 0001) from being written to PostgreSQL as
// a garbage BC-era timestamp. Postgres receives NULL instead.
func NullTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

// SanitizeText cleans a string for safe PostgreSQL TEXT storage:
//  1. Removes null bytes (\x00) — PostgreSQL cannot store them
//  2. Replaces invalid UTF-8 sequences with the Unicode replacement character
//  3. Strips other C0/C1 control characters (except \n, \r, \t)
//
// This is called on all text fields before database insertion.
func SanitizeText(s string) string {
	if s == "" {
		return s
	}

	// Fast path: if the string is clean, return as-is.
	if !needsSanitization(s) {
		return s
	}

	var b strings.Builder
	b.Grow(len(s))

	for i := 0; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])

		if r == utf8.RuneError && size <= 1 {
			// Invalid UTF-8 byte — replace with U+FFFD.
			b.WriteRune('\uFFFD')
			i++
			continue
		}

		if r == 0 {
			// Null byte — skip entirely.
			i++
			continue
		}

		// Strip C0 control characters except tab, newline, carriage return.
		if r < 0x20 && r != '\t' && r != '\n' && r != '\r' {
			i += size
			continue
		}

		// Strip C1 control characters (0x7F-0x9F).
		if r == 0x7F || (r >= 0x80 && r <= 0x9F) {
			i += size
			continue
		}

		// Strip Unicode "other" category control chars.
		if unicode.Is(unicode.Cc, r) && r != '\t' && r != '\n' && r != '\r' {
			i += size
			continue
		}

		b.WriteRune(r)
		i += size
	}

	return b.String()
}

// needsSanitization quickly checks if a string contains any bytes that need cleaning.
func needsSanitization(s string) bool {
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
		if b >= 0x80 && !utf8.ValidString(s[i:i+1]) {
			return true // potentially invalid UTF-8
		}
	}
	return false
}
