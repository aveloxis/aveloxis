// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package db — sanitize.go provides text sanitization for data going into PostgreSQL.
//
// PostgreSQL's TEXT type cannot store null bytes (\x00). GitHub and GitLab API
// responses sometimes contain them in issue bodies, PR descriptions, commit
// messages, and comments — especially from bot-generated content, copy-pasted
// binary data, or malformed Unicode. Without sanitization these cause:
//
//	ERROR: invalid byte sequence for encoding "UTF8": 0x00
//
// This module mirrors Augur's remove_null_characters_from_string() and the
// encode('UTF-8', errors='backslashreplace').decode('UTF-8', errors='ignore')
// pattern from augur/application/db/data_parse.py.
package db

import (
	"time"

	"github.com/aveloxis/aveloxis/internal/textclean"
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

// SanitizeText cleans a string for safe PostgreSQL TEXT storage: null bytes
// removed, invalid UTF-8 replaced with U+FFFD, C0 and C1 control characters
// removed except tab, newline and carriage return. The sanitizer itself is
// textclean.Sanitize (it moved there so that packages that may not import this
// one can use it); this name stays for the callers in this package.
func SanitizeText(s string) string {
	return textclean.Sanitize(s)
}
