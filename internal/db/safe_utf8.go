// Helper: strip invalid UTF-8 bytes from a string before sending it
// to Postgres as a TEXT column value. Postgres rejects whole INSERT
// statements with `ERROR: invalid byte sequence for encoding "UTF8"`
// when even one byte in any parameter doesn't validate, and the
// rejection is per-statement: a 500-row batch dies on a single
// poisoned row.
//
// The most common source of invalid UTF-8 in aveloxis is GitHub
// profile fields (cntrb_company, cntrb_full_name) where users
// occasionally paste binary content into their bio. The byte 0x89
// (the start of a PNG signature) showed up repeatedly in production
// logs from 2026-05-02 inside contributor_affiliations INSERTs.
//
// safeUTF8 is intentionally cheap: validates first and returns the
// input string unchanged when it's already valid (zero allocation).
// Only allocates when scrubbing is actually needed.

package db

import "unicode/utf8"

// safeUTF8 returns a UTF-8-valid version of s, replacing any invalid
// byte sequences with the Unicode replacement character (U+FFFD).
// Returns s unchanged when it's already valid (no allocation).
func safeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	// strings.ToValidUTF8 substitutes "" for each invalid byte —
	// equivalent to deletion. We use U+FFFD (replacement char) to
	// preserve string length and signal that scrubbing happened, in
	// case anyone reads the column value back later.
	return string([]rune(s)) // ranging over a string yields U+FFFD for invalid bytes
}
