// Source-contract test for v0.19.2 Fix #3: PopulateAffiliations
// must scrub non-UTF-8 bytes from cntrb_company values before
// passing them to the contributor_affiliations INSERT. Production
// logs from 2026-05-02 showed repeated:
//
//   ERROR:  invalid byte sequence for encoding "UTF8": 0x89
//   CONTEXT:  unnamed portal parameter $2
//   STATEMENT:  INSERT INTO aveloxis_data.contributor_affiliations ...
//
// 0x89 is the start of a PNG file signature; the byte was getting
// into cntrb_company through some contributor's GitHub profile bio
// containing arbitrary binary content. Postgres rejects the INSERT
// with the encoding error, but the scrubber lets the rest of the
// affiliation cycle proceed.

package db

import (
	"strings"
	"testing"
	"unicode/utf8"
)

// TestPopulateAffiliationsScrubsUTF8 pins that the function applies
// safeUTF8 (or equivalent) to ca_domain and ca_affiliation before
// passing them to the INSERT.
func TestPopulateAffiliationsScrubsUTF8(t *testing.T) {
	src := mustReadStoreSource(t, "affiliations_populate.go")
	body := extractBatchFunc(src, "PopulateAffiliations")
	if body == "" {
		t.Fatal("could not locate PopulateAffiliations body")
	}

	// Look for a call to a UTF-8 sanitizer. Accept various names.
	hasScrubber := strings.Contains(body, "safeUTF8") ||
		strings.Contains(body, "SafeUTF8") ||
		strings.Contains(body, "utf8.ValidString") ||
		strings.Contains(body, "ToValidUTF8")
	if !hasScrubber {
		t.Error("PopulateAffiliations must scrub invalid UTF-8 from ca_domain and ca_affiliation before " +
			"the INSERT — Postgres rejects the whole statement when even a single 0x89-style byte " +
			"slips through, and the production loop hits this every cycle.")
	}
}

// TestSafeUTF8HelperBehavior is a runtime test of the helper. It
// MUST strip or replace bytes that aren't valid UTF-8 so the
// returned string can safely round-trip through Postgres.
func TestSafeUTF8HelperBehavior(t *testing.T) {
	// Construct a string with an invalid UTF-8 byte (0x89 — start of
	// PNG signature, also the failing byte in production logs).
	bad := "Acme " + string([]byte{0x89, 0x50, 0x4E, 0x47}) + " Corp"
	if utf8.ValidString(bad) {
		t.Fatal("test setup invariant violated: 'bad' should not be valid UTF-8")
	}

	got := safeUTF8(bad)
	if !utf8.ValidString(got) {
		t.Errorf("safeUTF8 must return a valid UTF-8 string. Input: %q (bytes %v); got: %q (bytes %v)",
			bad, []byte(bad), got, []byte(got))
	}

	// Valid input must pass through unchanged (no allocation cost on
	// the common path is nice but not required).
	good := "Acme Corp"
	if safeUTF8(good) != good {
		t.Errorf("safeUTF8(valid input) must be a no-op: got %q, want %q", safeUTF8(good), good)
	}
}
