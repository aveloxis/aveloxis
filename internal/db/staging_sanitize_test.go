package db

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// nulEsc is the 6-char literal JSON NUL-escape sequence: backslash,
// u, 0, 0, 0, 0. Built from byte literals so this source file never
// contains the sequence directly — otherwise an over-eager editor or
// tool can decode it to an actual NUL byte and break the file.
var nulEsc = string([]byte{'\\', 'u', '0', '0', '0', '0'})

// doubleBackslashNulEsc is the 7-char wire sequence json.Marshal emits
// when a Go string's CONTENT is the 6 chars of nulEsc: the leading
// backslash gets escaped, yielding backslash-backslash-u-0-0-0-0.
var doubleBackslashNulEsc = "\\" + nulEsc

func readFileForTest(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestSanitizeJSONForJSONB_StripsNullEscape — the helper must strip
// the literal NUL-escape sequence from JSON bytes before they reach
// PostgreSQL. A single NUL-escape in any field value fails the whole
// JSONB insert with SQLSTATE 22P05 "unsupported Unicode escape
// sequence", killing 500-row batches on a single poisoned payload.
func TestSanitizeJSONForJSONB_StripsNullEscape(t *testing.T) {
	in := []byte(`{"body":"hello` + nulEsc + `world","id":42}`)
	got := sanitizeJSONForJSONB(in)
	if bytes.Contains(got, []byte(nulEsc)) {
		t.Errorf("expected NUL-escape removed; got %s", got)
	}
	if !bytes.Contains(got, []byte(`"id":42`)) {
		t.Errorf("sanitizer must not damage unrelated fields: %s", got)
	}
}

// TestSanitizeJSONForJSONB_PreservesValidContent — the scrubber must
// not touch other escape sequences (\n, \t, é …) or non-ASCII
// content.
func TestSanitizeJSONForJSONB_PreservesValidContent(t *testing.T) {
	in := []byte(`{"text":"hello\nworld\té","id":1}`)
	got := sanitizeJSONForJSONB(in)
	if !bytes.Equal(in, got) {
		t.Errorf("sanitizer must be a no-op when no NUL-escape is present.\n  in:  %s\n  got: %s", in, got)
	}
}

// TestSanitizeJSONForJSONB_StripsAllOccurrences — one row can carry
// many poisoned fields; every NUL-escape must go.
func TestSanitizeJSONForJSONB_StripsAllOccurrences(t *testing.T) {
	in := []byte(`{"a":"` + nulEsc + `","b":"x` + nulEsc + `y","c":"` + nulEsc + nulEsc + `"}`)
	got := sanitizeJSONForJSONB(in)
	if bytes.Contains(got, []byte(nulEsc)) {
		t.Errorf("every NUL-escape must be removed: %s", got)
	}
}

// TestStagingWriterStageCallsSanitizer — source-level contract: Stage
// must run the scrubber on the marshaled bytes before queuing the
// batch insert. Without this, the scrubber exists but is never
// called on the hot path.
func TestStagingWriterStageCallsSanitizer(t *testing.T) {
	src := readFileForTest(t, "staging.go")
	idx := strings.Index(src, "func (w *StagingWriter) Stage(")
	if idx < 0 {
		t.Fatal("cannot find StagingWriter.Stage")
	}
	fnBody := src[idx:]
	end := strings.Index(fnBody, "\n}\n")
	if end > 0 {
		fnBody = fnBody[:end]
	}
	if !strings.Contains(fnBody, "sanitizeJSONForJSONB") {
		t.Error("StagingWriter.Stage must call sanitizeJSONForJSONB on the " +
			"marshaled bytes before w.batch.Queue — otherwise a single " +
			"NUL-escape in a GitHub/GitLab payload kills the whole batch flush")
	}
}

// TestSanitizeJSONForJSONB_KeepsLiteralBackslashUSequence — the root
// cause of the v0.18.12 22P02 swarm in staging flushes.
//
// When an API payload carries a Go string whose CONTENT is the 6
// literal characters of nulEsc (e.g. a user pasted that exact text
// into an issue body, PR comment, or commit message), json.Marshal
// escapes the leading backslash and emits doubleBackslashNulEsc on
// the wire.
//
// The pre-fix sanitizer did a context-free bytes.ReplaceAll, matched
// its 6-byte needle starting at the SECOND backslash of that
// sequence, and stripped 6 bytes — leaving a lone backslash before
// the closing quote, which is an unterminated JSON string.
// PostgreSQL rejected the whole 500-row batch with SQLSTATE 22P02
// "invalid input syntax for type json".
//
// Correct behavior: a NUL-escape whose introducing backslash is
// itself ESCAPED (preceded by an odd-length run of backslashes)
// represents 6 literal characters inside a JSON string value. It is
// content, not an escape, and must be preserved verbatim.
func TestSanitizeJSONForJSONB_KeepsLiteralBackslashUSequence(t *testing.T) {
	in := []byte(`{"body":"` + doubleBackslashNulEsc + `","id":7}`)
	got := sanitizeJSONForJSONB(in)

	want := []byte(`"body":"` + doubleBackslashNulEsc + `"`)
	if !bytes.Contains(got, want) {
		t.Errorf("escaped-backslash + u0000 is content, not an escape; "+
			"sanitizer must leave it intact.\n  in:  %s\n  got: %s", in, got)
	}

	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Errorf("sanitized output must still be valid JSON; got parse error %v on %s", err, got)
	}
}

// TestSanitizeJSONForJSONB_MixedEscapeAndLiteral — a single payload
// can contain BOTH a real NUL-escape (NUL char, rejected by PG JSONB
// with 22P05 and which must be stripped) and a doubleBackslashNulEsc
// escaped-backslash literal (content, which must be preserved to
// avoid 22P02). A correct implementation distinguishes them by
// counting the run of backslashes introducing each candidate.
func TestSanitizeJSONForJSONB_MixedEscapeAndLiteral(t *testing.T) {
	in := []byte(`{"a":"x` + nulEsc + `y","b":"` + doubleBackslashNulEsc + `"}`)
	got := sanitizeJSONForJSONB(in)

	if !bytes.Contains(got, []byte(`"a":"xy"`)) {
		t.Errorf("real NUL-escape in field a must be stripped, yielding xy: got %s", got)
	}
	if !bytes.Contains(got, []byte(`"b":"`+doubleBackslashNulEsc+`"`)) {
		t.Errorf("escaped-backslash literal in field b must be preserved: got %s", got)
	}
	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Errorf("sanitized mixed-case output must parse as JSON; got %v on %s", err, got)
	}
}

// TestSanitizeJSONForJSONB_TripleBackslashBeforeU0000 — stress the
// backslash-run counting rule: runs of length 0, 2, 4, ... mean the
// NUL-escape IS an escape (strip it). Runs of length 1, 3, 5, ...
// mean it is content (keep it). A three-backslash source means
// "escaped backslash + real NUL escape": strip the NUL, keep the
// escaped backslash.
func TestSanitizeJSONForJSONB_TripleBackslashBeforeU0000(t *testing.T) {
	tripleBackslashNulEsc := "\\\\" + nulEsc // 3 backslashes + u0000
	in := []byte(`{"x":"` + tripleBackslashNulEsc + `"}`)
	got := sanitizeJSONForJSONB(in)

	wantFragment := []byte(`"x":"\\"`) // Go source: 2 backslashes inside the string
	if !bytes.Contains(got, wantFragment) {
		t.Errorf("triple-backslash + u0000 must become double-backslash "+
			"(escaped backslash remains, real NUL-escape stripped): got %s", got)
	}
	var obj map[string]any
	if err := json.Unmarshal(got, &obj); err != nil {
		t.Errorf("output must parse as valid JSON: got %v on %s", err, got)
	}
}

// TestSanitizeJSONForJSONB_RoundtripOnMarshaledRealPayload — the
// production-shaped regression. Marshal a Go struct whose string
// field contains the 6 literal characters of nulEsc as CONTENT,
// run it through the sanitizer, then Unmarshal. A correct round-trip
// must yield valid JSON AND preserve the original string exactly.
// Anything less is either data loss or another 22P02 waiting to fire.
func TestSanitizeJSONForJSONB_RoundtripOnMarshaledRealPayload(t *testing.T) {
	type payload struct {
		Body string `json:"body"`
		ID   int    `json:"id"`
	}
	original := payload{
		Body: "before " + nulEsc + " after",
		ID:   42,
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	sanitized := sanitizeJSONForJSONB(data)

	var decoded payload
	if err := json.Unmarshal(sanitized, &decoded); err != nil {
		t.Fatalf("sanitized output failed to parse as JSON (this is the 22P02 Postgres sees):"+
			"\n  err:       %v\n  marshaled: %s\n  sanitized: %s", err, data, sanitized)
	}
	if decoded.Body != original.Body {
		t.Errorf("body content corrupted by sanitizer:\n  before: %q\n  after:  %q", original.Body, decoded.Body)
	}
	if decoded.ID != original.ID {
		t.Errorf("unrelated field lost: got id=%d, want %d", decoded.ID, original.ID)
	}
}
