package db

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func readFileForTest(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// TestSanitizeJSONForJSONB_StripsNullEscape — the helper must strip literal
// `\u0000` sequences from JSON bytes before they reach PostgreSQL. A single
// `\u0000` in any field value fails the whole JSONB insert with
// SQLSTATE 22P05 "unsupported Unicode escape sequence", killing 500-row
// batches on a single poisoned GitHub/GitLab payload.
func TestSanitizeJSONForJSONB_StripsNullEscape(t *testing.T) {
	in := []byte(`{"body":"hello\u0000world","id":42}`)
	got := sanitizeJSONForJSONB(in)
	if bytes.Contains(got, []byte(`\u0000`)) {
		t.Errorf("expected \\u0000 removed; got %s", got)
	}
	// Must remain valid-ish JSON shape (outer braces, other fields intact).
	if !bytes.Contains(got, []byte(`"id":42`)) {
		t.Errorf("sanitizer must not damage unrelated fields: %s", got)
	}
}

// TestSanitizeJSONForJSONB_PreservesValidContent — scrubber must not touch
// other escape sequences (\n, \t, \u00e9 …) or non-ASCII content.
func TestSanitizeJSONForJSONB_PreservesValidContent(t *testing.T) {
	in := []byte(`{"text":"hello\nworld\t\u00e9","id":1}`)
	got := sanitizeJSONForJSONB(in)
	if !bytes.Equal(in, got) {
		t.Errorf("sanitizer must be a no-op when no \\u0000 present.\n  in:  %s\n  got: %s", in, got)
	}
}

// TestSanitizeJSONForJSONB_StripsAllOccurrences — one row can carry many
// poisoned fields; every one must go.
func TestSanitizeJSONForJSONB_StripsAllOccurrences(t *testing.T) {
	in := []byte(`{"a":"\u0000","b":"x\u0000y","c":"\u0000\u0000"}`)
	got := sanitizeJSONForJSONB(in)
	if bytes.Contains(got, []byte(`\u0000`)) {
		t.Errorf("every \\u0000 must be removed: %s", got)
	}
}

// TestStagingWriterStageCallsSanitizer — source-level contract: Stage must
// run the scrubber on the marshaled bytes before queuing the batch insert.
// Without this, the scrubber exists but is never called on the hot path.
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
			"\\u0000 in a GitHub/GitLab payload kills the whole batch flush")
	}
}
