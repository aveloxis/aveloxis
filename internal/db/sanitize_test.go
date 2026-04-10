package db

import (
	"testing"
	"time"
)

func TestSanitizeText_NullBytes(t *testing.T) {
	input := "hello\x00world"
	got := SanitizeText(input)
	if got != "helloworld" {
		t.Errorf("SanitizeText(%q) = %q, want %q", input, got, "helloworld")
	}
}

func TestSanitizeText_InvalidUTF8(t *testing.T) {
	input := "hello\xfe\xffworld"
	got := SanitizeText(input)
	if got != "hello\uFFFD\uFFFDworld" {
		t.Errorf("SanitizeText with invalid UTF-8 = %q, want replacement chars", got)
	}
}

func TestSanitizeText_ControlChars(t *testing.T) {
	// \x01 (SOH) should be stripped, \n and \t should be kept.
	input := "line1\x01\nline2\t"
	got := SanitizeText(input)
	if got != "line1\nline2\t" {
		t.Errorf("SanitizeText(%q) = %q, want %q", input, got, "line1\nline2\t")
	}
}

func TestSanitizeText_DEL(t *testing.T) {
	input := "abc\x7fdef"
	got := SanitizeText(input)
	if got != "abcdef" {
		t.Errorf("SanitizeText(%q) = %q, want %q", input, got, "abcdef")
	}
}

func TestSanitizeText_CleanString(t *testing.T) {
	input := "This is a perfectly normal string with emoji 🎉 and unicode café"
	got := SanitizeText(input)
	if got != input {
		t.Errorf("clean string was modified: %q", got)
	}
}

func TestSanitizeText_Empty(t *testing.T) {
	if SanitizeText("") != "" {
		t.Error("empty string should return empty")
	}
}

func TestSanitizeText_MultipleNullBytes(t *testing.T) {
	input := "\x00\x00\x00"
	got := SanitizeText(input)
	if got != "" {
		t.Errorf("all-null string should be empty, got %q", got)
	}
}

func TestSanitizeText_MixedBadContent(t *testing.T) {
	// Simulates real-world bad content from GitHub issues.
	input := "Bug report\x00\n\nSteps:\x01\n1. Do thing\x7f\n2. See \xfe error"
	got := SanitizeText(input)
	want := "Bug report\n\nSteps:\n1. Do thing\n2. See \uFFFD error"
	if got != want {
		t.Errorf("SanitizeText mixed = %q, want %q", got, want)
	}
}

// ============================================================
// NullTime tests
// ============================================================

func TestNullTime_ZeroReturnsNil(t *testing.T) {
	var zero time.Time
	if NullTime(zero) != nil {
		t.Error("NullTime(zero) should return nil")
	}
}

func TestNullTime_RealTimePassesThrough(t *testing.T) {
	ts := time.Date(2024, 3, 15, 10, 30, 0, 0, time.UTC)
	got := NullTime(ts)
	if got == nil {
		t.Fatal("NullTime(real timestamp) should not return nil")
	}
	if !got.Equal(ts) {
		t.Errorf("NullTime() = %v, want %v", *got, ts)
	}
}

func TestNullTime_EpochIsNotZero(t *testing.T) {
	// Unix epoch (1970-01-01) is a valid timestamp, not a zero value.
	epoch := time.Unix(0, 0)
	got := NullTime(epoch)
	if got == nil {
		t.Error("NullTime(Unix epoch) should not return nil — epoch is a real timestamp")
	}
}

func TestNullTime_HistoricalDatePassesThrough(t *testing.T) {
	// A timestamp from 2008 (early GitHub days) should pass through.
	old := time.Date(2008, 4, 10, 0, 0, 0, 0, time.UTC)
	got := NullTime(old)
	if got == nil {
		t.Error("NullTime(2008-04-10) should not return nil")
	}
}

func TestNullTime_GoZeroValue(t *testing.T) {
	// Explicitly construct what Go's zero time.Time looks like — year 1, month 1, day 1.
	// This is the value that causes BC-era garbage in PostgreSQL.
	goZero := time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)
	if NullTime(goZero) != nil {
		t.Error("NullTime(year 0001 Go zero) should return nil")
	}
}
