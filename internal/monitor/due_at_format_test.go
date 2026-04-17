package monitor

import (
	"os"
	"strings"
	"testing"
)

// TestDueAtIncludesDate — the monitor table column "Due" must include the
// date, not just the time. A value like "15:21:27" is ambiguous when due_at
// is days away (the v0.16.6 realignment pushes due_at out by up to 7 days,
// so "15:21" with no date is unreadable). The existing "Last Run" column
// uses "Jan 2 15:04"; this test pins the "Due" column to the same shape so
// both columns are consistent.
func TestDueAtIncludesDate(t *testing.T) {
	src, err := os.ReadFile("monitor.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "j.DueAt.Format(")
	if idx < 0 {
		t.Fatal("cannot find DueAt.Format call in monitor.go HTML rendering")
	}
	// Consider only the non-JSON usage — the JSON field at line 88 uses
	// RFC3339, which is fine. The HTML rendering is the one we're pinning.
	// Find the *second* occurrence of DueAt.Format (skipping the RFC3339 one).
	rest := code[idx+len("j.DueAt.Format("):]
	secondIdx := strings.Index(rest, "j.DueAt.Format(")
	var fmtArg string
	if secondIdx >= 0 {
		// Second call is the table cell.
		tail := rest[secondIdx+len("j.DueAt.Format("):]
		if end := strings.Index(tail, ")"); end > 0 {
			fmtArg = tail[:end]
		}
	} else {
		// Only one call — use the one we found.
		tail := code[idx+len("j.DueAt.Format("):]
		if end := strings.Index(tail, ")"); end > 0 {
			fmtArg = tail[:end]
		}
	}

	// The format string for the *table cell* must contain a date token
	// ("Jan", "2006", or "01/02") so users can distinguish today from a
	// week from now. Pure time formats ("15:04:05", "15:04") are rejected.
	hasDate := strings.Contains(fmtArg, "Jan") ||
		strings.Contains(fmtArg, "2006") ||
		strings.Contains(fmtArg, "01/02") ||
		strings.Contains(fmtArg, "RFC3339")
	if !hasDate {
		t.Errorf("monitor.go: DueAt table-cell format %s has no date component — "+
			"users cannot tell if a repo is due today or a week out. Use a format "+
			"like \"Jan 2 15:04\" to match the Last Run column.", fmtArg)
	}
}
