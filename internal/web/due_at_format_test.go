package web

import (
	"os"
	"strings"
	"testing"
)

// TestWebQueueDueAtIncludesDate — mirror of TestDueAtIncludesDate in the
// monitor package. The web server's queue table renders due_at in the same
// HH:MM:SS-only format, which is unreadable once due_at is days out (post
// v0.16.6 realignment). Pin it to a format that contains a date token.
func TestWebQueueDueAtIncludesDate(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "row.Due = j.DueAt.Format(")
	if idx < 0 {
		t.Fatal("cannot find row.Due = j.DueAt.Format(...) in web/server.go")
	}
	tail := code[idx+len("row.Due = j.DueAt.Format("):]
	end := strings.Index(tail, ")")
	if end < 0 {
		t.Fatal("malformed DueAt.Format call")
	}
	fmtArg := tail[:end]

	hasDate := strings.Contains(fmtArg, "Jan") ||
		strings.Contains(fmtArg, "2006") ||
		strings.Contains(fmtArg, "01/02")
	if !hasDate {
		t.Errorf("web/server.go: row.Due format %s has no date component — "+
			"use \"Jan 2 15:04\" to match the Last Run column so the user can "+
			"tell today's repos from next-week's repos.", fmtArg)
	}
}
