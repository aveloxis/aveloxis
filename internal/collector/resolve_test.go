package collector

import (
	"os"
	"strings"
	"testing"
)

// TestResolveUserLogsErrors verifies that resolveUser logs errors instead of
// silently swallowing them. The original code returned nil on error with no
// logging, which hid the SQL syntax bug that caused 131K+ NULL cntrb_id
// messages. Errors in contributor resolution should always be visible.
func TestResolveUserLogsErrors(t *testing.T) {
	data, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (p *Processor) resolveUser(")
	if idx < 0 {
		t.Fatal("cannot find resolveUser function")
	}
	fnBody := src[idx : idx+500]

	// Must log the error, not just return nil.
	if !strings.Contains(fnBody, "logger") && !strings.Contains(fnBody, "Warn") {
		t.Error("resolveUser must log errors when Resolve fails — silent nil returns hid the SQL syntax bug that caused 131K NULL cntrb_id messages")
	}
}
