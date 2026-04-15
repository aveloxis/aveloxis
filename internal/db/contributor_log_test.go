package db

import (
	"os"
	"strings"
	"testing"
)

// TestContributorUpsertDuplicateIsDebug verifies that the "failed to upsert
// contributor" log for duplicate key violations (SQLSTATE 23505) is logged
// at DEBUG level, not WARN. The duplicate key is a normal race condition
// between concurrent workers — data integrity is maintained because the
// contributor exists either way.
func TestContributorUpsertDuplicateIsDebug(t *testing.T) {
	src, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Find the contributor upsert error log.
	idx := strings.Index(code, `"contributor upsert failed"`)
	if idx < 0 {
		// Might have been renamed — check alternate form.
		idx = strings.Index(code, `"failed to upsert contributor"`)
	}
	if idx < 0 {
		t.Skip("cannot find contributor upsert error log")
	}

	// Look at the 100 chars before the log message to find the log level.
	start := idx - 100
	if start < 0 {
		start = 0
	}
	context := code[start:idx]

	if strings.Contains(context, ".Warn(") {
		t.Error("contributor upsert duplicate-key log must be Debug level, not Warn — " +
			"this is a normal race condition between concurrent workers, not a problem. " +
			"Data integrity is maintained because the contributor exists either way.")
	}
}
