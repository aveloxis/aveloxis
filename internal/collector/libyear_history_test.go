package collector

import (
	"os"
	"strings"
	"testing"
)

// TestLibyearHistoryTableExists verifies schema.sql has the libyear history table.
func TestLibyearHistoryTableExists(t *testing.T) {
	src, err := os.ReadFile("../db/schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "repo_deps_libyear_history") {
		t.Error("schema.sql must create repo_deps_libyear_history table")
	}
}

// TestRotateLibyearToHistoryExists verifies the history rotation method exists.
func TestRotateLibyearToHistoryExists(t *testing.T) {
	src, err := os.ReadFile("../db/history.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "RotateLibyearToHistory") {
		t.Error("history.go must contain RotateLibyearToHistory method")
	}
	if !strings.Contains(code, "repo_deps_libyear_history") {
		t.Error("RotateLibyearToHistory must reference repo_deps_libyear_history")
	}
}

// TestLibyearRotationCalledBeforeInsert verifies the analysis phase rotates
// libyear data to history before inserting fresh data. Without rotation, old
// rows with empty licenses persist and ON CONFLICT DO NOTHING skips the update.
func TestLibyearRotationCalledBeforeInsert(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	idx := strings.Index(code, "func (ac *AnalysisCollector) scanLibyear(")
	if idx < 0 {
		t.Fatal("cannot find scanLibyear function")
	}
	fnBody := code[idx : idx+800]

	if !strings.Contains(fnBody, "RotateLibyearToHistory") {
		t.Error("scanLibyear must call RotateLibyearToHistory before inserting fresh data")
	}
}
