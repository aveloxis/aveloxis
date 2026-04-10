package db

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// TestSchemaTableOrderingForLikeReferences verifies that any CREATE TABLE
// using "LIKE <table> INCLUDING ALL" only references tables that have already
// been defined earlier in the schema DDL. If a history table uses LIKE to
// clone a main table's structure, the main table must appear first.
//
// This catches the bug where repo_deps_scorecard_history was defined before
// repo_deps_scorecard, causing "relation does not exist" errors on fresh installs.
func TestSchemaTableOrderingForLikeReferences(t *testing.T) {
	data, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatalf("cannot read schema.sql: %v", err)
	}
	sql := string(data)
	lines := strings.Split(sql, "\n")

	// Track which tables have been created so far (by line order).
	created := make(map[string]bool)

	createRe := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+IF\s+NOT\s+EXISTS\s+(\S+)`)
	likeRe := regexp.MustCompile(`(?i)LIKE\s+(\S+)\s+INCLUDING\s+ALL`)

	for i, line := range lines {
		lineNum := i + 1
		trimmed := strings.TrimSpace(line)

		// Record table creation.
		if m := createRe.FindStringSubmatch(trimmed); m != nil {
			tableName := strings.ToLower(m[1])
			created[tableName] = true
			continue
		}

		// Check LIKE references.
		if m := likeRe.FindStringSubmatch(trimmed); m != nil {
			referenced := strings.ToLower(m[1])
			if !created[referenced] {
				t.Errorf("schema.sql line %d: LIKE %s INCLUDING ALL references table that has not been created yet",
					lineNum, referenced)
			}
		}
	}
}
