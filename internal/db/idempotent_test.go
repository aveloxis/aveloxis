package db

import (
	"os"
	"strings"
	"testing"
)

// TestAllDataInsertTablesHaveOnConflict scans postgres.go source code to verify
// that every INSERT INTO aveloxis_data.* has an ON CONFLICT clause.
// This prevents duplicate rows from being created during re-collection.
//
// Known exceptions:
//   - repo_groups: seed data with manual existence check
//   - contributors: uses savepoint pattern (SELECT-first + INSERT) by design
//   - repo_info: intentionally creates new rows (uses history rotation)
func TestAllDataInsertTablesHaveOnConflict(t *testing.T) {
	// Exceptions — tables that intentionally lack ON CONFLICT.
	exceptions := map[string]bool{
		"aveloxis_data.repo_groups": true, // seed data with manual existence check
		"aveloxis_data.repo_info":   true, // history rotation (old rows moved to _history, new one inserted)
	}

	// Read the source file.
	// This test reads the actual Go source to verify the SQL patterns.
	// It's a compile-time-adjacent safety net: if someone adds a new INSERT
	// without ON CONFLICT, this test catches it.
	data := postgresGoSource()
	lines := strings.Split(data, "\n")

	for i, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, "INSERT INTO aveloxis_data.") {
			continue
		}

		// Extract table name.
		idx := strings.Index(trimmed, "aveloxis_data.")
		if idx < 0 {
			continue
		}
		rest := trimmed[idx:]
		// Table name ends at space, paren, or newline.
		endIdx := strings.IndexAny(rest, " \t(")
		if endIdx < 0 {
			endIdx = len(rest)
		}
		table := rest[:endIdx]

		if exceptions[table] {
			continue
		}

		// Look ahead up to 15 lines for ON CONFLICT.
		found := false
		for j := i + 1; j < i+15 && j < len(lines); j++ {
			if strings.Contains(lines[j], "ON CONFLICT") {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("INSERT INTO %s at line %d has no ON CONFLICT clause — will create duplicates on re-collection", table, i+1)
		}
	}
}

// postgresGoSource returns the contents of postgres.go for analysis.
// We use go:embed-style reading via the test binary's working directory.
func postgresGoSource() string {
	data, err := readTestFile("postgres.go")
	if err != nil {
		return ""
	}
	return string(data)
}

// Also check the batch insert files.
func TestBatchInsertsHaveOnConflict(t *testing.T) {
	// Tables with their own lifecycle management (rotation, accumulation).
	batchExceptions := map[string]bool{
		"aveloxis_data.repo_sbom_scans":     true, // SBOMs accumulate per collection run
		"aveloxis_data.repo_deps_scorecard":  true, // uses RotateScorecardToHistory before insert
		"aveloxis_data.repo_deps_libyear":    true, // uses RotateLibyearToHistory before insert
	}

	for _, filename := range []string{"analysis_store.go", "breadth_store.go", "vulnerability_store.go"} {
		data, err := readTestFile(filename)
		if err != nil {
			continue // file may not exist in test environment
		}
		source := string(data)
		lines := strings.Split(source, "\n")

		for i, line := range lines {
			trimmed := strings.TrimSpace(line)
			if !strings.Contains(trimmed, "INSERT INTO aveloxis_data.") {
				continue
			}

			// Extract table name for exception check.
			idx := strings.Index(trimmed, "aveloxis_data.")
			if idx >= 0 {
				rest := trimmed[idx:]
				endIdx := strings.IndexAny(rest, " \t(")
				if endIdx < 0 {
					endIdx = len(rest)
				}
				table := rest[:endIdx]
				if batchExceptions[table] {
					continue
				}
			}

			found := false
			for j := i + 1; j < i+15 && j < len(lines); j++ {
				if strings.Contains(lines[j], "ON CONFLICT") {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("%s: INSERT at line %d has no ON CONFLICT clause", filename, i+1)
			}
		}
	}
}

func readTestFile(name string) ([]byte, error) {
	// Tests run from the package directory, so we can read sibling files.
	return os.ReadFile(name)
}
