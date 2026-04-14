package collector

import (
	"os"
	"strings"
	"testing"
)

// TestScanDependenciesSkipsSymlinks verifies the filepath.Walk callback in
// scanDependencies rejects symlinks. A malicious repo can contain a symlink
// like requirements.txt -> /etc/passwd; without this check, os.ReadFile
// follows the symlink and stores host file contents as dependency names.
func TestScanDependenciesSkipsSymlinks(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The walk callback must check for symlinks.
	if !strings.Contains(code, "ModeSymlink") {
		t.Error("analysis.go must check for os.ModeSymlink in filepath.Walk callbacks " +
			"to prevent symlink traversal attacks — a malicious repo can symlink " +
			"requirements.txt to /etc/passwd and have host file contents stored as dependencies")
	}
}

// TestScanLibyearSkipsSymlinks verifies the scanLibyear walk also rejects symlinks.
func TestScanLibyearSkipsSymlinks(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// Count occurrences of ModeSymlink — need at least 2 (one per walk).
	count := strings.Count(code, "ModeSymlink")
	if count < 2 {
		t.Errorf("analysis.go must check ModeSymlink in both scanDependencies and scanLibyear walks, "+
			"found %d occurrences (need at least 2)", count)
	}
}
