package main

import (
	"os"
	"strings"
	"testing"
)

// TestImportFoundationsCmdRegistered — source-contract: a new cobra subcommand
// that operators run to fetch CNCF + Apache project lists must be wired up in
// main.go. Without this wiring a caller can't discover the command via
// `aveloxis --help`.
func TestImportFoundationsCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Must be registered on the root command alongside the other subcommands.
	if !strings.Contains(src, "importFoundationsCmd(") {
		t.Error("main.go must invoke importFoundationsCmd(...) inside the root command setup (next to addRepoCmd etc.) so it is discoverable via `aveloxis --help`")
	}

	// The Use string is what appears in `--help`. It lives in the command's
	// own file — check both files so a future refactor that splits or merges
	// files doesn't silently break the user-facing slug.
	slug := `"import-foundations"`
	found := strings.Contains(src, slug)
	if !found {
		if data2, err := os.ReadFile("import_foundations.go"); err == nil {
			found = strings.Contains(string(data2), slug)
		}
	}
	if !found {
		t.Error(`importFoundationsCmd must use "import-foundations" as its Use string — the slug is the user-facing command name`)
	}
}

// TestImportFoundationsCmdFlags — verifies the key user-facing flags exist.
// These are the contract with the operator: `--dry-run` must preview only,
// `--cncf-only`/`--apache-only` must scope the run.
func TestImportFoundationsCmdFlags(t *testing.T) {
	data, err := os.ReadFile("import_foundations.go")
	if err != nil {
		// The command lives in its own file for clarity. If it doesn't exist
		// yet, the other test will already have failed with a clearer message.
		t.Skip("import_foundations.go not yet created")
	}
	src := string(data)
	for _, flag := range []string{"dry-run", "cncf-only", "apache-only", "priority"} {
		if !strings.Contains(src, flag) {
			t.Errorf("import_foundations.go must expose --%s flag — part of the documented operator contract", flag)
		}
	}
}
