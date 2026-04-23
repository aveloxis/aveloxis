// Package scripts_test verifies shell scripts under ./scripts/ stay sound.
//
// Shell scripts aren't covered by `go test ./...` by default; this file
// promotes the smoke.sh script to a first-class test target so a broken
// script (syntax error, deleted file) fails CI the same way a broken Go
// test does. It does NOT execute the script end-to-end — that requires a
// live Postgres + API keys and is the job of the CI workflow that calls
// the script directly. It only guards "does this script still parse".
package scripts_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// TestSmokeScriptParses runs `bash -n scripts/smoke.sh` from the repo root.
// A regression that breaks bash syntax (missing quote, unbalanced if/fi,
// mis-edited heredoc) fails this test before it reaches CI.
func TestSmokeScriptParses(t *testing.T) {
	// Locate the repo root. Tests run from the package dir (./scripts/) but
	// we want to check-parse the script at its actual path.
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	script := filepath.Join(wd, "smoke.sh")
	if _, err := os.Stat(script); err != nil {
		t.Fatalf("smoke.sh not found at %s: %v", script, err)
	}

	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("bash not found on PATH; skipping smoke.sh syntax check")
	}

	out, err := exec.Command(bash, "-n", script).CombinedOutput()
	if err != nil {
		t.Fatalf("smoke.sh failed bash -n syntax check:\n%s\nerror: %v", out, err)
	}
}

// TestSmokeScriptIsExecutable guards the chmod +x bit. A non-executable
// script fails with a cryptic "Permission denied" in CI; this test
// surfaces the issue immediately.
func TestSmokeScriptIsExecutable(t *testing.T) {
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	info, err := os.Stat(filepath.Join(wd, "smoke.sh"))
	if err != nil {
		t.Fatalf("smoke.sh not found: %v", err)
	}
	if info.Mode().Perm()&0o111 == 0 {
		t.Errorf("smoke.sh is not executable (mode=%v); run: chmod +x scripts/smoke.sh", info.Mode())
	}
}
