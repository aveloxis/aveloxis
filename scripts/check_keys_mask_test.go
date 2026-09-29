// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestCheckKeysNamesKeysLikeTheLogs — PR #220 CI review F1: check-keys.sh
// masked a key as its first and last 4 characters (4 secret characters, and
// no way to match a row to a log line). Its mask() must print exactly
// platform.TokenHash, the one name the logs use (SR-17). The function is
// cut from the script and run in bash, so the pin is the behavior.
func TestCheckKeysNamesKeysLikeTheLogs(t *testing.T) {
	if _, err := exec.LookPath("bash"); err != nil {
		t.Skip("bash not installed")
	}
	src := srctest.Read(t, "scripts/check-keys.sh")
	start := strings.Index(src, "\nmask() {")
	if start < 0 {
		t.Fatal("check-keys.sh has no mask() function")
	}
	end := strings.Index(src[start:], "\n}\n")
	if end < 0 {
		t.Fatal("mask() is not terminated")
	}
	fn := src[start : start+end+3]
	// One token per type marker (the marker-list test keeps the script's
	// list equal to the Go one), plus no marker, a bare marker and a short
	// token.
	toks := []string{"ZqXwRtYuKmNbVcLpQsWeDfGhJkLzXcVbNmQwZqXw", "ghp_", "ab"}
	for _, m := range []string{"github_pat_", "ghp_", "gho_", "ghu_", "ghs_", "ghr_", "glpat-", "gloas-", "gldt-", "glrt-", "glptt-", "glft-"} {
		toks = append(toks, m+"ZqXwRtYuKmNbVcLpQsWe")
	}
	// Each hashing branch runs with a PATH holding only that tool, so the
	// fallback is exercised wherever both are installed (review round 2).
	ran := 0
	for _, tool := range []string{"sha256sum", "shasum"} {
		bin, err := exec.LookPath(tool)
		if err != nil {
			t.Logf("%s not installed: its branch is not exercised here", tool)
			continue
		}
		dir := t.TempDir()
		if err := os.Symlink(bin, filepath.Join(dir, tool)); err != nil {
			t.Fatal(err)
		}
		ran++
		for _, tok := range toks {
			cmd := exec.Command("bash", "-c", fn+"\nmask \"$1\"", "mask", tok)
			cmd.Env = []string{"PATH=" + dir}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: mask %q: %v\n%s", tool, tok, err, out)
			}
			if got, want := strings.TrimSpace(string(out)), platform.TokenHash(tok); got != want {
				t.Errorf("%s: check-keys.sh mask(%q) = %q, want the logs' token_hash %q", tool, tok, got, want)
			}
		}
	}
	if ran == 0 {
		t.Skip("neither sha256sum nor shasum is installed")
	}
}
