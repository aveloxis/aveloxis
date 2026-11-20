// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestFacadeCloneSetsLFSSkip verifies that the bare clone in facade.go sets
// GIT_LFS_SKIP_SMUDGE=1 to prevent LFS smudge filter failures when repos
// have LFS objects that are no longer available on the server.
func TestFacadeCloneSetsLFSSkip(t *testing.T) {
	src, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GIT_LFS_SKIP_SMUDGE") {
		t.Error("facade.go must set GIT_LFS_SKIP_SMUDGE=1 on git clone commands " +
			"to prevent failures when LFS objects are missing from the server")
	}
}

// TestFacadeCloneSetsTerminalPrompt verifies that the bare clone sets
// GIT_TERMINAL_PROMPT=0 to prevent macOS keychain prompts when repos
// are deleted/private and git tries to ask for credentials.
func TestFacadeCloneSetsTerminalPrompt(t *testing.T) {
	src, err := os.ReadFile("facade.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "GIT_TERMINAL_PROMPT") {
		t.Error("facade.go must set GIT_TERMINAL_PROMPT=0 on git clone commands " +
			"to prevent macOS keychain prompts (errSecInteractionNotAllowed) " +
			"when running as a background process")
	}
}

// TestAnalysisCloneSetsLFSSkip verifies the analysis temp clone sets
// git env vars (via gitCloneEnv) to skip LFS. This is where most LFS
// failures occur because the full checkout triggers smudge filters.
func TestAnalysisCloneSetsLFSSkip(t *testing.T) {
	src, err := os.ReadFile("analysis.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	// The analysis clone must use gitCloneEnv() (defined in facade.go)
	// which sets GIT_LFS_SKIP_SMUDGE=1 and GIT_TERMINAL_PROMPT=0.
	if !strings.Contains(code, "gitCloneEnv") {
		t.Error("analysis.go must set cmd.Env = gitCloneEnv() on the temp clone " +
			"to prevent LFS smudge filter failures — dependency scanners and " +
			"ScanCode only need text source files, not binary LFS objects")
	}
}
