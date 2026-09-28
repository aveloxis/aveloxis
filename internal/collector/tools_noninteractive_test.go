// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"
)

// TestRunToolCommandIsNonInteractive (batch 4a review round 14): the tool
// installs run in their own process group (groupKilled), so a child that
// opens the controlling terminal to prompt — pip's getpass for an
// authenticated index, git's credential prompt under `go install` — is
// stopped by SIGTTIN instead of answered, and the operator watches
// ToolInstallBound() run out. Every tool command therefore carries the two
// ecosystems' no-prompt variables so a prompt is an immediate, named
// failure, and a caller's own environment is kept (round 15: Homebrew's
// NONINTERACTIVE was dropped — `brew install` never reads it).
func TestRunToolCommandIsNonInteractive(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  []string
	}{
		{"the process environment", nil},
		{"a caller's own environment", []string{"AVELOXIS_PROBE=kept"}},
	} {
		cmd := exec.CommandContext(context.Background(), "sh", "-c", `printf '%s|%s|%s' "$PIP_NO_INPUT" "$GIT_TERMINAL_PROMPT" "$AVELOXIS_PROBE"`)
		cmd.Env = tc.env
		var out bytes.Buffer
		cmd.Stdout = &out
		if err := runToolCommand(context.Background(), cmd); err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		want := "1|0|"
		if tc.env != nil {
			want += "kept"
		}
		if got := strings.TrimSpace(out.String()); got != want {
			t.Errorf("%s: the tool command's environment answered %q; want %q (PIP_NO_INPUT, GIT_TERMINAL_PROMPT, then the caller's variable)", tc.name, got, want)
		}
	}
}
