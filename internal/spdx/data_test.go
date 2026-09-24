// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package spdx

import (
	"os"
	"os/exec"
	"strings"
	"testing"
)

// TestDataFileIsGeneratedAndComplete — the list is generated from SPDX
// license-list-data (scripts/gen_spdx_data), never hand-edited, and the
// official list only grows: a short file is a truncated refresh.
func TestDataFileIsGeneratedAndComplete(t *testing.T) {
	if len(licenses) < 700 {
		t.Errorf("embedded SPDX license set has %d entries, want >= 700; spdx_data.tsv looks truncated", len(licenses))
	}
	if len(exceptions) < 50 {
		t.Errorf("embedded SPDX exception set has %d entries; spdx_data.tsv looks truncated", len(exceptions))
	}
	osi := 0
	for _, l := range licenses {
		if l.osi {
			osi++
		}
	}
	if osi < 100 {
		t.Errorf("%d OSI-approved licenses; the osi column looks lost", osi)
	}
	for _, want := range []string{"GENERATED, do not hand-edit", "# Refresh", "go run ./scripts/gen_spdx_data"} {
		if !strings.Contains(dataRaw, want) {
			t.Errorf("spdx_data.tsv header must contain %q", want)
		}
	}
	if _, err := os.Stat("../../scripts/gen_spdx_data/main.go"); err != nil {
		t.Errorf("the generator the header names is missing: %v", err)
	}
}

// TestDataFileIsNotGitignored — the embedded file must be trackable (the
// v0.27.11 lesson: an ignore rule silently dropped committed fixtures).
func TestDataFileIsNotGitignored(t *testing.T) {
	cmd := exec.Command("git", "check-ignore", "-q", "spdx_data.tsv")
	if err := cmd.Run(); err == nil {
		t.Error("spdx_data.tsv is gitignored; the binary would embed a file CI checkouts don't have")
	}
}
