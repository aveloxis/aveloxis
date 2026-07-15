// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// v0.27.6 — behavioral guard on the WRITE-TIME bound of the per-repo
// failure artifacts. The June 2026 incident left repo_<id>_stderr.log
// files up to 9.5 GB on disk (written by a pre-v0.25.28 binary). The
// v0.25.28 headTailBuffer already bounds the capture far below the
// work order's 50 MB ceiling (1 MB head + 256 KB tail + truncation
// marker); this test pins the END-TO-END property — subprocess spews
// hundreds of MB, the on-disk file stays ~1.3 MB and carries the
// marker — so no refactor of the capture plumbing can silently
// reintroduce unbounded files.

func TestWriteFailureArtifactsIsBoundedAtWriteTime(t *testing.T) {
	dir := t.TempDir()
	w := NewScancodeWorker(nil, gateTestLogger(), ScancodeWorkerOptions{CloneDir: dir})

	stderrTail := &tailBuffer{cap: scancodeStderrTailBytes}
	stdoutTail := &tailBuffer{cap: scancodeStderrTailBytes}
	stderrFull := &headTailBuffer{headCap: scancodeFailHeadBytes, tailCap: scancodeFailTailBytes}
	stdoutFull := &headTailBuffer{headCap: scancodeFailHeadBytes, tailCap: scancodeFailTailBytes}

	// Simulate a libmagic warning storm: ~256 MB streamed through the
	// capture in 64 KB chunks (the real incident streams were 9.5 GB;
	// the bound is size-independent so 256 MB proves the property).
	chunk := bytes.Repeat([]byte("/usr/share/misc/magic.mgc, 3673: Warning: offset `' invalid\n"), 1100) // ~64 KB
	const chunks = 4096                                                                                  // × 64 KB ≈ 256 MB
	for i := 0; i < chunks; i++ {
		if _, err := stderrFull.Write(chunk); err != nil {
			t.Fatal(err)
		}
		_, _ = stderrTail.Write(chunk)
	}

	ex := &scanExecution{
		pid:        1234,
		outputPath: filepath.Join(dir, "results.json"),
		waitErr:    errors.New("exit status 1"),
		stderrTail: stderrTail,
		stdoutTail: stdoutTail,
		stderrFull: stderrFull,
		stdoutFull: stdoutFull,
	}
	w.writeFailureArtifacts(db.ScancodeJob{RepoID: 42, RepoOwner: "o", RepoName: "r"}, ex)

	logPath := filepath.Join(dir, "repo_42_stderr.log")
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("failure artifact not written: %v", err)
	}
	// Hard ceiling from the work order is 50 MB; the actual bound is
	// head (1 MB) + tail (256 KB) + marker. Assert both.
	if info.Size() > 50<<20 {
		t.Fatalf("repo_42_stderr.log is %d bytes — the 50 MB write-time cap regressed (June 2026: 9.5 GB artifacts)", info.Size())
	}
	if max := int64(scancodeFailHeadBytes + scancodeFailTailBytes + 1024); info.Size() > max {
		t.Errorf("repo_42_stderr.log is %d bytes, expected <= %d (head+tail+marker) — the bounded capture leaked", info.Size(), max)
	}
	content, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(content), "aveloxis truncated") {
		t.Error("the truncated artifact must carry the elision marker reporting the true total byte count — without it operators can't tell a small failure from a truncated storm")
	}
}
