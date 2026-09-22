// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// whitespace_unmatched_test.go — v0.29.56. The walk refuses to stamp its
// marker when any emitted stat matched no stored commit row, and the
// message used to carry only counts ("1 of 266"), which cannot distinguish
// a missing commit row from a filename the numstat and patch walks spell
// differently. It now names the rows — bounded, because the store's sample
// is per BATCH and the walk flushes every 5,000 stats: a large repo whose
// stats all miss would otherwise put thousands of keys into one error
// string.

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestUnmatchedWhitespaceSampleIsBoundedAcrossFlushes(t *testing.T) {
	// Every flush offers a full batch-sized sample, as a repo whose stats
	// all miss would.
	batch := make([]string, db.WhitespaceUnmatchedSample)
	for i := range batch {
		batch[i] = "hash" + strings.Repeat("0", 36) + " file"
	}
	var unmatched []string
	for flush := 0; flush < 50; flush++ {
		unmatched = appendUnmatchedSample(unmatched, batch)
	}
	if len(unmatched) != db.WhitespaceUnmatchedSample {
		t.Errorf("collected %d keys over 50 flushes, want at most %d", len(unmatched), db.WhitespaceUnmatchedSample)
	}
	// A partial batch still fits, and the cap is not exceeded by one that
	// straddles it.
	got := appendUnmatchedSample([]string{"a", "b"}, []string{"c", "d", "e", "f"})
	if len(got) != db.WhitespaceUnmatchedSample {
		t.Errorf("straddling batch produced %d keys, want %d", len(got), db.WhitespaceUnmatchedSample)
	}
	if got := appendUnmatchedSample(nil, nil); got != nil {
		t.Errorf("empty = %v", got)
	}
}

// TestWhitespaceWalkUsesTheBoundedAppend is the wiring half: the walk must
// accumulate through the capped helper, not append directly.
func TestWhitespaceWalkUsesTheBoundedAppend(t *testing.T) {
	// Comment-stripped for BOTH needles: prose quoting the call must not
	// satisfy the positive one (SR-12 / the pre-launch checklist).
	body := srctest.StripGoComments(srctest.Read(t, "internal/collector/whitespace.go"))
	if !strings.Contains(body, "appendUnmatchedSample(unmatched, um)") {
		t.Error("the walk's flush must accumulate unmatched keys through appendUnmatchedSample")
	}
	if strings.Contains(body, "append(unmatched, um...)") {
		t.Error("an unbounded append of a batch's unmatched keys is back")
	}
}

func TestFormatUnmatchedWhitespace(t *testing.T) {
	if got := formatUnmatchedWhitespace(nil); got != "none reported" {
		t.Errorf("empty = %q", got)
	}
	got := formatUnmatchedWhitespace([]string{"abc README.md", "def main.go"})
	for _, want := range []string{"abc README.md", "def main.go", "(first 2)"} {
		if !strings.Contains(got, want) {
			t.Errorf("%q missing %q", got, want)
		}
	}
	// The count says how many are SHOWN, so a reader knows the list is a
	// sample rather than the whole shortfall (which the message's "N of M"
	// already gives).
	if strings.Count(got, "(first") != 1 {
		t.Errorf("%q must state the sample size once", got)
	}
}

// Copilot on PR #210: the sampled keys are repository-controlled filenames
// and go straight into the refusal the rewalk CLI prints, so a filename
// carrying CR/LF or an escape sequence could forge lines in that output.
// They are scrubbed like every other logged value (scrubLogValue).
func TestFormatUnmatchedWhitespaceScrubsFilenames(t *testing.T) {
	got := formatUnmatchedWhitespace([]string{
		"ok.go",
		"evil.go\nWARN: fake line",
		"carriage\rreturn.go",
		"esc\x1b[31mape.go",
	})
	for _, bad := range []string{"\n", "\r", "\x1b"} {
		if strings.Contains(got, bad) {
			t.Errorf("the sample kept %q from a repository-controlled filename: %q", bad, got)
		}
	}
	if !strings.Contains(got, "ok.go") {
		t.Errorf("the sample must still name the files: %q", got)
	}
}
