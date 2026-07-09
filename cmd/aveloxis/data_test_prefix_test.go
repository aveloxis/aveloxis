// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

// v0.26.2: every line a data-test subprocess emits must carry a
// [released] / [new] side tag. During the 2026-07-09 run, hours of
// WARN-level FK violations streamed with no way to tell WHICH binary
// produced them — the operator had to interrupt the run and we had to
// reconstruct the side from scratch-DB tool_version stamps.

import (
	"bytes"
	"os"
	"strings"
	"testing"
)

func TestPrefixWriterTagsEveryLine(t *testing.T) {
	var buf bytes.Buffer
	w := &prefixWriter{w: &buf, prefix: "[released] "}

	// Multi-line write.
	if _, err := w.Write([]byte("alpha\nbeta\n")); err != nil {
		t.Fatal(err)
	}
	// Split mid-line across two writes: the continuation must NOT get a
	// second prefix.
	if _, err := w.Write([]byte("gam")); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("ma\n")); err != nil {
		t.Fatal(err)
	}

	got := buf.String()
	want := "[released] alpha\n[released] beta\n[released] gamma\n"
	if got != want {
		t.Errorf("prefixWriter output mismatch:\n got: %q\nwant: %q", got, want)
	}
}

func TestDataTestRunnersTagSubprocessOutput(t *testing.T) {
	src, err := os.ReadFile("data_test_cmd.go")
	if err != nil {
		t.Fatal(err)
	}
	code := string(src)

	if !strings.Contains(code, "type prefixWriter struct") {
		t.Fatal("data_test_cmd.go must define prefixWriter — the side-tagging " +
			"writer that makes subprocess log lines attributable to a binary")
	}
	// The three subprocess runners take a side label and wrap
	// stdout/stderr — a bare `cmd.Stdout = os.Stdout` in any of them
	// regresses to untagged output.
	for _, fn := range []string{"func runMigrate(", "func dtRunAddRepo(", "func dtRunCollect("} {
		idx := strings.Index(code, fn)
		if idx < 0 {
			t.Fatalf("cannot find %s", fn)
		}
		body := code[idx:]
		if end := strings.Index(body[1:], "\nfunc "); end > 0 {
			body = body[:end+1]
		}
		if !strings.Contains(body, "sideTaggedOutputs(") {
			t.Errorf("%s must route subprocess output through sideTaggedOutputs so every "+
				"line carries its [released]/[new] side tag (2026-07-09 incident: "+
				"hours of FK WARNs with no attributable side)", fn)
		}
		if strings.Contains(body, "cmd.Stdout = os.Stdout") {
			t.Errorf("%s still assigns bare os.Stdout — untagged subprocess output", fn)
		}
	}
}
