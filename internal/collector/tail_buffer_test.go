// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"
)

func TestTailBufferRetainsOnlyTail(t *testing.T) {
	tb := &tailBuffer{cap: 10}
	n, err := tb.Write([]byte("0123456789ABCDEF"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 16 {
		t.Errorf("Write must report bytes-written = full input length (Go io.Writer contract), got %d, want 16", n)
	}
	got := tb.String()
	if got != "6789ABCDEF" {
		t.Errorf("tailBuffer with cap=10 must retain only last 10 bytes after writing 16; got %q want %q", got, "6789ABCDEF")
	}
}

func TestTailBufferAccumulatesAcrossWrites(t *testing.T) {
	tb := &tailBuffer{cap: 8}
	tb.Write([]byte("hello "))
	tb.Write([]byte("world!"))
	got := tb.String()
	// Total 12 bytes written; cap=8 → keep last 8: "o world!"
	want := "o world!"
	if got != want {
		t.Errorf("got %q want %q", got, want)
	}
}

func TestTailBufferZeroCapDiscardsEverything(t *testing.T) {
	tb := &tailBuffer{cap: 0}
	tb.Write([]byte("anything"))
	if tb.String() != "" {
		t.Errorf("cap=0 must discard all input; got %q", tb.String())
	}
}

func TestTailBufferShorterInputThanCap(t *testing.T) {
	tb := &tailBuffer{cap: 100}
	tb.Write([]byte("short"))
	if !strings.HasPrefix(tb.String(), "short") {
		t.Errorf("input shorter than cap should be preserved verbatim; got %q", tb.String())
	}
}
