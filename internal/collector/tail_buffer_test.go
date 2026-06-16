// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
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

// TestHeadTailBufferBoundedRAM is the regression guard for the 2026-06-11
// 15 GB-in-RAM bug: no matter how much is written, the buffer keeps at most
// headCap+tailCap bytes (plus a small marker), while Total() still reports the
// true byte count.
func TestHeadTailBufferBoundedRAM(t *testing.T) {
	b := &headTailBuffer{headCap: 16, tailCap: 8}
	// Write 1 MB in 64 KB chunks.
	chunk := bytes.Repeat([]byte("x"), 64<<10)
	var written int64
	for i := 0; i < 16; i++ {
		n, _ := b.Write(chunk)
		written += int64(n)
	}
	if b.Total() != written {
		t.Errorf("Total()=%d, want %d (true byte count)", b.Total(), written)
	}
	if len(b.head) != 16 {
		t.Errorf("head must be capped at 16, got %d", len(b.head))
	}
	if len(b.tail) != 8 {
		t.Errorf("tail must be capped at 8, got %d", len(b.tail))
	}
	out := b.Bytes()
	// head(16) + marker + tail(8): the marker is small; the rendered output must
	// be a tiny fraction of the 1 MB written.
	if len(out) > 1024 {
		t.Errorf("rendered output must be bounded (head+marker+tail), got %d bytes for a 1MB stream", len(out))
	}
	if !strings.Contains(string(out), "truncated") {
		t.Errorf("rendered output must carry the elision marker; got %q", out)
	}
}

func TestHeadTailBufferExactWhenSmall(t *testing.T) {
	// Stream smaller than headCap is reproduced verbatim, no marker.
	b := &headTailBuffer{headCap: 100, tailCap: 20}
	b.Write([]byte("hello world"))
	if got := string(b.Bytes()); got != "hello world" {
		t.Errorf("small stream must be verbatim; got %q", got)
	}
	if b.Total() != 11 {
		t.Errorf("Total()=%d want 11", b.Total())
	}
}

func TestHeadTailBufferOverlapNoDuplication(t *testing.T) {
	// Stream between headCap and headCap+tailCap: rendered exactly with no
	// duplicated bytes and no elision marker.
	b := &headTailBuffer{headCap: 5, tailCap: 5}
	b.Write([]byte("ABCDEFGH")) // 8 bytes; headCap+tailCap=10 >= 8
	got := string(b.Bytes())
	if got != "ABCDEFGH" {
		t.Errorf("overlap case must reproduce the stream exactly; got %q want %q", got, "ABCDEFGH")
	}
	if strings.Contains(got, "truncated") {
		t.Errorf("no elision marker expected when stream fits in head+tail; got %q", got)
	}
}

func TestHeadTailBufferElidesMiddle(t *testing.T) {
	// Stream larger than headCap+tailCap: head + marker + tail, middle dropped.
	b := &headTailBuffer{headCap: 5, tailCap: 5}
	b.Write([]byte("AAAAA"))
	b.Write([]byte("MMMMMMMMMM")) // middle — must be elided
	b.Write([]byte("ZZZZZ"))
	got := string(b.Bytes())
	if !strings.HasPrefix(got, "AAAAA") {
		t.Errorf("must start with the head; got %q", got)
	}
	if !strings.HasSuffix(got, "ZZZZZ") {
		t.Errorf("must end with the tail; got %q", got)
	}
	if strings.Contains(got, "MMMMMMMMMM") {
		t.Errorf("middle must be elided; got %q", got)
	}
	if !strings.Contains(got, "truncated 10 bytes of 20 total") {
		t.Errorf("marker must report elided + total bytes; got %q", got)
	}
}
