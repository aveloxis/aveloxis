// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
	"fmt"
)

// tailBuffer is a bounded io.Writer that retains only the last `cap`
// bytes written. Used to capture the tail of scancode subprocess
// output for failure diagnostics without unbounded memory growth on
// chatty failures.
//
// Not safe for concurrent writes — the caller (exec.Cmd's IO
// goroutines) writes serially per stream.
type tailBuffer struct {
	cap int
	buf bytes.Buffer
}

// Write implements io.Writer. Appends to the underlying buffer, then
// trims from the front if the buffer exceeds cap. Trimming is done
// in-place by copying the tail into a fresh buffer — bytes.Buffer
// doesn't expose a "discard front N bytes" method.
func (t *tailBuffer) Write(p []byte) (int, error) {
	if t.cap <= 0 {
		return len(p), nil
	}
	n, _ := t.buf.Write(p)
	if t.buf.Len() > t.cap {
		// Keep only the last cap bytes.
		tail := t.buf.Bytes()
		t.buf.Reset()
		t.buf.Write(tail[len(tail)-t.cap:])
	}
	return n, nil
}

// String returns the captured tail.
func (t *tailBuffer) String() string {
	return t.buf.String()
}

// headTailBuffer is a bounded io.Writer that retains the FIRST headCap bytes and
// the LAST tailCap bytes of everything written, discarding the middle. It never
// blocks the writer and never grows past ~headCap+tailCap bytes — regardless of
// how many gigabytes the source emits.
//
// Motivation (2026-06-11): a corrupt host libmagic makes scancode spam the same
// ~4,000 magic-DB parse warnings on every file/process, so a large repo
// (aws/aws-sdk-cpp, Azure/azure-rest-api-specs, aws/lumberyard) produced 15+ GB
// of stderr. The pre-fix per-repo failure capture used an unbounded
// bytes.Buffer, which buffered ALL of it in RAM before writing an equally huge
// file to disk — a multi-GB heap spike per failing repo plus a filled scancode
// clone volume. The head shows the failure's onset; the tail shows the final
// error / exit context; together they retain the diagnostic value at a fixed,
// tiny cost.
//
// Not safe for concurrent writes — exec.Cmd's IO goroutines write serially per
// stream.
type headTailBuffer struct {
	head    []byte
	headCap int
	tail    []byte
	tailCap int
	total   int64
}

// Write implements io.Writer. Fills head until full, maintains tail as a ring of
// the last tailCap bytes, and always reports the full length consumed so the
// pipe never blocks.
func (b *headTailBuffer) Write(p []byte) (int, error) {
	b.total += int64(len(p))
	if room := b.headCap - len(b.head); room > 0 {
		take := room
		if take > len(p) {
			take = len(p)
		}
		b.head = append(b.head, p[:take]...)
	}
	if b.tailCap > 0 {
		if len(p) >= b.tailCap {
			b.tail = append(b.tail[:0], p[len(p)-b.tailCap:]...)
		} else {
			b.tail = append(b.tail, p...)
			if len(b.tail) > b.tailCap {
				b.tail = b.tail[len(b.tail)-b.tailCap:]
			}
		}
	}
	return len(p), nil
}

// Total returns the true number of bytes written (not the retained count).
func (b *headTailBuffer) Total() int64 { return b.total }

// Bytes renders head + an elision marker + tail. When the whole stream fit
// within headCap+tailCap it is reproduced exactly (no marker, no duplication).
func (b *headTailBuffer) Bytes() []byte {
	if b.total <= int64(len(b.head)) {
		// Everything fit in head.
		return b.head
	}
	if b.total <= int64(b.headCap+b.tailCap) {
		// head + tail cover the whole stream with overlap; append only the
		// suffix of the stream that head doesn't already contain.
		suffix := int(b.total) - len(b.head) // bytes after head; <= len(tail)
		out := make([]byte, 0, len(b.head)+suffix)
		out = append(out, b.head...)
		out = append(out, b.tail[len(b.tail)-suffix:]...)
		return out
	}
	elided := b.total - int64(len(b.head)) - int64(len(b.tail))
	marker := fmt.Sprintf("\n\n... [aveloxis truncated %d bytes of %d total — head+tail captured] ...\n\n", elided, b.total)
	out := make([]byte, 0, len(b.head)+len(marker)+len(b.tail))
	out = append(out, b.head...)
	out = append(out, marker...)
	out = append(out, b.tail...)
	return out
}
