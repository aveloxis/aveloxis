// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"bytes"
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
