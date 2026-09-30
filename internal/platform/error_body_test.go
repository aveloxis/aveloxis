// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"strings"
	"testing"
)

// endless yields bytes forever and counts them.
type endless struct{ n int64 }

func (e *endless) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	e.n += int64(len(p))
	return len(p), nil
}

// TestReadErrorBodyIsBounded — O5 (v0.29.71): an error response's body was
// read whole (io.ReadAll), so a broken or hostile upstream answering an
// error with gigabytes exhausted the worker's memory. The readers use at
// most the first 200 bytes and a small JSON message, so the read stops at
// ErrorBodyLimit.
func TestReadErrorBodyIsBounded(t *testing.T) {
	src := &endless{}
	body, err := ReadErrorBody(src)
	if err != nil {
		t.Fatalf("reaching the limit is not an error: %v", err)
	}
	if int64(len(body)) != ErrorBodyLimit {
		t.Errorf("read %d bytes, want exactly ErrorBodyLimit (%d)", len(body), ErrorBodyLimit)
	}
	if src.n > ErrorBodyLimit+64*1024 {
		t.Errorf("pulled %d bytes from the body, want about ErrorBodyLimit", src.n)
	}
	if got, _ := ReadErrorBody(strings.NewReader(`{"message":"Not Found"}`)); string(got) != `{"message":"Not Found"}` {
		t.Errorf("a small body must come back whole, got %q", got)
	}
	if b, err := ReadErrorBody(nil); b != nil || err != nil {
		t.Error("a nil body reads as nil")
	}
}
