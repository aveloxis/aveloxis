// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"testing"
	"time"
)

// TestSweptWaiterPanicStillAnswers — O6 (v0.29.71): the subprocess waiter
// goroutine had no recovery; a panic there took down the process, and one
// that were merely recovered would leave Wait blocked forever. It answers
// with an error instead.
func TestSweptWaiterPanicStillAnswers(t *testing.T) {
	ch := make(chan error, 1)
	killed := false
	go waitAndSweep(func() error { panic("wait blew up") }, func() { killed = true }, ch)
	select {
	case err := <-ch:
		if err == nil {
			t.Error("a panicking wait must answer with an error")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait would block forever: the waiter did not answer")
	}
	// The kill happens before the answer is sent, so reading it here is
	// ordered by the channel (review round 1: it was recorded and dropped).
	if !killed {
		t.Error("a panicking wait must still kill what is left of the process group")
	}
	ch2 := make(chan error, 1)
	swept := false
	go waitAndSweep(func() error { return nil }, func() { swept = true }, ch2)
	if err := <-ch2; err != nil || !swept {
		t.Errorf("clean wait: err=%v swept=%v, want nil and the group killed", err, swept)
	}
}
