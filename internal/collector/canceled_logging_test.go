// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"os"
	"strings"
	"testing"
)

// TestProcessorDemotesCancellationFromError (v0.27.28) — the staged
// processor's entity-type failure path must distinguish "the shutdown
// asked us to stop" (context.Canceled → Info; staging rows stay
// unprocessed and drain on restart) from a real processing failure
// (→ Error). These were the only two ERROR-level lines in the
// 2026-07-21 shutdown's ~600-line noise burst.
func TestProcessorDemotesCancellationFromError(t *testing.T) {
	src, err := os.ReadFile("staged.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	i := strings.Index(s, `"failed to process entity type"`)
	if i < 0 {
		t.Fatal("entity-type error log site not found")
	}
	// The cancellation branch must sit in the same region (within the
	// surrounding ~600 bytes) as the Error call it guards.
	region := s[maxInt(0, i-600):i]
	if !strings.Contains(region, "errors.Is(err, context.Canceled)") {
		t.Error("the entity-type failure path lost its context.Canceled demotion — shutdown aborts must log Info, not Error (v0.27.28)")
	}
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
