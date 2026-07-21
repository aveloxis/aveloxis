// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
)

// TestFillWorkerSlotsPassesLargeRepoExclusions pins the v0.27.35
// wiring: the fill cycle computes the exclusion set once and threads
// it into every DequeueNext claim.
func TestFillWorkerSlotsPassesLargeRepoExclusions(t *testing.T) {
	src, err := os.ReadFile("scheduler.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(src)
	start := strings.Index(s, "func (s *Scheduler) fillWorkerSlots(")
	if start < 0 {
		t.Fatal("fillWorkerSlots not found")
	}
	body := s[start:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "s.largeRepoExclusions(ctx)") {
		t.Error("fillWorkerSlots must compute the large-repo exclusion set (once per cycle)")
	}
	if !strings.Contains(body, "s.store.DequeueNext(ctx, s.workerID, excludeLargest)") {
		t.Error("every claim must pass the exclusion set to DequeueNext")
	}
}

// TestLargeRepoExclusionsZeroCostWhenDisabled is the config→behavior
// check for the OFF path: with the knob unset the helper returns
// before touching the store at all (nil store would panic otherwise),
// so the default deployment pays nothing.
func TestLargeRepoExclusionsZeroCostWhenDisabled(t *testing.T) {
	s := &Scheduler{cfg: Config{Collection: &config.CollectionConfig{}}}
	if got := s.largeRepoExclusions(context.Background()); got != nil {
		t.Errorf("knob off must return nil without any store access, got %v", got)
	}
}
