// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/config"
)

// TestScancodeStartGapScalesWithWorkers — worklist item 65, option (c)
// (operator, 2026-09-28): one global start gap after every start capped
// starts at 3600/gap per hour whatever the worker count (40/hour at the
// 90 s default, so a first pass of 40K repositories took ~42 days, not the
// documented ~12). The gap is now scancode_start_interval_s divided by the
// workers: throughput scales with workers, and starts stay evenly spaced
// after a restart (no burst). End to end (SR-10): JSON → the shared options
// builder → the worker.
func TestScancodeStartGapScalesWithWorkers(t *testing.T) {
	for _, tc := range []struct {
		json string
		want time.Duration
	}{
		{`{"collection":{"scancode_workers":3,"scancode_start_interval_s":90}}`, 30 * time.Second},
		{`{"collection":{"scancode_workers":7,"scancode_start_interval_s":91}}`, 13 * time.Second},
		{`{"collection":{"scancode_workers":1,"scancode_start_interval_s":90}}`, 90 * time.Second},
		{`{}`, 45 * time.Second}, // the defaults: 2 workers, 90 s
	} {
		p := filepath.Join(t.TempDir(), "aveloxis.json")
		if err := os.WriteFile(p, []byte(tc.json), 0o600); err != nil {
			t.Fatal(err)
		}
		cfg, err := config.Load(p)
		if err != nil {
			t.Fatal(err)
		}
		w := NewScancodeWorker(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), ScancodeOptionsFromConfig(&cfg.Collection))
		if w.startGap != tc.want {
			t.Errorf("%s: start gap %v; want %v", tc.json, w.startGap, tc.want)
		}
	}
}
