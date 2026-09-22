// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestEveryGoroutineLabelIsClassifiedForPoolDemand guards the denominator
// of the demand registry: every goroutine label in this package's
// non-test sources (safego.Go, goTracked, safego.Recover) must be listed
// in backgroundDBLoops or perWorkerLoops, so a new loop cannot start
// acquiring from the pool without being counted (v0.29.58).
func TestEveryGoroutineLabelIsClassifiedForPoolDemand(t *testing.T) {
	classified := map[string]bool{}
	for _, n := range backgroundDBLoops {
		classified[n] = true
	}
	for _, n := range perWorkerLoops {
		classified[n] = true
	}
	// Any logger receiver (s.logger, w.Logger), goTracked, singleFlight
	// (whose label is its second argument) and safego.Recover.
	labelRe := regexp.MustCompile(`(?:safego\.Go\([^,]+,|s\.goTracked\(|goTracked\(|singleFlight\([^,]+,|safego\.Recover\([^,]+,)\s*"([^"]+)"`)
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	examined := 0
	seen := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range labelRe.FindAllStringSubmatch(srctest.StripGoComments(string(src)), -1) {
			examined++
			label := m[1]
			seen[label] = true
			if !classified[label] {
				t.Errorf("%s: goroutine label %q is not classified in backgroundDBLoops or perWorkerLoops — PoolDemand would not count it", f, label)
			}
		}
	}
	if examined < 10 {
		t.Fatalf("examined only %d goroutine labels — the regex no longer matches the sources", examined)
	}
	for _, n := range backgroundDBLoops {
		if !seen[n] && n != "run-loop" {
			t.Errorf("backgroundDBLoops lists %q but no goroutine in the sources carries that label", n)
		}
	}
}

// TestStagedFanOutMatchesTheCollector pins perSlotConnections' input: the
// staged collector's concurrent phase goroutines are exactly stagedFanOut.
func TestStagedFanOutMatchesTheCollector(t *testing.T) {
	src, err := os.ReadFile(filepath.Join("..", "collector", "staged.go"))
	if err != nil {
		t.Fatal(err)
	}
	n := strings.Count(srctest.StripGoComments(string(src)), `safego.Recover(sc.logger, "collect-`)
	if n != stagedFanOut {
		t.Fatalf("staged.go runs %d concurrent collect-* goroutines, stagedFanOut says %d — update the constant with the fan-out", n, stagedFanOut)
	}
}

// TestPoolDemandIsTheSumOfEveryConsumer pins the arithmetic and that a
// disabled subsystem contributes nothing.
func TestPoolDemandIsTheSumOfEveryConsumer(t *testing.T) {
	cfg := &config.Config{}
	cfg.Collection.DistributionTrackingEnabled = true
	cfg.Collection.DistributionTrackingWorkers = 10
	cfg.Collection.MailingListEnabled = true
	cfg.Collection.MailingListWorkers = 8
	cfg.Collection.MailingListProcessorWorkers = 3
	cfg.Collection.JiraEnabled = false
	cfg.Collection.JiraWorkers = 6
	cfg.Collection.ScancodeWorkers = 0
	cfg.Collection.ActivityHistoryConcurrency = 4

	demand, attrs := PoolDemand(cfg, 120, 2)
	want := 120*perSlotConnections + 10 + 8*2 + 3*2 + 0 + 0 + 4 + len(backgroundDBLoops)
	if demand != want {
		t.Fatalf("PoolDemand = %d, want %d (slots %d×%d + dist 10 + ml 8×2 + drain 3×2 + history 4 + bg %d)", demand, want, 120, perSlotConnections, len(backgroundDBLoops))
	}
	if perSlotConnections != 5 {
		t.Fatalf("perSlotConnections = %d, want 5: three staged phases + heartbeat + long-jobs watchdog", perSlotConnections)
	}
	if demand <= 120+15 {
		t.Fatalf("the kate configuration's demand (%d) must exceed the old workers+15 literal (135) — that gap is the incident", demand)
	}
	if len(attrs) == 0 || attrs[0] != "pool_demand" || attrs[1] != demand {
		t.Fatalf("attributes must lead with the effective demand, got %v", attrs[:2])
	}

	off := &config.Config{}
	off.Collection.DistributionTrackingWorkers = 10
	off.Collection.MailingListWorkers = 8
	off.Collection.JiraEnabled = true
	off.Collection.JiraWorkers = 6
	off.Collection.ActivityHistoryConcurrency = 1
	d2, _ := PoolDemand(off, 1, 5)
	if d2 != perSlotConnections+6+1+len(backgroundDBLoops) {
		t.Fatalf("disabled subsystems must contribute 0: got %d", d2)
	}
}
