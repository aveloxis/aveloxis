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
// of the demand registry: every goroutine label in the non-test sources
// of every package that receives the store (safego.Go, goTracked,
// singleFlight, safego.Recover) must be in exactly one of the registry's
// lists — backgroundDBLoops, scancodeDBLoops, mailingListDBLoops,
// jiraDBLoops, perWorkerLoops or nonDBGoroutines — so a new loop cannot
// start acquiring from the pool without being counted, or being declared
// connection-free (v0.29.58).
func TestEveryGoroutineLabelIsClassifiedForPoolDemand(t *testing.T) {
	classified := map[string]bool{}
	for _, list := range [][]string{backgroundDBLoops, scancodeDBLoops, mailingListDBLoops, jiraDBLoops, perWorkerLoops, nonDBGoroutines} {
		for _, n := range list {
			if classified[n] {
				t.Errorf("label %q is classified twice", n)
			}
			classified[n] = true
		}
	}
	// Any logger receiver (s.logger, w.Logger), goTracked, singleFlight
	// (whose label is its second argument) and safego.Recover.
	labelRe := regexp.MustCompile(`(?:safego\.Go\([^,]+,|s\.goTracked\(|goTracked\(|singleFlight\([^,]+,|safego\.Recover\([^,]+,)\s*"([^"]+)"`)
	// Every package that receives the store (review round 5: the sweep
	// stopped at this package while the collector, its distribution
	// sub-package and db spawn pool consumers of their own).
	dirs := []string{".", filepath.Join("..", "collector"), filepath.Join("..", "collector", "distribution"), filepath.Join("..", "db")}
	examined := 0
	seen := map[string]bool{}
	for _, d := range dirs {
		files, err := filepath.Glob(filepath.Join(d, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
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
					t.Errorf("%s: goroutine label %q is not classified in any registry list (backgroundDBLoops, scancodeDBLoops, mailingListDBLoops, jiraDBLoops, perWorkerLoops, nonDBGoroutines) — PoolDemand would not count it (or say why it holds no connection)", f, label)
				}
			}
		}
	}
	if examined < 40 {
		t.Fatalf("examined only %d goroutine labels across %d packages — the regex no longer matches the sources", examined, len(dirs))
	}
	// Names without a goroutine label of their own: the run loop is Run
	// itself; the monitor allowance is the :5555 handlers.
	unlabeled := map[string]bool{"run-loop": true, "monitor-dashboard": true}
	for _, list := range [][]string{backgroundDBLoops, scancodeDBLoops, mailingListDBLoops, jiraDBLoops, perWorkerLoops, nonDBGoroutines} {
		for _, n := range list {
			if !seen[n] && !unlabeled[n] {
				t.Errorf("the registry lists %q but no goroutine in the swept sources carries that label", n)
			}
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
	cfg.Collection.BreadthFetchConcurrency = 5

	demand, attrs := PoolDemand(cfg, 120, 2)
	want := 120*perSlotConnections + (10 + 1) + (8*2 + len(mailingListDBLoops)) + 3*2 + 0 + 0 + 4 + 5 + len(backgroundDBLoops)
	if demand != want {
		t.Fatalf("PoolDemand = %d, want %d (slots %d×%d + dist 10+dispatcher + ml 8×2+%d loops + drain 3×2 + history 4 + breadth 5 + bg %d)", demand, want, 120, perSlotConnections, len(mailingListDBLoops), len(backgroundDBLoops))
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
	off.Collection.BreadthFetchConcurrency = 1
	d2, _ := PoolDemand(off, 1, 5)
	if d2 != perSlotConnections+(6+len(jiraDBLoops))+1+1+len(backgroundDBLoops) {
		t.Fatalf("disabled subsystems (distribution, mailing list, scancode) must contribute 0, and an enabled one carries its singletons: got %d", d2)
	}

	// scancode follows the spawn site's transform: 0 disables; a negative
	// is NOT 0 there (ScancodeWorkersOrDefault → 2 runners + dispatcher);
	// a positive count runs that many (review round 5).
	neg := &config.Config{}
	neg.Collection.ScancodeWorkers = -1
	dNeg, _ := PoolDemand(neg, 0, 0)
	pos := &config.Config{}
	pos.Collection.ScancodeWorkers = 2
	dPos, _ := PoolDemand(pos, 0, 0)
	if dNeg != dPos {
		t.Fatalf("scancode_workers -1 must count like the spawn site runs it (2 runners): got %d vs %d for 2", dNeg, dPos)
	}
	if dPos-d2 <= 0 && pos.Collection.ScancodeWorkers > 0 {
		t.Fatal("scancode runners and their dispatcher/monitor loops must add to the demand")
	}

	// review round 7: with the mailing list enabled but no system loaded,
	// the wiring spawns nothing (not even the two sender loops).
	noSys := &config.Config{}
	noSys.Collection.MailingListEnabled = true
	noSys.Collection.MailingListWorkers = 8
	dNoSys, _ := PoolDemand(noSys, 0, 0)
	base, _ := PoolDemand(&config.Config{}, 0, 0)
	if dNoSys != base {
		t.Fatalf("mailing list enabled with zero systems must add nothing: got %d vs %d", dNoSys, base)
	}
}
