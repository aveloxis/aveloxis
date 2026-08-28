// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Adoption tripwire (v0.25.36): the background/worker goroutine sites
// enumerated in the 2026-07-08 audit must stay wrapped. A refactor that
// reverts a `safego.Go(...)` back to a bare `go ...` (or drops a
// `defer safego.Recover(...)` from a closure) reintroduces the
// "one panicking cycle kills the whole fleet" failure mode.

package safego

import (
	"os"
	"strings"
	"testing"
)

func TestSafegoAdoptionAtAuditedSites(t *testing.T) {
	// file -> minimum number of safego call sites expected.
	expectations := map[string]int{
		// v0.27.40: four ticker arms route through singleFlight, which
		// wraps safego.Go internally — recovery preserved, direct
		// call-site count lower (was 15).
		"../scheduler/scheduler.go":           12,
		"../scheduler/mailinglist_wiring.go":  4,
		"../scheduler/distribution_wiring.go": 1,
		"../scheduler/long_jobs_watchdog.go":  1,
		"../collector/staged.go":              4, // 3 parallel collectors + PR shard
		"../collector/scancode_worker.go":     5,
		"../collector/distribution/worker.go": 2,
		"../web/server.go":                    1, // org-repo-scan
		"../web/admin.go":                     1, // group-approved-email
	}
	if sched, err := os.ReadFile("../scheduler/scheduler.go"); err == nil {
		body := string(sched)
		i := strings.Index(body, "func (s *Scheduler) goTracked(")
		if i < 0 || !strings.Contains(body[i:min(i+400, len(body))], "safego.Go(") {
			t.Errorf("scheduler.goTracked must delegate to safego.Go — every s.goTracked launch is counted here as a recovered site")
		}
	}
	for path, min := range expectations {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}
		// s.goTracked (pass 38) delegates to safego.Go — the scheduler's
		// tracked-pool launcher counts as a recovered site; its own
		// delegation is pinned below so the count cannot rot.
		got := strings.Count(string(src), "safego.Go(") + strings.Count(string(src), "safego.Recover(") + strings.Count(string(src), "s.goTracked(")
		if got < min {
			t.Errorf("%s has %d safego call sites, expected >= %d — a goroutine "+
				"launch lost its panic recovery. Wrap degrade-not-die goroutines "+
				"with safego.Go / defer safego.Recover (see the safego package "+
				"doc for the policy).", path, got, min)
		}
	}
}
