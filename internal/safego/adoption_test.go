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
		"../scheduler/scheduler.go":           15, // 12 named tasks + 3 closures (job, heartbeat, matview)
		"../scheduler/mailinglist_wiring.go":  4,
		"../scheduler/distribution_wiring.go": 1,
		"../scheduler/long_jobs_watchdog.go":  1,
		"../collector/staged.go":              4, // 3 parallel collectors + PR shard
		"../collector/scancode_worker.go":     5,
		"../collector/distribution/worker.go": 2,
		"../web/server.go":                    1, // org-repo-scan
		"../web/admin.go":                     1, // group-approved-email
	}
	for path, min := range expectations {
		src, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("read %s: %v", path, err)
			continue
		}
		got := strings.Count(string(src), "safego.Go(") + strings.Count(string(src), "safego.Recover(")
		if got < min {
			t.Errorf("%s has %d safego call sites, expected >= %d — a goroutine "+
				"launch lost its panic recovery. Wrap degrade-not-die goroutines "+
				"with safego.Go / defer safego.Recover (see the safego package "+
				"doc for the policy).", path, got, min)
		}
	}
}
