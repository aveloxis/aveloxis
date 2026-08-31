// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_wiring_test.go — C3 scheduler wiring: the Jira worker pool +
// drain loop spawn only when jira_enabled, via goTracked (the shutdown
// arm must wait for their post-cancel bookkeeping), and the staging
// cleanup covers jira_staging on the shared retention knob.
package scheduler

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestJiraWorkersSpawnGatedOnEnabled(t *testing.T) {
	sched := srctest.Read(t, "internal/scheduler/scheduler.go")
	if !strings.Contains(sched, "s.cfg.Collection.JiraEnabled") || !strings.Contains(sched, "s.spawnJiraWorkers(ctx)") {
		t.Error("Run must spawn the Jira subsystem gated on collection.jira_enabled")
	}
	wiring := srctest.Read(t, "internal/scheduler/jira_wiring.go")
	for _, needle := range []string{
		"goTracked(",             // shutdown waits for bookkeeping
		"JiraWorkersOrDefault()", // knob at point of use (SR-10)
		"JiraCadenceDuration()",  //
		"NewJiraWorker(",         //
		"NewJiraProcessor(",      //
	} {
		if !strings.Contains(wiring, needle) {
			t.Errorf("jira_wiring.go must contain %q", needle)
		}
	}
}

func TestStagingCleanupCoversJira(t *testing.T) {
	body := srctest.FuncBody(t, srctest.Read(t, "internal/scheduler/scheduler.go"),
		"func (s *Scheduler) runStagingCleanup(")
	if !strings.Contains(body, "PurgeJiraStagingProcessed(") {
		t.Error("runStagingCleanup must purge jira_staging tombstones on the shared staging_retention_hours knob")
	}
}
