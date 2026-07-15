// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"os"
	"strings"
	"testing"
)

// v0.27.6 — `aveloxis scancode-worker`: the dedicated-scancode-host
// subcommand (the `aveloxis api` single-purpose-process precedent).

func readScancodeWorkerCmdSource(t *testing.T) string {
	t.Helper()
	data, err := os.ReadFile("scancode_worker_cmd.go")
	if err != nil {
		t.Fatal("v0.27.6 introduces cmd/aveloxis/scancode_worker_cmd.go")
	}
	return string(data)
}

// Negative pins below strip // comments via the package-shared
// stripLineComments helper (migrate_only_serve_and_migrate_test.go) —
// the v0.21.5 / v0.27.4 lesson: comments legitimately NAME the
// forbidden pattern (this file's own comment cites store.Migrate).

func TestScancodeWorkerCommandRegistered(t *testing.T) {
	src, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "scancodeWorkerCmd(&cfgPath)") {
		t.Error("main.go must wire scancodeWorkerCmd(&cfgPath) into root.AddCommand — without it `aveloxis scancode-worker` is not callable")
	}
}

func TestScancodeWorkerCmdShape(t *testing.T) {
	src := readScancodeWorkerCmdSource(t)
	if !strings.Contains(src, `Use:   "scancode-worker"`) && !strings.Contains(src, `"scancode-worker"`) {
		t.Error("the command's Use must be `scancode-worker` (kebab-case, matching the docs + start/stop conventions)")
	}
	if !strings.Contains(src, "func scancodeWorkerCmd") {
		t.Error("must define scancodeWorkerCmd() so main.go has a stable wiring target")
	}
	if !strings.Contains(src, `pidfile.Path("scancode-worker")`) {
		t.Error("the command must write its own pidfile (~/.aveloxis/aveloxis-scancode-worker.pid) so operators can manage it like serve/web/api")
	}
	if !strings.Contains(src, `ConnectionStringWithAppName("aveloxis-scancode-worker")`) {
		t.Error("the command must tag its pgx backends with a distinct application_name — the v0.20.0 pg_stat_activity diagnostics depend on it")
	}
}

func TestScancodeWorkerCmdDoesNotMigrate(t *testing.T) {
	// v0.21.5 contract: only `serve` and `migrate` run store.Migrate.
	// The dedicated host trusts the primary server's schema; two
	// processes racing startup DDL is the exact conflict class
	// v0.21.5 removed.
	code := stripLineComments(readScancodeWorkerCmdSource(t))
	if strings.Contains(code, "store.Migrate(") {
		t.Error("scancode-worker must NOT call store.Migrate — the v0.21.5 contract restricts migrations to serve + migrate")
	}
	if !strings.Contains(code, "CheckSchemaVersion(") {
		t.Error("scancode-worker must call store.CheckSchemaVersion so schema drift surfaces loudly at startup (the v0.20.15 pattern)")
	}
}

func TestScancodeWorkerCmdUsesSharedOptionsMapping(t *testing.T) {
	src := readScancodeWorkerCmdSource(t)
	if !strings.Contains(src, "collector.ScancodeOptionsFromConfig(") {
		t.Error("scancode-worker must build the worker via collector.ScancodeOptionsFromConfig — a hand-rolled options literal here can drift from the scheduler's spawn site")
	}
	if !strings.Contains(src, "collector.NewScancodeWorker(") {
		t.Error("scancode-worker must construct collector.NewScancodeWorker and Run it")
	}
}

func TestScancodeWorkerCmdNeedsNoAPIKeys(t *testing.T) {
	// scancode clones anonymously — unlike `aveloxis serve`, this
	// command must start with ZERO worker_oauth rows. Pin the absence
	// of any key-pool plumbing.
	code := stripLineComments(readScancodeWorkerCmdSource(t))
	for _, forbidden := range []string{"KeyPool", "worker_oauth", "GetAPIKeys", "LoadKeys"} {
		if strings.Contains(code, forbidden) {
			t.Errorf("scancode-worker must not touch API-key plumbing (%q) — scancode clones anonymously and the dedicated host deliberately holds no tokens", forbidden)
		}
	}
}
