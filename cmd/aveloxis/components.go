// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// allComponents is what `aveloxis start all` / `stop all` expand to:
// the three processes of a primary deployment.
var allComponents = []string{"serve", "web", "api"}

// scancodeWorkerComponent is the dedicated scancode host's process
// (`aveloxis scancode-worker`, v0.27.6). Manageable by name through
// start/stop since v0.29.4 — the 2026-09-09 incident: with only
// serve|web|api on offer, a "scancode-only" host was started with
// `aveloxis start serve` against production. Never part of `all`: a
// primary that still runs its in-serve pool would double up.
const scancodeWorkerComponent = "scancode-worker"

// resolveComponents maps an operator's start/stop target to the
// component list it names. The ONE place the component set lives.
func resolveComponents(target string) ([]string, error) {
	t := strings.ToLower(strings.TrimSpace(target))
	switch {
	case t == "all":
		return slices.Clone(allComponents), nil
	case slices.Contains(allComponents, t) || t == scancodeWorkerComponent:
		return []string{t}, nil
	}
	return nil, fmt.Errorf("unknown component %q (use serve, web, api, scancode-worker, or all)", target)
}

// componentAppName is the application_name a component tags its pool
// with and `aveloxis stop` verifies against — one derivation for both
// sides (SR-17); serve's spelling is also db.ServeApplicationName,
// which the migrate-time other-serve probe reads.
func componentAppName(component string) string {
	return "aveloxis-" + component
}

// pollBackends is the poll behind `aveloxis stop`: probe once a second
// until THIS host's backends are gone or the budget elapses, and hand
// back the last sighting for the verdict. Other hosts' backends never
// keep a local stop waiting (the 2026-09-09 incident waited the full
// budget on the primary's pool), and neither do backends whose address
// this role cannot see — a stop cannot verify what it cannot place. A failed probe is neither "all clear"
// nor the orphan verdict: the pids from the last successful check may
// be a serve mid-graceful-shutdown, and a terminate recipe here would
// invite the operator to kill live backends mid-bookkeeping (pass 41)
// — so it aborts with the hedge and returns render=false. The clock
// and the sleep are parameters so the exit rule is provable without a
// database.
func pollBackends(probe func() (db.AppNameBackends, error), budget time.Duration, elapsed func() time.Duration, sleep func(), out io.Writer, appName string) (last db.AppNameBackends, render bool) {
	for elapsed() < budget {
		found, err := probe()
		if err != nil {
			fmt.Fprintf(out, "(backend verification aborted %s into the %s budget: %v)\n",
				elapsed().Truncate(time.Second), budget.Truncate(time.Second), err)
			if len(last.ThisHost) > 0 {
				fmt.Fprintf(out, "%s backends from this host still connected as of the last successful check: %v — re-check pg_stat_activity before terminating anything.\n",
					appName, last.ThisHost)
			}
			return last, false
		}
		last = found
		if len(last.ThisHost) == 0 {
			return last, true
		}
		sleep()
	}
	return last, true
}

// printBackendVerdict renders what `aveloxis stop` found after its
// poll. Terminate recipes go out ONLY for this host's backends; backends
// carrying the same tag from OTHER client addresses — normally another
// machine's aveloxis (the primary seen from a dedicated scancode host,
// or the reverse), though the predicate only knows the address — are
// counted, named as such, never offered for termination, and so are
// backends of another database ROLE, whose address this role cannot
// see at all (round-7 finding 1). The 2026-09-09 incident printed 64
// recipes for the primary's pool.
func printBackendVerdict(out io.Writer, appName string, budget time.Duration, last db.AppNameBackends) {
	if len(last.ThisHost) > 0 {
		fmt.Fprintf(out, "WARNING: %d %s backend(s) from this host did not disconnect within %s after SIGTERM.\n",
			len(last.ThisHost), appName, budget.Truncate(time.Second))
		fmt.Fprintf(out, "Persistent PIDs (this host): %v\n", last.ThisHost)
		fmt.Fprintf(out, "If no aveloxis process for this component (%s) is running on this host (`ps`), these backends are orphans.\n", appName)
		fmt.Fprintln(out, "Terminate them with:")
		for _, pid := range last.ThisHost {
			fmt.Fprintf(out, "  SELECT pg_terminate_backend(%d);\n", pid)
		}
	}
	if last.OtherHosts > 0 {
		fmt.Fprintf(out, "Note: %d %s backend(s) are connected from other client addresses (normally another machine running this component against the same database); they are not this host's and were left alone.\n",
			last.OtherHosts, appName)
	}
	if last.Hidden > 0 {
		fmt.Fprintf(out, "Note: %d %s backend(s) belong to a database role whose privileges this one does not hold, so their client address is not visible here (pg_stat_activity shows a session's address only to roles that HOLD that session's role's privileges, and to roles that hold pg_read_all_stats's) — no verdict from here; re-check as that role, or `GRANT pg_read_all_stats TO <this role>` (a plain grant to a role with INHERIT: a NOINHERIT member, or a grant made WITH INHERIT FALSE, is a member without the privileges and still reads NULL).\n",
			last.Hidden, appName)
	}
}

// stopAllHint tells the operator when `stop all` (serve/web/api) left a
// scancode worker running on this host, since the worker is deliberately
// outside `all`.
func stopAllHint(out io.Writer, running func(component string) bool) {
	if running(scancodeWorkerComponent) {
		fmt.Fprintf(out, "Note: the scancode worker is still running (it is not part of 'all'); stop it with `aveloxis stop %s`.\n", scancodeWorkerComponent)
	}
}
