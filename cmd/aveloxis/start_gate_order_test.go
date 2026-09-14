// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round-8 finding 3: `start serve` ran the deploy gate BEFORE
// startComponent's already-running check, so a plain re-run on the
// primary probed the database, sighted its OWN live serve at
// "(this host)", printed both readings of that entry — and then said
// "serve is already running (PID N)" two lines later. The gate exists
// to stop a NEW serve from starting against an un-migrated or
// un-acknowledged fleet; a live serve on this host makes it moot,
// because startComponent is about to refuse the start anyway.
//
// The gate must therefore be skipped when serve is already running
// here. Pinned as a source contract because the alternative — driving
// startCmd's RunE — would need a live database and a live serve.
func TestStartSkipsDeployGateWhenServeAlreadyRunning(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func startCmd("))

	gateAt := strings.Index(body, "runDeployGate(")
	if gateAt < 0 {
		t.Fatal("startCmd must still call runDeployGate")
	}
	runAt := strings.Index(body, "componentAlreadyRunning(")
	if runAt < 0 {
		t.Fatal("startCmd must consult componentAlreadyRunning before running the deploy gate — a live serve on this host makes the gate moot and its other-serve note sights the operator's own serve")
	}
	if runAt > gateAt {
		t.Errorf("componentAlreadyRunning must be consulted BEFORE runDeployGate (running=%d gate=%d): probing the database first is exactly the round-8 finding-3 symptom", runAt, gateAt)
	}
	// The check has to be about serve specifically: `start all` still
	// starts web and api when serve is already up, and neither of
	// those was ever gated.
	if !strings.Contains(body, `componentAlreadyRunning("serve")`) {
		t.Error(`the already-running check must name "serve" — the gate only ever applied to the serve component`)
	}
}

// SR-17: "this host is already running that component" is ONE
// predicate. Before round 8 it was spelled inline four times —
// startComponent's double-start refusal, the `stop all` hint,
// run-scorecard's refusal to compete with a live serve, and
// run-scorecard's own bulk-pass lock — and the round-8 gate skip would
// have been a fifth. Four hand-spelled copies of a liveness test are
// four chances to disagree about whether a start is a no-op.
//
// stopComponent is deliberately NOT in the set: it branches on BOTH
// outcomes (a stale pidfile is cleaned up, a live one is signalled), so
// a boolean predicate cannot express what it needs.
func TestComponentAlreadyRunningIsTheOneSpelling(t *testing.T) {
	mainSrc := srctest.Read(t, "cmd/aveloxis/main.go")
	if !strings.Contains(srctest.StripGoComments(mainSrc), "func componentAlreadyRunning(") {
		t.Fatal("componentAlreadyRunning must exist as the shared liveness predicate")
	}
	for _, site := range []struct{ file, fn string }{
		{"cmd/aveloxis/main.go", "func startComponent("},
		{"cmd/aveloxis/run_scorecard.go", "func refuseIfServeRunning("},
		{"cmd/aveloxis/run_scorecard.go", "func acquireRunScorecardPidfile("},
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, site.file), site.fn))
		if !strings.Contains(body, "componentAlreadyRunning(") {
			t.Errorf("%s must route its liveness test through componentAlreadyRunning (SR-17)", site.fn)
		}
		if strings.Contains(body, "pidfile.IsRunning(") {
			t.Errorf("%s must not re-spell the liveness test inline (SR-17)", site.fn)
		}
	}
	// The `stop all` hint reads the same predicate through a bool
	// adapter (it wants only the answer, not the PID).
	stopBody := srctest.StripGoComments(srctest.FuncBody(t, mainSrc, "func stopCmd("))
	if !strings.Contains(stopBody, "componentAlreadyRunning(") {
		t.Error("the `stop all` hint must read componentAlreadyRunning, not a second inline spelling")
	}
	if strings.Contains(stopBody, "pidfile.IsRunning(") {
		t.Error("stopCmd must not re-spell the liveness test inline (SR-17)")
	}
}

// Round-8 finding 4: round 5 made the stamp refusal fire independently
// of deployChecklists, so for a version with no map entry
// checkDeployReadiness can only have returned false on the stamp
// evidence — and the abort line still told the operator that
// `aveloxis deploy-checklist` lists the steps, while that command
// answers "aveloxis <version> has no manual deploy steps". A dead end
// at exactly the moment the operator needs the way out. LATENT at
// 0.29.4 (the map has an entry); it fires the release the entry ages
// out, which the map's own comment schedules.
func TestStartAbortMessageBranchesOnChecklist(t *testing.T) {
	const noChecklist = "0.0.0-no-checklist"
	if _, ok := deployChecklistFor(noChecklist); ok {
		t.Fatalf("test fixture %q must not have a checklist", noChecklist)
	}

	msg := startAbortMessage(noChecklist)
	for _, dead := range []string{"deploy-checklist", "ack-deploy", "deploy steps"} {
		if strings.Contains(msg, dead) {
			t.Errorf("a version with no checklist must not point at %q — that command prints %q:\n%s", dead, "has no manual deploy steps", msg)
		}
	}
	if !strings.Contains(msg, "migrate --skip-views") {
		t.Errorf("the stamp remedy is the only reason left, so the message must carry it:\n%s", msg)
	}
	if !strings.Contains(msg, "--skip-deploy-check") {
		t.Errorf("the bypass stays named in both wordings:\n%s", msg)
	}

	// A version that DOES carry a checklist keeps both halves: either
	// reason can have produced the refusal.
	withChecklist := db.ToolVersion
	if _, ok := deployChecklistFor(withChecklist); !ok {
		t.Skipf("this binary's version (%s) has no checklist entry; the with-checklist arm is unreachable here", withChecklist)
	}
	msg = startAbortMessage(withChecklist)
	for _, needle := range []string{"deploy-checklist", "ack-deploy", "migrate --skip-views", "--skip-deploy-check"} {
		if !strings.Contains(msg, needle) {
			t.Errorf("a version WITH a checklist must still name %q — either reason can have refused:\n%s", needle, msg)
		}
	}

	// Wiring: startCmd must use the branching message, not a literal.
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/main.go"), "func startCmd("))
	if !strings.Contains(body, "startAbortMessage(") {
		t.Error("startCmd must build its abort line through startAbortMessage so the wording follows the version's checklist state")
	}
	if strings.Contains(body, "`aveloxis deploy-checklist` lists them") {
		t.Error("the unconditional wording must be gone from startCmd (round-8 finding 4)")
	}
}
