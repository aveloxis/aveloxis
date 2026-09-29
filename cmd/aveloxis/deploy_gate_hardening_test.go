// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// Round-11 finding 4: runDeployGate dialed on context.Background() with
// no deadline, and v0.29.4 removed the no-checklist short-circuit so
// this now runs on EVERY `start serve` / `start all`. A database that
// accepts TCP but stalls the handshake (a pgbouncer restart, a half-open
// NAT connection, a pool at max_connections) blocked `start all` forever
// — before web and api ever launched.
//
// The bound goes on the DIAL ONLY. checkDeployReadiness blocks on the
// interactive `[y/N]` ReadString; a gate-wide deadline would expire
// while the operator reads the checklist and make RecordDeployAck fail
// for anyone who takes longer than the timeout to answer. The precedent
// is verifyBackendsDisconnected (pass 41), which gives its dial its own
// 30s ctx for exactly this reason.
func TestRunDeployGateBoundsTheDialOnly(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/deploy_checklist.go")
	// PR #218 fix review r8 F1: the dial lives in openDeployGate, the one
	// opener shared with `deploy-checklist --pending`; the gate call stays
	// in runDeployGate.
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func openDeployGate("))
	gate := srctest.StripGoComments(srctest.FuncBody(t, src, "func runDeployGate("))
	if !strings.Contains(gate, "openDeployGate(") {
		t.Fatal("runDeployGate must open its store through openDeployGate")
	}

	if !strings.Contains(body, "context.WithTimeout") {
		t.Fatal("runDeployGate must bound the DIAL: db.NewPostgresStore pings the pool, and an\n" +
			"unbounded context.Background() lets a stalled handshake block `start all` forever\n" +
			"before web and api launch (round-11 finding 4).")
	}

	dialIdx := strings.Index(body, "NewPostgresStore(")
	if dialIdx < 0 {
		t.Fatal("runDeployGate must still construct the store")
	}
	timeoutIdx := strings.Index(body, "context.WithTimeout")
	if timeoutIdx > dialIdx {
		t.Errorf("the bounded context must be created BEFORE the dial it bounds.\nbody:\n%s", body)
	}

	// The dial must receive the bounded ctx...
	dialArgs := body[dialIdx:min(len(body), dialIdx+80)]
	if !strings.Contains(dialArgs, "dialCtx") {
		t.Errorf("NewPostgresStore must take the bounded dial context, not the gate's ctx.\ngot: %s", dialArgs)
	}
	// ...and checkDeployReadiness must NOT — it blocks on the operator's
	// [y/N] answer, which no deadline may cut short.
	// PR #218 fix review r1 F2: the body is checkDeployReadinessNaming
	// (it also returns the migrate its refusal named).
	gateIdx := strings.Index(gate, "checkDeployReadinessNaming(")
	if gateIdx < 0 {
		t.Fatal("runDeployGate must still call checkDeployReadinessNaming")
	}
	gateArgs := gate[gateIdx:min(len(gate), gateIdx+80)]
	if strings.Contains(gateArgs, "dialCtx") {
		t.Errorf("checkDeployReadiness must NOT run under the dial's deadline — it blocks on the\n"+
			"interactive `[y/N]` prompt, and an expired ctx there would make RecordDeployAck fail\n"+
			"for an operator who reads the checklist for longer than the dial timeout.\ngot: %s", gateArgs)
	}
}

// Round-11 finding 7: in the deployStepsProvablyUnrun block,
// `--skip-deploy-check` returned BEFORE printing the checklist — so the
// one path that has PROVEN the steps did not run was the one path that
// did not show the operator what to run. Every other bypass path
// (the un-acked branch below it) prints the steps first.
func TestSkipDeployCheckStillPrintsTheChecklist(t *testing.T) {
	g := &fakeGate{hasData: true, acked: false, stamp: "0.29.2"}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", true, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("--skip-deploy-check must proceed: proceed=%v err=%v", proceed, err)
	}
	s := out.String()
	if !strings.Contains(s, "Deployment steps for aveloxis 0.29.3") {
		t.Fatalf("the bypass must print the steps it just proved were not run — this is the one\n"+
			"path with EVIDENCE the operator has work to do (round-11 finding 7):\n%s", s)
	}
	// The warning still leads; the steps follow it.
	warnIdx := strings.Index(s, "Proceeding anyway")
	stepsIdx := strings.Index(s, "Deployment steps for aveloxis")
	if warnIdx < 0 || stepsIdx < warnIdx {
		t.Errorf("the WARNING must precede the checklist:\n%s", s)
	}
	if g.recorded {
		t.Fatal("a bypass records nothing")
	}
}

// Round-11 finding 11: deployGateNeeded(hasChecklist, fleetHasData,
// acked) was reached only after `if !fleetHasData { return }` and after
// deployChecklistFor produced a non-empty checklist — so two of its
// three arguments were provably true and the whole call reduced to
// `!acked`. It advertised a decision surface that did not exist.
// Removed outright (remove-don't-deprecate); this pin keeps it removed.
func TestDeployGateNeededRemoved(t *testing.T) {
	src := srctest.StripGoComments(srctest.Read(t, "cmd/aveloxis/deploy_checklist.go"))
	if strings.Contains(src, "deployGateNeeded") {
		t.Error("deployGateNeeded reduced to !acked at its only call site (hasChecklist and\n" +
			"fleetHasData are provably true by then) — a three-argument predicate advertising a\n" +
			"decision surface that does not exist. Keep it removed; the call site reads `if acked`.")
	}
}
