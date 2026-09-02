// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

func TestDeployGateNeeded(t *testing.T) {
	cases := []struct {
		checklist, data, acked, want bool
	}{
		{true, true, false, true},   // existing fleet, not acked → gate
		{true, true, true, false},   // acked → pass
		{true, false, false, false}, // fresh install → pass
		{false, true, false, false}, // no checklist for this version → pass
	}
	for _, c := range cases {
		if got := deployGateNeeded(c.checklist, c.data, c.acked); got != c.want {
			t.Errorf("deployGateNeeded(%v,%v,%v) = %v, want %v", c.checklist, c.data, c.acked, got, c.want)
		}
	}
}

// The current binary version must have a checklist (this release ships
// data-side heals) so the gate is not dead for v0.29.0.
func TestCurrentVersionHasDeployChecklist(t *testing.T) {
	if _, ok := deployChecklistFor(db.ToolVersion); !ok {
		t.Fatalf("aveloxis %s has data-side heals but no deploy checklist — the start gate would be a no-op", db.ToolVersion)
	}
}

type fakeGate struct {
	hasData, acked bool
	recorded       bool
}

func (f *fakeGate) FleetHasCollectedData(context.Context) (bool, error)   { return f.hasData, nil }
func (f *fakeGate) DeployAckExists(context.Context, string) (bool, error) { return f.acked, nil }
func (f *fakeGate) RecordDeployAck(context.Context, string, string) error {
	f.recorded = true
	return nil
}

// A non-interactive invocation with an un-acked existing fleet REFUSES
// (proceed=false) — a systemd/nohup serve can't silently skip the heals.
func TestCheckDeployReadinessNonInteractiveRefuses(t *testing.T) {
	g := &fakeGate{hasData: true, acked: false}
	var out bytes.Buffer
	// os.Stdin under `go test` is not a char device → non-interactive.
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.0", false, os.Stdin, &out)
	if err != nil {
		t.Fatal(err)
	}
	if proceed {
		t.Fatal("a non-interactive start with un-acked deploy steps must refuse")
	}
	if !strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.0") {
		t.Fatalf("the checklist must be printed:\n%s", out.String())
	}
}

// --skip-deploy-check bypasses (for automation).
func TestCheckDeployReadinessSkipBypasses(t *testing.T) {
	g := &fakeGate{hasData: true, acked: false}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.0", true, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("--skip-deploy-check must proceed: proceed=%v err=%v", proceed, err)
	}
}

// An already-acked release proceeds silently (no checklist print).
func TestCheckDeployReadinessAckedPasses(t *testing.T) {
	g := &fakeGate{hasData: true, acked: true}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.0", false, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("acked release must proceed: proceed=%v err=%v", proceed, err)
	}
	if out.Len() != 0 {
		t.Fatalf("acked release must not print the checklist:\n%s", out.String())
	}
}

// A fresh install (no collected data) proceeds silently.
func TestCheckDeployReadinessFreshInstallPasses(t *testing.T) {
	g := &fakeGate{hasData: false, acked: false}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.0", false, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("fresh install must proceed: proceed=%v err=%v", proceed, err)
	}
}

// Wiring: startCmd gates on `serve`/`all` and registers the flag; the
// two operator commands are registered in main.
func TestStartCmdGatesOnDeploySteps(t *testing.T) {
	src := srctest.Read(t, "cmd/aveloxis/main.go")
	if !strings.Contains(src, "runDeployGate(*cfgPath, skipDeployCheck)") {
		t.Error("startCmd must call runDeployGate")
	}
	if !strings.Contains(src, `slices.Contains(components, "serve")`) {
		t.Error("the deploy gate must fire for serve/all (components contains serve)")
	}
	if !strings.Contains(src, "--skip-deploy-check") {
		t.Error("start must register --skip-deploy-check")
	}
	for _, name := range []string{"deployChecklistCmd()", "ackDeployCmd(&cfgPath)"} {
		if !strings.Contains(src, name) {
			t.Errorf("%s must be registered in main", name)
		}
	}
}

// TestDeployChecklistStartsWithStopAndHealIsUsable (Copilot round 21 on
// PR #193): the gated checklist IS the ordered deploy procedure, so it
// must (a) run `aveloxis stop all` before any schema change — never
// migrate under a live serve — and (b) print a USABLE mirror-link heal
// command: the script exits immediately at DB="${1:?}" without a
// database positional argument, so the bare form cannot be run.
func TestDeployChecklistStartsWithStopAndHealIsUsable(t *testing.T) {
	steps, ok := deployChecklistFor("0.29.0")
	if !ok || len(steps) == 0 {
		t.Fatal("0.29.0 checklist missing")
	}
	var buf bytes.Buffer
	printChecklist(&buf, "0.29.0", steps)
	out := buf.String()

	stopIdx := strings.Index(out, "aveloxis stop all")
	migrateIdx := strings.Index(out, "aveloxis migrate")
	if stopIdx < 0 || migrateIdx < 0 || stopIdx > migrateIdx {
		t.Errorf("checklist must run `aveloxis stop all` BEFORE `aveloxis migrate` — never migrate under a live serve (round 21)")
	}

	var sawHeal bool
	for _, line := range strings.Split(out, "\n") {
		if !strings.Contains(line, "heal_mirror_links.sh") {
			continue
		}
		sawHeal = true
		if !strings.Contains(line, "<database>") || !strings.Contains(line, "--dry-run") {
			t.Errorf("mirror-link heal command must carry a database argument AND --dry-run (the bare form exits at DB=\"${1:?}\"); got: %s", strings.TrimSpace(line))
		}
	}
	if !sawHeal {
		t.Fatal("mirror-link heal step missing from the checklist")
	}
}
