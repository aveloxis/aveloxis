// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

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
	stamp          string // "" = unstamped (the pre-v0.29.4 fixtures)
	stampErr       error
	otherServe     bool
	otherErr       error
	listErr        error // round 6: a confirmed positive whose address listing failed
	otherProbed    bool
}

func (f *fakeGate) FleetHasCollectedData(context.Context) (bool, error)   { return f.hasData, nil }
func (f *fakeGate) DeployAckExists(context.Context, string) (bool, error) { return f.acked, nil }
func (f *fakeGate) SchemaVersion(context.Context) (string, error)         { return f.stamp, f.stampErr }
func (f *fakeGate) OtherServeConnected(context.Context) (db.OtherServe, error) {
	f.otherProbed = true
	if f.otherErr != nil {
		return db.OtherServe{}, f.otherErr
	}
	sight := db.OtherServe{Connected: f.otherServe, ListErr: f.listErr}
	if f.otherServe && f.listErr == nil {
		sight.From = []string{"127.0.0.1 (other address)"}
	}
	return sight, nil
}
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
	// Round 8: this pinned the literal "--skip-deploy-check", which was
	// satisfied by the abort MESSAGE, not by the registration — and the
	// message moved to startAbortMessage (round-8 finding 4). Pin the
	// flag declaration itself, which is what the failure message claims.
	if !strings.Contains(src, `BoolVar(&skipDeployCheck, "skip-deploy-check"`) {
		t.Error("start must register the --skip-deploy-check flag")
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

// v0.29.4 (the 2026-09-09 scancode-runner incident): a second host's
// `start serve` with a NEWER binary than the schema stamp was prompted
// and wrote a deploy_ack for 0.29.3 onto production's ledger — for
// steps that provably had not completed (the stamp moves only when a
// migration of the binary completes — step 2 of the ladder). The gate
// now reads the evidence before it trusts either the ledger or the
// prompt.
func TestDeployStepsProvablyUnrun(t *testing.T) {
	cases := []struct {
		stamp, binary string
		unrun         bool
	}{
		{"0.29.2", "0.29.3", true},
		{"0.29.3", "0.29.3", false},
		{"0.29.10", "0.29.9", false}, // stamp ahead: not our concern here
		{"", "0.29.3", false},        // unstamped: unknown, the prompt flow decides
		{"garbage", "0.29.3", true},  // unparseable: fail closed
	}
	for _, c := range cases {
		if got := deployStepsProvablyUnrun(c.stamp, c.binary); got != c.unrun {
			t.Errorf("deployStepsProvablyUnrun(%q, %q) = %v, want %v", c.stamp, c.binary, got, c.unrun)
		}
	}
}

// A stamp behind the binary refuses BEFORE the ledger is consulted and
// before any prompt: even an existing (foreign) ack does not pass, and
// nothing is recorded. The refusal names the evidence and both ways out.
func TestCheckDeployReadinessRefusesWhenStampBehindBinary(t *testing.T) {
	g := &fakeGate{hasData: true, acked: true, stamp: "0.29.2"}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err != nil {
		t.Fatal(err)
	}
	if proceed {
		t.Fatal("a stamp behind the binary must refuse even when the ledger carries an ack")
	}
	if g.recorded {
		t.Fatal("nothing may be acknowledged on the fleet ledger from a refusal")
	}
	s := out.String()
	for _, want := range []string{"0.29.2", "0.29.3", "step 2", "aveloxis migrate --skip-views", "has not completed", "aveloxis start scancode-worker", "--skip-deploy-check"} {
		if !strings.Contains(s, want) {
			t.Errorf("the refusal must carry %q:\n%s", want, s)
		}
	}
	if strings.Contains(s, "Have you completed") {
		t.Errorf("no prompt when the evidence says the steps did not run:\n%s", s)
	}
}

// --skip-deploy-check still bypasses (automation), with the evidence
// printed and still nothing recorded.
func TestCheckDeployReadinessSkipProceedsWhenStampBehind(t *testing.T) {
	g := &fakeGate{hasData: true, acked: false, stamp: "0.29.2"}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", true, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("--skip-deploy-check must proceed: proceed=%v err=%v", proceed, err)
	}
	if g.recorded {
		t.Fatal("a bypass records nothing")
	}
	if !strings.Contains(out.String(), "0.29.2") || !strings.Contains(out.String(), "--skip-deploy-check") {
		t.Errorf("the bypass must still print the evidence:\n%s", out.String())
	}
	// Round-5 finding 4: a path that proceeds must not say it refuses.
	if strings.Contains(out.String(), "Refusing") || !strings.Contains(out.String(), "Proceeding anyway") {
		t.Errorf("the bypass must read as a warning that proceeds, not a refusal:\n%s", out.String())
	}
}

// A failed stamp read is not "unstamped" (SR-5): the gate fails closed.
func TestCheckDeployReadinessStampProbeErrorFailsClosed(t *testing.T) {
	g := &fakeGate{hasData: true, acked: true, stampErr: errors.New("boom")}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err == nil || proceed {
		t.Fatalf("a stamp probe error must surface: proceed=%v err=%v", proceed, err)
	}
	if g.recorded {
		t.Fatal("nothing may be acknowledged on a probe error")
	}
}

// With the stamp current, the pre-v0.29.4 flow is unchanged: an acked
// release passes silently, an un-acked one prints the checklist.
func TestCheckDeployReadinessStampCurrentKeepsPromptFlow(t *testing.T) {
	g := &fakeGate{hasData: true, acked: true, stamp: "0.29.3"}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err != nil || !proceed || out.Len() != 0 {
		t.Fatalf("current stamp + ack must pass silently: proceed=%v err=%v out=%q", proceed, err, out.String())
	}
	g = &fakeGate{hasData: true, acked: false, stamp: "0.29.3"}
	out.Reset()
	proceed, err = checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err != nil || proceed {
		t.Fatalf("current stamp, un-acked, non-interactive must refuse with the checklist: proceed=%v err=%v", proceed, err)
	}
	if !strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.3") {
		t.Fatalf("the checklist must be printed:\n%s", out.String())
	}
}

// Round-4 finding 3: on an existing fleet, `start serve` says out loud
// when another aveloxis-serve is already connected — the dedicated-host
// mistake with the version drift removed passes both refusals — and
// names the way out. Observation only: it never blocks (two serves may
// be deliberate), a failed probe is reported and never treated as "no
// other serve", and a fresh fleet is not probed at all.
func TestCheckDeployReadinessNotesAnotherServe(t *testing.T) {
	g := &fakeGate{hasData: true, acked: true, stamp: "0.29.3", otherServe: true}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("another serve is a note, never a refusal: proceed=%v err=%v", proceed, err)
	}
	for _, want := range []string{"another aveloxis-serve", "from 127.0.0.1 (other address)", "aveloxis start scancode-worker", "aveloxis stop"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the note must carry %q (the address and both readings — the primary, or a backend of a serve just stopped):\n%s", want, out.String())
		}
	}

	g = &fakeGate{hasData: true, acked: true, stamp: "0.29.2", otherServe: true}
	out.Reset()
	proceed, _ = checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if proceed || !strings.Contains(out.String(), "another aveloxis-serve") {
		t.Errorf("a stamp refusal still carries the note:\n%s", out.String())
	}

	g = &fakeGate{hasData: true, acked: true, stamp: "0.29.3", otherErr: errors.New("boom")}
	out.Reset()
	proceed, err = checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err != nil || !proceed || !strings.Contains(out.String(), "could not check") || !strings.Contains(out.String(), "boom") {
		t.Errorf("a failed probe is said, not folded into all-clear: proceed=%v err=%v\n%s", proceed, err, out.String())
	}

	g = &fakeGate{hasData: false, otherServe: true}
	out.Reset()
	if proceed, err := checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out); err != nil || !proceed || g.otherProbed || out.Len() != 0 {
		t.Errorf("a fresh fleet is neither probed nor noted: proceed=%v err=%v probed=%v out=%q", proceed, err, g.otherProbed, out.String())
	}

	// Round-6 finding 3: a CONFIRMED positive whose address listing
	// failed is still another serve — the note (and its way out) is
	// printed, with the address honestly unrecorded.
	g = &fakeGate{hasData: true, acked: true, stamp: "0.29.3", otherServe: true, listErr: errors.New("boom")}
	out.Reset()
	proceed, err = checkDeployReadiness(context.Background(), g, "0.29.3", false, os.Stdin, &out)
	if err != nil || !proceed {
		t.Fatalf("a listing failure is not a probe failure: proceed=%v err=%v", proceed, err)
	}
	for _, want := range []string{"another aveloxis-serve", "aveloxis start scancode-worker", "unrecorded address", "boom"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("the note must still be printed with %q:\n%s", want, out.String())
		}
	}
	if strings.Contains(out.String(), "could not check") || strings.Contains(out.String(), "[]") {
		t.Errorf("a confirmed sighting must not read as unknown or as an empty list:\n%s", out.String())
	}
}

// Round-5 finding 1 (L4): the stamp evidence and the other-serve note
// belong to every existing-fleet `start serve`, not to the releases
// that happen to carry a deploy checklist — a checklist-less release
// would otherwise reopen the incident's shape (binary ahead of the
// stamp, `start serve` proceeding to a full migration).
func TestCheckDeployReadinessStampGateIsIndependentOfTheChecklist(t *testing.T) {
	// Dotted-numeric on purpose: an unparseable stamp refuses by design.
	const noChecklist = "0.0.1"
	if _, ok := deployChecklistFor(noChecklist); ok {
		t.Fatal("fixture: this version must have no checklist")
	}
	g := &fakeGate{hasData: true, acked: false, stamp: "0.0.0"}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, noChecklist, false, os.Stdin, &out)
	if err != nil || proceed || g.recorded || !strings.Contains(out.String(), "Refusing to start") {
		t.Errorf("stamp behind the binary must refuse even without a checklist: proceed=%v err=%v recorded=%v\n%s", proceed, err, g.recorded, out.String())
	}

	g = &fakeGate{hasData: true, acked: false, stamp: noChecklist, otherServe: true}
	out.Reset()
	proceed, err = checkDeployReadiness(context.Background(), g, noChecklist, false, os.Stdin, &out)
	if err != nil || !proceed || strings.Contains(out.String(), "Deployment steps") {
		t.Errorf("a current stamp without a checklist proceeds with no prompt: proceed=%v err=%v\n%s", proceed, err, out.String())
	}
	if !strings.Contains(out.String(), "another aveloxis-serve") {
		t.Errorf("the other-serve note does not depend on the checklist either:\n%s", out.String())
	}

	g = &fakeGate{hasData: false, stamp: "0.0.0"}
	out.Reset()
	if proceed, err := checkDeployReadiness(context.Background(), g, noChecklist, false, os.Stdin, &out); err != nil || !proceed || out.Len() != 0 {
		t.Errorf("a fresh fleet still starts silently: proceed=%v err=%v out=%q", proceed, err, out.String())
	}
	// Wiring: runDeployGate must not short-circuit on "no checklist"
	// before the evidence runs.
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/deploy_checklist.go"), "func runDeployGate("))
	if strings.Contains(body, "deployChecklistFor(") {
		t.Error("runDeployGate must always consult checkDeployReadiness on the store — the checklist short-circuit belongs inside it, after the evidence")
	}
}

// blockingGate blocks its FIRST query until the caller's context ends —
// the shape a concurrent `aveloxis migrate` produces. addColumnIfMissing
// issues its ALTERs unconditionally (server-side IF NOT EXISTS), so
// Postgres takes ACCESS EXCLUSIVE on collection_queue on EVERY migrate,
// and FleetHasCollectedData — the gate's FIRST call — queues behind it.
type blockingGate struct{ fakeGate }

func (b *blockingGate) FleetHasCollectedData(ctx context.Context) (bool, error) {
	<-ctx.Done()
	return false, ctx.Err()
}

// L10 finding 2 (on round 11's own finding-4 fix). runDeployGate bounded
// the DIAL and then handed checkDeployReadiness a fresh unbounded
// context.Background(), so all four pre-prompt queries could block
// `start all` forever — before web and api ever launch, which is
// precisely what deployGateDialTimeout's own comment says the bound
// prevents. Not a regression (there was no bound at all before round
// 11) but the fix under-delivered against its own stated purpose.
func TestDeployGateBoundsItsPrePromptQueries(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	var out bytes.Buffer
	start := time.Now()
	proceed, err := checkDeployReadiness(ctx, &blockingGate{}, "0.29.4", false, nil, &out)
	elapsed := time.Since(start)

	if err == nil {
		t.Error("a pre-prompt query that outlives the bound must surface an error, not proceed silently")
	}
	if proceed {
		t.Error("a gate that could not read the fleet state must not report proceed=true (fail closed)")
	}
	if elapsed > 5*time.Second {
		t.Errorf("checkDeployReadiness took %v — the pre-prompt queries must honor the caller's deadline so `start all` reaches web and api", elapsed)
	}
}

// The counter-test, and the reason the bound cannot simply wrap the
// whole call: checkDeployReadiness blocks on the interactive `[y/N]`,
// and an operator who reads the checklist for longer than the query
// bound must still get their acknowledgement RECORDED. The prompt needs
// a real TTY (isInteractive is a char-device stat, so a pipe refuses
// before the prompt), so the contract is pinned where it lives —
// deployAckContext — rather than through a pty fixture.
func TestDeployAckContextSurvivesAnExpiredCaller(t *testing.T) {
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	if expired.Err() == nil {
		t.Fatal("fixture: the caller's context should be done")
	}

	ackCtx, ackCancel := deployAckContext(expired)
	defer ackCancel()

	if ackCtx.Err() != nil {
		t.Error("deployAckContext must strip the caller's cancellation — an operator who reads the " +
			"checklist for longer than the pre-prompt query bound would otherwise lose the " +
			"acknowledgement they just gave")
	}
	if _, ok := ackCtx.Deadline(); !ok {
		t.Error("deployAckContext must still BOUND the write — an unbounded ack is the same hang one " +
			"statement later (the write takes its own locks)")
	}
}

// The wiring pin: runDeployGate must not hand the readiness check a
// fresh unbounded context.Background().
func TestRunDeployGateDoesNotPassAnUnboundedContext(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/deploy_checklist.go"), "func runDeployGate("))
	if strings.Contains(body, "checkDeployReadiness(context.Background()") {
		t.Error("runDeployGate must bound checkDeployReadiness's pre-prompt queries — an unbounded context " +
			"lets a concurrent migrate's ACCESS EXCLUSIVE on collection_queue block `start all` forever, " +
			"before web and api ever launch (L10 finding 2)")
	}
	if !strings.Contains(body, "deployGateDialTimeout") {
		t.Error("runDeployGate must derive its query bound from the named timeout, not a fresh literal")
	}
	check := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/deploy_checklist.go"), "func checkDeployReadiness("))
	if !strings.Contains(check, "deployAckContext(ctx)") {
		t.Error("RecordDeployAck must run on deployAckContext(ctx) — the caller's bound belongs to the " +
			"pre-prompt READS, and the operator's answer arrives after it has expired")
	}
}
