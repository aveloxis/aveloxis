// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"strconv"
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
// command, dry run first. The script reads its connection and database
// from aveloxis.json since then, so the command needs no database
// argument (Copilot on PR #210: the `<database>` placeholder it carried
// was a shell redirection).
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
		if !strings.Contains(line, "--dry-run") {
			t.Errorf("mirror-link heal command must be the dry run first; got: %s", strings.TrimSpace(line))
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

// TestV02957ChecklistAppliesTheNewViewDefinition — v0.29.57. The release
// changes explorer_libyear_summary's DEFINITION (it now orders NULLS LAST).
// A view definition is applied only by a plain `aveloxis migrate`:
// `migrate --skip-views` skips the views entirely, `refresh-views`
// refreshes their data under the definition they already have, and serve
// never re-creates a view at startup. Deployed by the standard ladder, the
// new ordering never shipped — and heal-libyear, which raises the share of
// repos with no libyear, made the old ordering's problem worse. Found while
// writing the release notes, after nine review rounds had passed it.
//
// Scoped by meaning, not by map key (post-loop review finding 4): every
// checklist that carries the libyear heal is one an operator can use to
// cross v0.29.57, so each must also apply the definition — a later
// release that reuses the heal on a --skip-views ladder fails here.
func TestV02957ChecklistAppliesTheNewViewDefinition(t *testing.T) {
	if _, ok := deployChecklistFor("0.29.57"); !ok {
		t.Fatal("0.29.57 must have a deploy checklist")
	}
	examined := 0
	for version, steps := range deployChecklists {
		carriesHeal := false
		for _, s := range steps {
			if s.cmd == "aveloxis heal-libyear --apply" {
				carriesHeal = true
			}
		}
		if !carriesHeal {
			continue
		}
		examined++
		checkChecklistAppliesLibyearView(t, version, steps)
	}
	if examined == 0 {
		t.Fatal("no checklist carries `aveloxis heal-libyear --apply` — 0.29.57's must")
	}
}

func checkChecklistAppliesLibyearView(t *testing.T, version string, steps []deployStep) {
	t.Helper()
	migrateAt, verifyAt, healAt, refreshAt := -1, -1, -1, -1
	for i, s := range steps {
		switch {
		case s.cmd == "aveloxis migrate":
			migrateAt = i
		case strings.HasPrefix(s.cmd, "aveloxis migrate") && strings.Contains(s.cmd, "--skip-views"):
			t.Errorf("%s step %d is %q: --skip-views skips the view definitions, so explorer_libyear_summary keeps its old ordering", version, i+1, s.cmd)
		case strings.Contains(s.cmd, "pg_get_viewdef('aveloxis_data.explorer_libyear_summary')") && strings.Contains(s.cmd, "NULLS LAST"):
			verifyAt = i
		case s.cmd == "aveloxis heal-libyear --apply":
			healAt = i
		case s.cmd == "aveloxis refresh-views":
			refreshAt = i
		}
	}
	if migrateAt < 0 {
		t.Fatalf("%s needs a plain `aveloxis migrate` step — it is the only step that re-creates a view from its new definition", version)
	}
	// Post-loop review finding 3: migrate's view block is warn-only, so a
	// failed re-create exits 0 and stamps the schema. The checklist must
	// check the OUTCOME before the heals, not trust the exit status.
	if verifyAt < migrateAt || healAt < verifyAt {
		t.Errorf("%s needs a check that the new definition is in place between the migrate (step %d) and the heal (step %d); found it at step %d", version, migrateAt+1, healAt+1, verifyAt+1)
	}
	if healAt < migrateAt || refreshAt < healAt {
		t.Errorf("%s: order must be migrate (step %d) → heal-libyear --apply (step %d) → refresh-views (step %d): the migrate applies the definition, the heal changes the rows, and the refresh makes the view reflect them", version, migrateAt+1, healAt+1, refreshAt+1)
	}
}

// The check step's description names the WARN a failed re-create logs. A
// renamed log line would leave the operator searching for text that is
// never printed, so the quoted text must exist in RunMigrations' view block.
func TestV02957ViewCheckQuotesARealLogLine(t *testing.T) {
	steps, _ := deployChecklistFor("0.29.57")
	const warn = "materialized view creation had errors"
	quoted := false
	for _, s := range steps {
		if strings.Contains(s.cmd, "pg_get_viewdef") && strings.Contains(s.desc, warn) {
			quoted = true
		}
	}
	if !quoted {
		t.Fatalf("the view-definition check must tell the operator which WARN means the re-create failed (%q)", warn)
	}
	// L10 pass finding 1: a bare `psql` connects with libpq defaults
	// (local socket, port 5432), not aveloxis.json's database block — on a
	// fleet whose database is elsewhere the check fails to connect or
	// answers for a different database. It must name every connection
	// field, each from a variable the operator sets that stops the command
	// when unset (Copilot on PR #210: `<host>`-style placeholders are shell
	// redirections, so the pasted line failed).
	for _, s := range steps {
		if !strings.Contains(s.cmd, "pg_get_viewdef") {
			continue
		}
		for _, field := range []string{`-h "${PGHOST:?}"`, `-p "${PGPORT:?}"`, `-U "${PGUSER:?}"`, `-d "${PGDATABASE:?}"`} {
			if !strings.Contains(s.cmd, field) {
				t.Errorf("the view check must connect with aveloxis.json's database block; %q is missing from %q", field, s.cmd)
			}
		}
		if !strings.Contains(s.desc, "aveloxis.json") {
			t.Errorf("the view check's description must say where the connection values come from (aveloxis.json's database block): %q", s.desc)
		}
	}
	// Scoped to the branch a plain migrate takes: the create-if-missing
	// branch logs the same text, so a match anywhere in RunMigrations
	// survived renaming the one that matters (mutation proof, this round).
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/db/migrate.go"), "func RunMigrations("))
	start := strings.Index(body, "case MatviewsRebuild:")
	if start < 0 {
		t.Fatal("RunMigrations has no `case MatviewsRebuild:` branch — the plain-migrate view block moved; re-scope this pin")
	}
	branch := body[start:]
	if end := strings.Index(branch, "case MatviewsIfMissing:"); end >= 0 {
		branch = branch[:end]
	} else {
		t.Fatal("cannot find the end of the MatviewsRebuild branch (`case MatviewsIfMissing:`)")
	}
	if !strings.Contains(branch, "CreateMaterializedViews(") {
		t.Fatal("the MatviewsRebuild branch no longer calls CreateMaterializedViews — re-scope this pin")
	}
	if !strings.Contains(branch, `logger.Warn("`+warn+`"`) {
		t.Errorf("a plain migrate's failed view re-create no longer logs %q — update the 0.29.57 checklist's view check to the line it logs now", warn)
	}
}

// Post-loop review finding 2: the deploy gate's refusal hardcoded
// `aveloxis migrate --skip-views` as "step 2", so an operator who ran
// `start all` first and did what the refusal said never applied 0.29.57's
// view definition. The gate now names the migrate step of the checklist it
// is enforcing; a version with no checklist keeps the standard one.
func TestDeployGateNamesThisReleasesMigrateStep(t *testing.T) {
	if got := ladderMigrateStep("0.29.57"); got != "aveloxis migrate" {
		t.Fatalf("ladderMigrateStep(0.29.57) = %q, want the checklist's plain migrate", got)
	}
	if got := ladderMigrateStep("0.29.3"); got != "aveloxis migrate --skip-views" {
		t.Errorf("ladderMigrateStep(0.29.3) = %q, want its checklist's `aveloxis migrate --skip-views`", got)
	}
	const noChecklist = "0.0.0-no-checklist"
	if got := ladderMigrateStep(noChecklist); got != "aveloxis migrate --skip-views" {
		t.Errorf("ladderMigrateStep(%s) = %q, want the standard ladder's step", noChecklist, got)
	}

	for _, skip := range []bool{false, true} {
		g := &fakeGate{hasData: true, stamp: "0.29.55"}
		var out bytes.Buffer
		if _, err := checkDeployReadiness(context.Background(), g, "0.29.57", skip, os.Stdin, &out); err != nil {
			t.Fatal(err)
		}
		// Everything BEFORE the printed checklist is the gate's own advice;
		// the checklist itself (skip path) is covered by the test above.
		advice := out.String()
		if i := strings.Index(advice, "=== Deployment steps"); i >= 0 {
			advice = advice[:i]
		}
		if strings.Contains(advice, "--skip-views") {
			t.Errorf("skip=%v: the 0.29.57 gate must not send the operator to --skip-views:\n%s", skip, advice)
		}
		if !strings.Contains(advice, "(`aveloxis migrate`)") {
			t.Errorf("skip=%v: the 0.29.57 gate must name its checklist's plain `aveloxis migrate` as step 2:\n%s", skip, advice)
		}
	}
	if msg := startAbortMessage("0.29.57"); strings.Contains(msg, "--skip-views") || !strings.Contains(msg, "`aveloxis migrate`") {
		t.Errorf("start's abort line for 0.29.57 must name `aveloxis migrate`, not --skip-views:\n%s", msg)
	}
}

// L10 round 3 finding 5: the email_message-index precondition refusals in
// reconcile-repos and dedup-repos named `aveloxis migrate --skip-views`.
// Followed on an undeployed v0.29.57 binary, that stamps the schema around
// the release's view definitions, so they name db.DeployStepsAdvice.
func TestIndexPreconditionRefusalsNameTheDeploySteps(t *testing.T) {
	examined := 0
	for _, file := range []string{"cmd/aveloxis/reconcile_repos.go", "cmd/aveloxis/dedup_repos.go"} {
		src := srctest.StripGoComments(srctest.Read(t, file))
		if strings.Contains(src, "`aveloxis migrate --skip-views` on this binary first") {
			t.Errorf("%s: a precondition refusal still names `aveloxis migrate --skip-views`; use db.DeployStepsAdvice", file)
		}
		if n := strings.Count(src, `db.DeployStepsAdvice+" on this binary first`); n == 0 {
			t.Errorf("%s: the precondition refusal must send the operator to db.DeployStepsAdvice", file)
		} else {
			examined += n
		}
	}
	if examined < 2 {
		t.Errorf("found %d refusal sites using db.DeployStepsAdvice, want at least the reconcile-repos and dedup-repos refusals", examined)
	}

	// L10 round 4: the returned error is what cobra prints last on the
	// nonzero exit, so it carries the same advice as the ERROR log.
	reconcile := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/reconcile_repos.go"), "func runReconcileRepos("))
	found := false
	for _, line := range strings.Split(reconcile, "\n") {
		if strings.Contains(line, "stranded repos refused for the email_message index precondition") {
			found = true
			if !strings.Contains(line, "db.DeployStepsAdvice") {
				t.Errorf("reconcile-repos' returned precondition error must pass db.DeployStepsAdvice: %s", strings.TrimSpace(line))
			}
		}
	}
	if !found {
		t.Error("cannot find reconcile-repos' returned precondition error — re-scope this pin")
	}
}

// L10 round 4 finding 1: dedup-repos' advice to run `migrate --skip-views`
// (to build the backstop index, and in the help's precondition) is reached
// BEFORE any deploy on two paths: the no-duplicates return comes before the
// index precondition, and the precondition checks index validity, never the
// schema stamp, so a v0.28.18+ fleet passes it on an undeployed binary.
// Every mention must therefore come AFTER the release's deploy steps.
//
// Round 5 made this a syntax-level check: the first version matched per
// source line, and a re-wrapped help line, a mention placed before the
// condition, and a message split across `+` literals all escaped it. Now
// every string expression in the file (a literal, or a chain of literals
// joined by `+`) is evaluated, split into paragraphs, whitespace-collapsed,
// and each `--skip-views` must have `deploy-checklist` earlier in its
// paragraph. Round 6 matched the flag alone (it exists only on migrate, so
// `migrate --no-wait --skip-views`, a `%s` for "migrate", and a literal cut
// off by a non-literal operand are all still seen) and split paragraphs on
// whitespace-only lines, not just "\n\n".
func TestDedupReposSendsTheDeployStepsFirst(t *testing.T) {
	strs := stringExpressionsIn(t, "cmd/aveloxis/dedup_repos.go")
	paragraphBreak := regexp.MustCompile(`\n[ \t]*\n`)
	const flag = "--skip-views"
	mentions := 0
	for _, str := range strs {
		for _, para := range paragraphBreak.Split(str, -1) {
			flat := strings.Join(strings.Fields(para), " ")
			for rest, offset := flat, 0; ; {
				i := strings.Index(rest, flag)
				if i < 0 {
					break
				}
				mentions++
				if !strings.Contains(flat[:offset+i], "deploy-checklist") {
					t.Errorf("dedup_repos.go names `migrate --skip-views` without the deploy steps before it:\n%s", flat)
				}
				offset += i + len(flag)
				rest = flat[offset:]
			}
		}
	}
	// The help's precondition and backstop paragraphs, the no-duplicates
	// INFO and the next-steps INFO. Fewer means a mention was dropped or
	// the scan stopped seeing one; re-derive before lowering.
	srctest.MinCount(t, "`--skip-views` mentions in dedup_repos.go's strings", mentions, 4)
}

// stringExpressionsIn returns the value of every string expression in a Go
// file: each string literal, with chains of literals joined by `+`
// evaluated as one string (so a message split across lines is seen whole).
func stringExpressionsIn(t *testing.T, repoRelPath string) []string {
	t.Helper()
	f, err := parser.ParseFile(token.NewFileSet(), repoRelPath, srctest.Read(t, repoRelPath), 0)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	ast.Inspect(f, func(n ast.Node) bool {
		e, ok := n.(ast.Expr)
		if !ok {
			return true
		}
		if v, ok := constStringExpr(e); ok {
			out = append(out, v)
			return false // its parts are already in v
		}
		return true
	})
	return out
}

// constStringExpr evaluates e when it is a string literal, or a chain of
// string literals joined by `+` (parenthesised or not). Anything else —
// an identifier, a call, a chain with a non-literal operand — reports false.
func constStringExpr(e ast.Expr) (string, bool) {
	switch x := e.(type) {
	case *ast.BasicLit:
		if x.Kind != token.STRING {
			return "", false
		}
		v, err := strconv.Unquote(x.Value)
		return v, err == nil
	case *ast.BinaryExpr:
		if x.Op != token.ADD {
			return "", false
		}
		l, ok := constStringExpr(x.X)
		if !ok {
			return "", false
		}
		r, ok := constStringExpr(x.Y)
		return l + r, ok
	case *ast.ParenExpr:
		return constStringExpr(x.X)
	}
	return "", false
}

// TestDeployChecklistCommandsHaveNoPlaceholders (Copilot on PR #210): an
// operator pastes these commands, and an unquoted `<host>` or `<database>`
// is a shell redirection, so the line fails before the command runs. The
// docs' shell fences are held to the same rule (srctest.AnglePlaceholder).
func TestDeployChecklistCommandsHaveNoPlaceholders(t *testing.T) {
	checked := 0
	for version, steps := range deployChecklists {
		for _, s := range steps {
			checked++
			if m := srctest.AnglePlaceholder.FindString(s.cmd); m != "" {
				t.Errorf("%s checklist: %q carries the placeholder %s — the operator pastes this line: use a quoted variable, or drop an argument the command reads from aveloxis.json", version, s.cmd, m)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no checklist step examined")
	}
}
