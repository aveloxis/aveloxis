// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestDeployStepsFromOrdersVersions pins the accumulated checklist's range
// (worklist §1 item 2): every version with a checklist after `after` up to
// the binary, compared as versions (0.29.9 < 0.29.10), oldest first;
// inclusive takes `after` itself (the stamp's own steps are unacknowledged
// when no ack exists).
func TestDeployStepsFromOrdersVersions(t *testing.T) {
	got := deployStepsFrom("0.29.65", false, "0.29.68")
	want := []string{"0.29.66", "0.29.67", "0.29.68"}
	if len(got) != len(want) {
		t.Fatalf("versions after 0.29.65 up to 0.29.68: %d; want %v", len(got), want)
	}
	for i, vs := range got {
		if vs.version != want[i] || len(vs.steps) == 0 {
			t.Errorf("[%d] = %s (%d steps); want %s", i, vs.version, len(vs.steps), want[i])
		}
	}
	if got := deployStepsFrom("0.29.65", true, "0.29.68"); len(got) != 4 || got[0].version != "0.29.65" {
		t.Errorf("inclusive from 0.29.65: %d versions, first %q; want 4 starting at 0.29.65", len(got), first(got))
	}
	if got := deployStepsFrom("0.29.9", false, "0.29.11"); len(got) != 2 || got[0].version != "0.29.10" {
		t.Errorf("0.29.9 → 0.29.11 must compare as versions: %v", versions(got))
	}
	if got := deployStepsFrom("0.29.68", false, "0.29.68"); len(got) != 0 {
		t.Errorf("nothing after the binary's own version: %v", versions(got))
	}
}

func first(vs []versionSteps) string {
	if len(vs) == 0 {
		return ""
	}
	return vs[0].version
}

func versions(vs []versionSteps) []string {
	out := make([]string, 0, len(vs))
	for _, v := range vs {
		out = append(out, v.version)
	}
	return out
}

// TestStartPrintsEverySkippedVersionsSteps: a fleet acked at 0.29.64 and
// migrated to the binary (0.29.68) is shown the steps of 0.29.65 through
// 0.29.68, not the current version's alone — the heals of skipped releases
// never ran. With no ack at all, the stamp's own version is included.
func TestStartPrintsEverySkippedVersionsSteps(t *testing.T) {
	g := &fakeGate{hasData: true, stamp: "0.29.68", latestAck: "0.29.64"}
	var out bytes.Buffer
	proceed, err := checkDeployReadiness(context.Background(), g, "0.29.68", false, os.Stdin, &out)
	if err != nil || proceed {
		t.Fatalf("proceed=%v err=%v; want a refusal (non-interactive, un-acked)", proceed, err)
	}
	for _, v := range []string{"0.29.65", "0.29.66", "0.29.67", "0.29.68"} {
		if !strings.Contains(out.String(), "Deployment steps for aveloxis "+v) {
			t.Errorf("the steps of %s (skipped since the last ack) are not printed:\n%s", v, out.String())
		}
	}
	if strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.64") {
		t.Error("the acknowledged version's steps are printed again")
	}
	// No ack ever recorded: from the stamp, inclusive.
	g = &fakeGate{hasData: true, stamp: "0.29.67"}
	out.Reset()
	if _, err := checkDeployReadiness(context.Background(), g, "0.29.68", true, os.Stdin, &out); err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"0.29.67", "0.29.68"} {
		if !strings.Contains(out.String(), "Deployment steps for aveloxis "+v) {
			t.Errorf("with no ack, the stamp's own version %s must be included:\n%s", v, out.String())
		}
	}
	if strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.66") {
		t.Error("0.29.66 precedes the stamp (no ack) and must not be printed")
	}
}

// TestAccumulatedChecklistCollapsesIdenticalBlocks: consecutive versions
// with the same steps print once, labelled with the range.
func TestAccumulatedChecklistCollapsesIdenticalBlocks(t *testing.T) {
	blocks := collapseIdenticalChecklists(deployStepsFrom("0.29.9", true, "0.29.20"))
	if len(blocks) < 1 || len(blocks) >= 12 {
		t.Fatalf("0.29.9–0.29.20 collapsed to %d blocks; want the identical early-0.29 lists merged (fewer than the 12 versions)", len(blocks))
	}
	if !strings.Contains(blocks[0].version, "same steps") {
		t.Errorf("the first block %q must be labelled by its latest version with the range it also covers", blocks[0].version)
	}
	if !strings.Contains(blocks[0].version, "(also 0.29.9–") {
		t.Errorf("the first block %q must name the earlier versions it covers", blocks[0].version)
	}
	g := &fakeGate{hasData: true, stamp: "0.29.13"}
	var out bytes.Buffer
	if _, err := checkDeployReadiness(context.Background(), g, "0.29.68", true, os.Stdin, &out); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out.String(), "Deployment steps for aveloxis"); n >= 20 {
		t.Errorf("a no-ack fleet at stamp 0.29.13 printed %d blocks; identical consecutive lists must collapse", n)
	}
	if !strings.Contains(out.String(), "from the schema stamp (0.29.13, whose own steps are unacknowledged)") {
		t.Errorf("with no acknowledgement the header must name the stamp, not \"the last acknowledged deploy\":\n%s", out.String()[:min(400, out.Len())])
	}
}

// TestUnreadableAcksPrintThisVersionsStepsOnly — PR #218 review D1: when the
// acknowledgements cannot be read, the message says "printing this
// version's steps only", so exactly that must print: the binary's own
// block, no stamp-range fallback and no multi-release header (the stamp
// fallback printed every release since the stamp under a header that
// contradicted the message).
func TestUnreadableAcksPrintThisVersionsStepsOnly(t *testing.T) {
	g := &fakeGate{hasData: true, stamp: "0.29.66", latestAckErr: errors.New("boom")}
	var out bytes.Buffer
	printDeploySteps(context.Background(), g, &out, g.stamp, "0.29.69")
	s := out.String()
	if !strings.Contains(s, "could not read the deploy acknowledgements: boom") {
		t.Errorf("the read error must be reported:\n%s", s)
	}
	if !strings.Contains(s, "Deployment steps for aveloxis 0.29.69") {
		t.Errorf("this version's steps must print:\n%s", s)
	}
	for _, v := range []string{"0.29.66", "0.29.67", "0.29.68"} {
		if strings.Contains(s, "Deployment steps for aveloxis "+v) {
			t.Errorf("%s printed although the message says this version's steps only:\n%s", v, s)
		}
	}
	if strings.Contains(s, "releases have deploy steps") {
		t.Errorf("no multi-release header when only this version's steps print:\n%s", s)
	}
}

// TestDeployChecklistSinceSharesThePrinter — PR #218 review D2: `aveloxis
// deploy-checklist --since` printed deployStepsFrom's blocks raw (no
// collapse of identical consecutive lists, no multi-release header), and an
// unparseable --since printed "no release after X" and exited 0 — a typo
// read as "nothing to do".
func TestDeployChecklistSinceSharesThePrinter(t *testing.T) {
	var out bytes.Buffer
	if err := runDeployChecklist(&out, "0.29.0", "0.29.20"); err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if n := strings.Count(s, "Deployment steps for aveloxis"); n >= 20 {
		t.Errorf("--since 0.29.0 printed %d blocks; identical consecutive lists must collapse", n)
	}
	if !strings.Contains(s, "same steps") {
		t.Errorf("--since must label a collapsed block with the range it covers:\n%s", s[:min(600, len(s))])
	}
	if !strings.Contains(s, "releases have deploy steps since 0.29.0") {
		t.Errorf("--since must print the multi-release header naming its start:\n%s", s[:min(600, len(s))])
	}
	for _, bad := range []string{"abc", "0.29.x", "", " ", "v0.29.64", "0.29.-1"} {
		if bad == "" {
			continue // "" is "no --since": the binary's own list
		}
		out.Reset()
		err := runDeployChecklist(&out, bad, "0.29.69")
		if err == nil {
			t.Errorf("--since %q: nil error; an unparseable version must exit non-zero (printed %q)", bad, out.String())
			continue
		}
		if !strings.Contains(err.Error(), "--since") {
			t.Errorf("--since %q: error %q must name the flag", bad, err)
		}
	}
	// A valid --since with nothing after it still exits 0.
	out.Reset()
	if err := runDeployChecklist(&out, "0.29.69", "0.29.69"); err != nil || !strings.Contains(out.String(), "no release after 0.29.69") {
		t.Errorf("--since at the binary's version = %v, %q; want nil and the nothing-after line", err, out.String())
	}
}

// TestMultiReleaseHeaderRunsTheLadderOnce — PR #218 review D3: the header
// said "run each block's steps, oldest first", but nearly every block is a
// full stop → migrate → … → start ladder, and a `start all` in the middle of
// the list re-enters the deploy gate. The header says to stop, migrate and
// start once, naming the strongest migrate any block asks for (a plain
// `aveloxis migrate` beats --skip-views: only it applies changed 8Knot view
// definitions).
func TestMultiReleaseHeaderRunsTheLadderOnce(t *testing.T) {
	for _, tc := range []struct {
		since, upTo, migrate string
	}{
		// PR #218 fix review r1 F1: 0.29.60 asks for the plain migrate, but
		// 0.29.61 in the same range lifts it (the supply-chain views left
		// the 8Knot batch) — its own block says --skip-views is enough.
		{"0.29.59", "0.29.61", "`aveloxis migrate --skip-views`"},
		{"0.29.58", "0.29.69", "`aveloxis migrate --skip-views`"},
		// 0.29.57's plain migrate (explorer_libyear_summary's changed
		// definition, an 8Knot view) is lifted by nothing later.
		{"0.29.56", "0.29.69", "`aveloxis migrate`"},
		{"0.29.64", "0.29.69", "`aveloxis migrate --skip-views`"}, // none does
	} {
		var out bytes.Buffer
		if err := runDeployChecklist(&out, tc.since, tc.upTo); err != nil {
			t.Fatal(err)
		}
		s := out.String()
		header := s[:strings.Index(s, "=== Deployment steps")]
		if strings.Contains(header, "run each block's steps") {
			t.Errorf("--since %s: the header still tells the operator to run every block's ladder:\n%s", tc.since, header)
		}
		if !strings.Contains(header, "once") || !strings.Contains(header, tc.migrate+" (the migrate this range needs)") {
			t.Errorf("--since %s: the header must say to stop, migrate and start once with %s:\n%s", tc.since, tc.migrate, header)
		}
		// PR #218 fix review r1 (header nit): every block's footer says to
		// ack after its steps, and a start without a terminal refuses until
		// the ack exists — so the header's sequence acks BEFORE the start.
		if !strings.Contains(header, "then `aveloxis ack-deploy`, then `aveloxis start all`") {
			t.Errorf("--since %s: the header must end the sequence with ack-deploy before start all:\n%s", tc.since, header)
		}
		// PR #218 fix review r2 F4: 0.29.60's printed block says "NOT
		// --skip-views", so a header naming --skip-views must say which
		// release lifted that — otherwise the operator reads two printed
		// instructions that contradict each other.
		// A range that also holds 0.29.57 names the plain migrate, which
		// carries out 0.29.60's instruction, so it gets no note.
		lifted := strings.Contains(header, "0.29.61 lifts 0.29.60's plain migrate")
		if want := tc.since == "0.29.59" || tc.since == "0.29.58"; lifted != want {
			t.Errorf("--since %s: the lift note is printed=%v; want %v:\n%s", tc.since, lifted, want, header)
		}
	}
	// The gate's path prints the same header.
	g := &fakeGate{hasData: true, stamp: "0.29.69", latestAck: "0.29.64"}
	var out bytes.Buffer
	printDeploySteps(context.Background(), g, &out, g.stamp, "0.29.69")
	if !strings.Contains(out.String(), "`aveloxis migrate --skip-views` (the migrate this range needs)") {
		t.Errorf("printDeploySteps must print the once-only header:\n%s", out.String()[:min(600, out.Len())])
	}
}

// TestStrongestMigrateReadsStepsAndSupersession — PR #218 fix review r1 F1:
// strongestMigrate matched the exact string "aveloxis migrate", unlike
// ladderMigrateStep's prefix reading, and ignored that 0.29.61 lifts
// 0.29.60's plain migrate. The lift is data (migrateSupersededBy), and it
// applies only when the lifting release is in the same range.
func TestStrongestMigrateReadsStepsAndSupersession(t *testing.T) {
	block := func(v, migrate string) versionSteps {
		return versionSteps{version: v, steps: []deployStep{{"aveloxis stop all", ""}, {migrate, ""}, {"aveloxis start all", ""}}}
	}
	// A migrate step with a flag other than --skip-views still rebuilds the
	// 8Knot batch: it is the strong one.
	if got := strongestMigrate([]versionSteps{block("9.0.1", "aveloxis migrate --skip-views"), block("9.0.2", "aveloxis migrate --config other.json")}); got != "aveloxis migrate --config other.json" {
		t.Errorf("a non-skip-views migrate step with a flag = %q; want it named", got)
	}
	if got := strongestMigrate([]versionSteps{block("9.0.1", "aveloxis migrate --skip-views")}); got != "aveloxis migrate --skip-views" {
		t.Errorf("only --skip-views blocks = %q", got)
	}
	s60, _ := deployChecklistFor("0.29.60")
	s61, _ := deployChecklistFor("0.29.61")
	if got := strongestMigrate([]versionSteps{{version: "0.29.60", steps: s60}}); got != "aveloxis migrate" {
		t.Errorf("0.29.60 alone = %q; want its plain migrate", got)
	}
	if got := strongestMigrate([]versionSteps{{version: "0.29.60", steps: s60}, {version: "0.29.61", steps: s61}}); got != "aveloxis migrate --skip-views" {
		t.Errorf("0.29.60 with 0.29.61 = %q; want 0.29.61's --skip-views (it supersedes 0.29.60's plain migrate)", got)
	}
	// Every lift names two releases that both carry a checklist, the lifted
	// one asks for a plain migrate, and the lifter comes after it.
	for lifted, by := range migrateSupersededBy {
		ls, ok1 := deployChecklistFor(lifted)
		_, ok2 := deployChecklistFor(by)
		if !ok1 || !ok2 || !db.SchemaVersionAtLeast(by, lifted) || by == lifted {
			t.Errorf("migrateSupersededBy[%s] = %s: both need a checklist and the lifter must be later", lifted, by)
			continue
		}
		if m, _ := migrateStepOf(ls); m == "aveloxis migrate --skip-views" {
			t.Errorf("migrateSupersededBy[%s]: that release does not ask for a plain migrate — the entry lifts nothing", lifted)
		}
	}
}

// TestGateNamesTheRangesMigrate — PR #218 fix review r1 F2: the gate's
// refusal, its --skip-deploy-check warning and start's abort line named the
// BINARY's migrate while the checklist below them covered a range whose
// strongest migrate differs: a stamp at 0.29.56 and a 0.29.69 binary need
// 0.29.57's plain migrate, and the refusal said --skip-views.
func TestGateNamesTheRangesMigrate(t *testing.T) {
	for _, skip := range []bool{false, true} {
		g := &fakeGate{hasData: true, stamp: "0.29.56"}
		var out bytes.Buffer
		if _, err := checkDeployReadiness(context.Background(), g, "0.29.69", skip, os.Stdin, &out); err != nil {
			t.Fatal(err)
		}
		advice := out.String()
		if i := strings.Index(advice, "releases have deploy steps"); i >= 0 {
			advice = advice[:i]
		}
		if strings.Contains(advice, "--skip-views") || !strings.Contains(advice, "(`aveloxis migrate`)") {
			t.Errorf("skip=%v: a 0.29.56 stamp under a 0.29.69 binary must be told the range's plain `aveloxis migrate`:\n%s", skip, advice)
		}
	}
	// PR #218 fix review r2 F5: the plain migrate belongs to the range, not
	// to 0.29.69's own checklist (whose step 2 is --skip-views), so the
	// refusal must not label it "step 2 of the deploy steps for 0.29.69".
	for _, skip := range []bool{false, true} {
		var out bytes.Buffer
		if _, err := checkDeployReadiness(context.Background(), &fakeGate{hasData: true, stamp: "0.29.56"}, "0.29.69", skip, os.Stdin, &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "deploy steps for 0.29.69 (`aveloxis migrate`)") || !strings.Contains(out.String(), "the migrate of the deploy steps from the schema stamp (0.29.56") {
			t.Errorf("skip=%v: the refusal must label the range's migrate as the range's:\n%s", skip, out.String())
		}
	}
	// PR #218 fix review r3 F1: the label is the range deployRange
	// computed. An ack BEHIND the stamp starts the range after the ack, so
	// the plain migrate comes from 0.29.57, before the stamp: "from the
	// stamp (0.29.58)" named a range that asks for no plain migrate.
	for _, skip := range []bool{false, true} {
		var out bytes.Buffer
		if _, err := checkDeployReadiness(context.Background(), &fakeGate{hasData: true, stamp: "0.29.58", latestAck: "0.29.56"}, "0.29.69", skip, os.Stdin, &out); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(out.String(), "from 0.29.58") || !strings.Contains(out.String(), "the migrate of the deploy steps since the last acknowledged deploy (0.29.56) (`aveloxis migrate`)") {
			t.Errorf("skip=%v: the label must name the range after the ack at 0.29.56:\n%s", skip, out.String())
		}
	}
	// With an ack at 0.29.64 the range is 0.29.65–0.29.69: --skip-views.
	g := &fakeGate{hasData: true, stamp: "0.29.64", latestAck: "0.29.64"}
	var out bytes.Buffer
	if _, err := checkDeployReadiness(context.Background(), g, "0.29.69", false, os.Stdin, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "(`aveloxis migrate --skip-views`)") {
		t.Errorf("an ack at 0.29.64 leaves only --skip-views blocks:\n%s", out.String())
	}
	// The abort line names the migrate it is given, not the binary's.
	if msg := startAbortMessage("0.29.69", "aveloxis migrate"); strings.Contains(msg, "--skip-views") || !strings.Contains(msg, "`aveloxis migrate`") {
		t.Errorf("start's abort line must name the range's migrate:\n%s", msg)
	}
	if got := gateMigrateStep(&fakeGate{hasData: true, stamp: "0.29.56"}, "0.29.56", "0.29.69"); got != "aveloxis migrate" {
		t.Errorf("the range's migrate (stamp 0.29.56, binary 0.29.69) = %q; want the plain migrate", got)
	}
	// An unreadable ack prints the binary's own steps, so it names the
	// binary's migrate.
	if got := gateMigrateStep(&fakeGate{hasData: true, stamp: "0.29.56", latestAckErr: errors.New("boom")}, "0.29.56", "0.29.69"); got != ladderMigrateStep("0.29.69") {
		t.Errorf("the range's migrate with an unreadable ack = %q; want the binary's own", got)
	}
}

// gateMigrateStep is the migrate the gate names for a stamp and binary.
func gateMigrateStep(g deployGate, stamp, version string) string {
	list, _, _ := deployRange(context.Background(), g, stamp, version)
	return rangeMigrateStep(list, version)
}

// TestDeployChecklistInclusive — PR #218 fix review r1 F4: --since is
// exclusive, so a fleet that never acknowledged a deploy (FROM = the schema
// stamp) never saw the stamp release's own block, which the start gate
// includes. --inclusive prints it.
func TestDeployChecklistInclusive(t *testing.T) {
	var out bytes.Buffer
	if err := runDeployChecklistRange(&out, "0.29.67", true, "0.29.69"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.67 ===") {
		t.Errorf("--inclusive must print the --since release's own block:\n%s", out.String()[:min(600, out.Len())])
	}
	out.Reset()
	if err := runDeployChecklistRange(&out, "0.29.67", false, "0.29.69"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.67 ===") {
		t.Errorf("without --inclusive the --since release's block must not print:\n%s", out.String()[:min(600, out.Len())])
	}
}

// TestAckAheadOfTheStampDoesNotHideTheStampsRange — PR #218 fix review r2
// F1: ack-deploy never checks the stamp, so an acknowledgement can run
// ahead of it (an ack after a migrate that failed closed, or the v0.29.4
// second-host path). A stamp behind the ack is the evidence: the range
// starts AT the stamp, not after the ack, so a 0.29.56 stamp with a 0.29.65
// ack under a 0.29.69 binary is still told 0.29.57's plain migrate.
func TestAckAheadOfTheStampDoesNotHideTheStampsRange(t *testing.T) {
	g := &fakeGate{hasData: true, stamp: "0.29.56", latestAck: "0.29.65"}
	if got := gateMigrateStep(g, "0.29.56", "0.29.69"); got != "aveloxis migrate" {
		t.Errorf("stamp 0.29.56 with an ack at 0.29.65 names %q; want the range's plain `aveloxis migrate`", got)
	}
	var out bytes.Buffer
	if _, err := checkDeployReadiness(context.Background(), g, "0.29.69", true, os.Stdin, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.57") {
		t.Errorf("the --skip-deploy-check path must print 0.29.57's block, which the ack ahead of the stamp hid:\n%s", out.String())
	}
	// An ack AT the stamp is consistent evidence: the range starts after it.
	if got := gateMigrateStep(&fakeGate{hasData: true, stamp: "0.29.64", latestAck: "0.29.64"}, "0.29.64", "0.29.69"); got != "aveloxis migrate --skip-views" {
		t.Errorf("an ack at the stamp names %q; want --skip-views", got)
	}
}

// TestPendingChecklistIsTheGatesRange — PR #218 fix review r3 F2: rounds 1–3
// each found the upgrade page's shell script computing a different range
// from the start gate's (the stamp fallback, the empty-stamp --inclusive,
// an ack ahead of the stamp). `deploy-checklist --pending` prints the range
// deployRange computes — the gate's one computation (SR-17) — and names the
// migrate the gate's refusal names, for every ack/stamp shape.
func TestPendingChecklistIsTheGatesRange(t *testing.T) {
	for _, g := range []*fakeGate{
		{hasData: true, stamp: "0.29.56", latestAck: "0.29.65"}, // ack ahead of the stamp
		{hasData: true, stamp: "0.29.58", latestAck: "0.29.56"}, // ack behind the stamp
		{hasData: true, stamp: "0.29.56"},                       // never acknowledged
		{hasData: true, stamp: "0.29.64", latestAck: "0.29.64"},
		{hasData: true}, // neither
		{hasData: true, stamp: "0.29.56", latestAckErr: errors.New("boom")},
		{hasData: true, stampErr: errors.New("boom"), latestAck: "0.29.60"},
	} {
		var out bytes.Buffer
		if err := runDeployChecklistPending(context.Background(), g, &out, "0.29.69"); err != nil {
			t.Fatal(err)
		}
		list, _, _ := deployRange(context.Background(), g, g.stamp, "0.29.69")
		want := rangeMigrateStep(list, "0.29.69")
		if !strings.Contains(out.String(), "The migrate to run: `"+want+"`") {
			t.Errorf("stamp %q ack %q: --pending must name the gate's migrate %q:\n%s", g.stamp, g.latestAck, want, out.String())
		}
		for _, vs := range list {
			if !strings.Contains(out.String(), "Deployment steps for aveloxis "+vs.version) {
				t.Errorf("stamp %q ack %q: --pending omits %s from the gate's range", g.stamp, g.latestAck, vs.version)
			}
		}
		if g.stampErr != nil && !strings.Contains(out.String(), "could not read the schema stamp") {
			t.Errorf("an unreadable stamp must be said, not folded into \"no stamp\" (SR-5):\n%s", out.String())
		}
	}
}

// TestSchemaBehindAdviceNamesThePendingRange — PR #218 fix review r3 (L11
// sweep of F2): every schema-behind message (db.DeployStepsAdvice) sent the
// operator to `aveloxis deploy-checklist`, the binary's own steps, so a
// fleet that skipped 0.29.57 was never shown its plain migrate. The advice
// names `--pending`, and the command registers that flag.
func TestSchemaBehindAdviceNamesThePendingRange(t *testing.T) {
	if !strings.Contains(db.DeployStepsAdvice, "aveloxis deploy-checklist --pending") {
		t.Errorf("DeployStepsAdvice must name the pending range, not the binary's own steps: %q", db.DeployStepsAdvice)
	}
	if deployChecklistCmd(new(string)).Flags().Lookup("pending") == nil {
		t.Error("deploy-checklist must register --pending, which DeployStepsAdvice names")
	}
}

// TestPendingSaysNothingWhenTheGateWouldPass — PR #218 fix review r4 F1:
// on a fully deployed database (stamp current, this binary acknowledged)
// deployRange falls back to the binary's own block, and --pending printed
// it — a full stop/migrate ladder — as "still needed", while the gate
// proceeds silently. --pending applies the gate's decision first; an ack
// read failure is said and the range prints (SR-5: an error is not "no").
func TestPendingSaysNothingWhenTheGateWouldPass(t *testing.T) {
	var out bytes.Buffer
	if err := runDeployChecklistPending(context.Background(), &fakeGate{hasData: true, stamp: "0.29.69", latestAck: "0.29.69", acked: true}, &out, "0.29.69"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Deployment steps for") || strings.Contains(out.String(), "The migrate to run") || !strings.Contains(out.String(), "nothing pending") {
		t.Errorf("a deployed, acknowledged database has nothing pending:\n%s", out.String())
	}
	out.Reset()
	if err := runDeployChecklistPending(context.Background(), &fakeGate{hasData: true, stamp: "0.29.69", latestAck: "0.29.64", ackedErr: errors.New("boom")}, &out, "0.29.69"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "could not read whether") || !strings.Contains(out.String(), "The migrate to run") {
		t.Errorf("an unreadable acknowledgement must be said and the range printed:\n%s", out.String())
	}
	out.Reset()
	if err := runDeployChecklistPending(context.Background(), &fakeGate{hasData: true, stamp: "0.29.69", latestAck: "0.29.64"}, &out, "0.29.69"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.65") {
		t.Errorf("a current stamp with the binary unacknowledged still has the range pending:\n%s", out.String())
	}
}

// TestAckAheadRefusalReadsAsOneSentence — PR #218 fix review r4 F3: the
// ack-ahead origin carried a "; …" clause, which stranded the migrate and
// the verb when rangePhrase put it inside the refusal.
func TestAckAheadRefusalReadsAsOneSentence(t *testing.T) {
	var out bytes.Buffer
	if _, err := checkDeployReadiness(context.Background(), &fakeGate{hasData: true, stamp: "0.29.56", latestAck: "0.29.65"}, "0.29.69", false, os.Stdin, &out); err != nil {
		t.Fatal(err)
	}
	line := strings.SplitN(out.String(), "\n", 2)[0]
	if strings.Contains(line, ";") || !strings.Contains(line, "0.29.65") || !strings.Contains(line, "to this binary (`aveloxis migrate`) has not completed") {
		t.Errorf("the ack-ahead refusal must read as one sentence naming the ack:\n%s", line)
	}
}

// TestPendingAgreesWithTheGateOnEveryShape — PR #218 fix review r5 F1, the
// class decision after rounds 3–5 each found one more shape where
// --pending and the gate disagreed: "nothing pending" requires POSITIVE
// evidence (a readable, current stamp and an acknowledged binary, or one
// with no checklist). Every other shape prints the range, and where the
// gate itself would pass silently it says first why it prints anyway (the
// safer direction: an unstamped or unreadable database has not been shown
// to have completed a migrate). Driven over every stamp × ack × acked ×
// checklist shape, against checkDeployReadiness itself.
func TestPendingAgreesWithTheGateOnEveryShape(t *testing.T) {
	// noList must PARSE as a version (PR #218 fix review r6 F1: a
	// "0.0.0-no-checklist" fixture never did, so every stamp shape in that
	// half took the stamp refusal and the no-checklist branch was
	// unreachable — deleting it left the suite green).
	const withList, noList = "0.29.69", "0.29.999"
	if _, ok := deployChecklistFor(noList); ok || !db.SchemaVersionAtLeast(noList, noList) {
		t.Fatalf("fixture %q must parse as a version and have no checklist", noList)
	}
	boom := errors.New("boom")
	reasons := []string{"could not read", "carries no schema stamp", "is not a version", "no collected data yet"}
	shapes, noListSilent := 0, 0
	for _, hasData := range []bool{true, false} {
		for _, version := range []string{withList, noList} {
			for _, st := range []struct {
				stamp string
				err   error
			}{{version, nil}, {"0.29.56", nil}, {"9.0.0", nil}, {"", nil}, {"garbage", nil}, {"", boom}} {
				for _, ak := range []struct {
					latest string
					err    error
				}{{"", nil}, {"0.29.56", nil}, {version, nil}, {"9.0.0", nil}, {"", boom}} {
					for _, acked := range []struct {
						v   bool
						err error
					}{{true, nil}, {false, nil}, {false, boom}} {
						shapes++
						g := &fakeGate{hasData: hasData, stamp: st.stamp, stampErr: st.err, latestAck: ak.latest, latestAckErr: ak.err, acked: acked.v, ackedErr: acked.err}
						var gateOut bytes.Buffer
						proceed, gateErr := checkDeployReadiness(context.Background(), g, version, false, nil, &gateOut)
						gateSilent := gateErr == nil && proceed && gateOut.Len() == 0
						var out bytes.Buffer
						if err := runDeployChecklistPending(context.Background(), g, &out, version); err != nil {
							t.Fatal(err)
						}
						s := out.String()
						nothing := strings.Contains(s, "nothing pending")
						if nothing && version == noList {
							noListSilent++
						}
						name := fmt.Sprintf("data=%v %s stamp=%q/%v ack=%q/%v acked=%v/%v", hasData, version, st.stamp, st.err, ak.latest, ak.err, acked.v, acked.err)
						if nothing && !gateSilent {
							t.Errorf("%s: --pending says nothing pending while the gate refuses or prints:\n%s", name, s)
						}
						if nothing && strings.Contains(s, "The migrate to run") {
							t.Errorf("%s: nothing pending must not name a migrate:\n%s", name, s)
						}
						if !nothing && !strings.Contains(s, "The migrate to run") {
							t.Errorf("%s: a printed range must end by naming the migrate:\n%s", name, s)
						}
						if gateSilent && !nothing && !slices.ContainsFunc(reasons, func(r string) bool { return strings.Contains(s, r) }) {
							t.Errorf("%s: the gate passes silently, so --pending must say why it prints steps:\n%s", name, s)
						}
					}
				}
			}
		}
	}
	srctest.MinCount(t, "gate shapes driven", shapes, 360)
	// Reachability of the no-checklist branch: a current or ahead stamp
	// under a binary with no checklist says nothing pending.
	srctest.MinCount(t, "no-checklist shapes reported as nothing pending", noListSilent, 1)
}

// TestPendingReasonLinesArePinned — PR #218 fix review r7 F1/F2: three of
// --pending's lines survived deletion in mutation probes: the
// collected-data read error (SR-5: folding it into "no data yet" left the
// suite green), the not-a-version stamp line, and the "no manual deploy
// steps" line. And r7 F4: the no-data line prints only when steps follow.
func TestPendingReasonLinesArePinned(t *testing.T) {
	run := func(g *fakeGate, version string) string {
		var out bytes.Buffer
		if err := runDeployChecklistPending(context.Background(), g, &out, version); err != nil {
			t.Fatal(err)
		}
		return out.String()
	}
	s := run(&fakeGate{hasDataErr: errors.New("boom"), stamp: "0.29.56"}, "0.29.69")
	if !strings.Contains(s, "could not read whether this database has collected data: boom") || strings.Contains(s, "no collected data yet") || !strings.Contains(s, "The migrate to run") {
		t.Errorf("a failed collected-data read must be said, not read as \"no data\", and the range still prints:\n%s", s)
	}
	if s := run(&fakeGate{hasData: true, stamp: "garbage"}, "0.29.69"); !strings.Contains(s, `the schema stamp "garbage" is not a version`) {
		t.Errorf("a malformed stamp must be named:\n%s", s)
	}
	if s := run(&fakeGate{hasData: true}, "0.29.999"); !strings.Contains(s, "aveloxis 0.29.999 has no manual deploy steps") {
		t.Errorf("an empty range for a binary with no checklist must say so:\n%s", s)
	}
	s = run(&fakeGate{stamp: "0.29.69", latestAck: "0.29.69", acked: true}, "0.29.69")
	if !strings.Contains(s, "nothing pending") || strings.Contains(s, "no collected data yet") {
		t.Errorf("with nothing pending, the no-data line has no steps to describe:\n%s", s)
	}
	// r8 F2: an empty range (a binary with no checklist) has no steps for
	// the no-data line to describe.
	if s := run(&fakeGate{}, "0.29.999"); strings.Contains(s, "no collected data yet") {
		t.Errorf("the no-data line must not precede an empty range:\n%s", s)
	}
	if s := run(&fakeGate{stamp: "0.29.56"}, "0.29.69"); !strings.Contains(s, "no collected data yet") {
		t.Errorf("a fresh fleet with steps printed must say the gate does not require them yet:\n%s", s)
	}
}

// ctxRecordingGate records the context the pending run reads the stamp on.
type ctxRecordingGate struct {
	*fakeGate
	got context.Context
}

func (g *ctxRecordingGate) SchemaVersion(ctx context.Context) (string, error) {
	g.got = ctx
	return g.fakeGate.SchemaVersion(ctx)
}

type openerMark struct{}

// TestDeployChecklistCommandWiring — PR #218 fix review r8 F1: the
// command's wiring was unpinned — `if !pending` → `if true`, an unbounded
// query context, and dropping the --since/--inclusive refusal each left the
// suite green, while every piece of advice now points at --pending. Driven
// through runDeployChecklistCommand with a fake opener: --pending opens the
// gate, reads it on the opener's (bounded) context and closes it; without
// it nothing is opened; mixing the flags is refused before anything opens.
func TestDeployChecklistCommandWiring(t *testing.T) {
	var opened, closed int
	g := &ctxRecordingGate{fakeGate: &fakeGate{hasData: true, stamp: "0.29.56"}}
	openCtx, cancel := context.WithTimeout(context.WithValue(context.Background(), openerMark{}, "opener"), time.Minute)
	defer cancel()
	open := func() (deployGate, context.Context, func(), error) {
		opened++
		return g, openCtx, func() { closed++ }, nil
	}
	var out bytes.Buffer
	if err := runDeployChecklistCommand(&out, "", false, true, "0.29.69", open); err != nil {
		t.Fatal(err)
	}
	if opened != 1 || closed != 1 || !strings.Contains(out.String(), "The migrate to run: `aveloxis migrate`") {
		t.Errorf("--pending must open the gate once, print its range and close it (opened %d, closed %d):\n%s", opened, closed, out.String())
	}
	if g.got == nil || g.got.Value(openerMark{}) != "opener" {
		t.Error("--pending must read on the opener's bounded context, not a fresh one")
	}
	out.Reset()
	if err := runDeployChecklistCommand(&out, "", false, false, "0.29.69", open); err != nil {
		t.Fatal(err)
	}
	if opened != 1 || !strings.Contains(out.String(), "Deployment steps for aveloxis 0.29.69") {
		t.Errorf("without --pending the binary's own list prints and nothing is opened (opened %d):\n%s", opened, out.String())
	}
	for _, tc := range []struct {
		since     string
		inclusive bool
	}{{"0.29.1", false}, {"", true}, {"0.29.1", true}} {
		if err := runDeployChecklistCommand(&out, tc.since, tc.inclusive, true, "0.29.69", open); err == nil || !strings.Contains(err.Error(), "does not take --since or --inclusive") {
			t.Errorf("--pending with since=%q inclusive=%v = %v; want the refusal", tc.since, tc.inclusive, err)
		}
	}
	if opened != 1 {
		t.Errorf("a refused flag mix must not open the database (opened %d)", opened)
	}
	// The command's RunE goes through the shared bounded opener.
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "cmd/aveloxis/deploy_checklist.go"), "func deployChecklistCmd("))
	if !strings.Contains(body, "runDeployChecklistCommand(") || !strings.Contains(body, "return openDeployGate(*cfgPath, deployGateDialTimeout)\n") {
		t.Error("deploy-checklist's RunE must run runDeployChecklistCommand with openDeployGate, the bounded opener the start gate uses")
	}
}

// TestStampRefusalSaysWhereTheStepsAre — PR #218 fix review r12 F1: the
// stamp refusal told the operator to run "the heals" without naming where
// they are listed; on a binary with no checklist nothing on screen did.
func TestStampRefusalSaysWhereTheStepsAre(t *testing.T) {
	for _, version := range []string{"0.29.69", "0.29.999"} {
		var out bytes.Buffer
		if _, err := checkDeployReadiness(context.Background(), &fakeGate{hasData: true, stamp: "0.29.60", latestAck: "0.29.60"}, version, false, nil, &out); err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(out.String(), "`aveloxis deploy-checklist --pending` lists them") {
			t.Errorf("%s: the stamp refusal must name where its steps are listed:\n%s", version, out.String())
		}
	}
}

// TestPendingMigrateLabelStandsAlone — r12 F2: "Step 3's migrate" is the
// upgrade page's numbering, printed under blocks whose own step 3 is a
// check; DeployStepsAdvice and the troubleshooting guide send operators
// here without that page.
func TestPendingMigrateLabelStandsAlone(t *testing.T) {
	var out bytes.Buffer
	if err := runDeployChecklistPending(context.Background(), &fakeGate{hasData: true, stamp: "0.29.68", latestAck: "0.29.68"}, &out, "0.29.69"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "Step 3") || !strings.Contains(out.String(), "The migrate to run: `aveloxis migrate --skip-views`") {
		t.Errorf("the migrate line must stand alone:\n%s", out.String())
	}
}
