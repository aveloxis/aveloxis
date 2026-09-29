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

	"github.com/aveloxis/aveloxis/internal/db"
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
		if !strings.Contains(header, "once") || !strings.Contains(header, tc.migrate+" (the strongest migrate any block asks for)") {
			t.Errorf("--since %s: the header must say to stop, migrate and start once with %s:\n%s", tc.since, tc.migrate, header)
		}
		// PR #218 fix review r1 (header nit): every block's footer says to
		// ack after its steps, and a start without a terminal refuses until
		// the ack exists — so the header's sequence acks BEFORE the start.
		if !strings.Contains(header, "then `aveloxis ack-deploy`, then `aveloxis start all`") {
			t.Errorf("--since %s: the header must end the sequence with ack-deploy before start all:\n%s", tc.since, header)
		}
	}
	// The gate's path prints the same header.
	g := &fakeGate{hasData: true, stamp: "0.29.69", latestAck: "0.29.64"}
	var out bytes.Buffer
	printDeploySteps(context.Background(), g, &out, g.stamp, "0.29.69")
	if !strings.Contains(out.String(), "`aveloxis migrate --skip-views` (the strongest migrate any block asks for)") {
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
