// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
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
		t.Error("versions at or below a migrated-and-acked... no: 0.29.66 precedes the stamp and must not be printed")
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
