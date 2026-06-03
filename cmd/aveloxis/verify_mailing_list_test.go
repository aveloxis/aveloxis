// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

func TestVerifyMailingListCmdRegistered(t *testing.T) {
	data, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "verifyMailingListCmd(") {
		t.Error("main.go must register verifyMailingListCmd")
	}
}

func TestVerifyMailingListCmdShape(t *testing.T) {
	data, err := os.ReadFile("verify_mailing_list.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	for _, needle := range []string{`"verify-mailing-list"`, `"strict"`, "MailingListCoverage(", "reportMailingListCoverage("} {
		if !strings.Contains(src, needle) {
			t.Errorf("verify-mailing-list must contain %q", needle)
		}
	}
}

// fullCoverage returns a coverage rollup where every required branch fired.
func fullCoverage() db.MailingListCoverage {
	var cov db.MailingListCoverage
	cov.Lists = 6
	cov.EmailMessages = 1000
	cov.Mirrors = 120
	cov.SignaledCaptured = 90
	cov.SignaledResolved = 80
	cov.SenderTotal = 1000
	cov.SenderResolved = 850
	cov.ByClass = map[string]int64{
		"issue_event": 200, "patch_submission": 150, "review": 300,
		"github_mirror": 120, "commit_notify": 30, "vote": 40, "discuss": 60,
	}
	cov.BySystem = map[string]int64{"apache_ponymail": 700, "lore_public_inbox": 300}
	cov.BridgedToIssue = 200
	cov.BridgedToPR = 150
	cov.MirrorLinked = 90
	cov.ThreadRooted = 800
	cov.ExternalKeyIssues = 120
	return cov
}

func TestReportCoverageStrictPassesWhenAllRequiredFired(t *testing.T) {
	var buf bytes.Buffer
	if err := reportMailingListCoverage(&buf, fullCoverage(), true); err != nil {
		t.Fatalf("strict should pass on full coverage, got: %v", err)
	}
	if !strings.Contains(buf.String(), "PASS") {
		t.Error("report should mark fired branches PASS")
	}
}

func TestReportCoverageStrictFailsOnEmptyRequiredBranch(t *testing.T) {
	cov := fullCoverage()
	delete(cov.ByClass, "issue_event") // a mailing-list-native REQUIRED branch goes empty

	var buf bytes.Buffer
	err := reportMailingListCoverage(&buf, cov, true)
	if err == nil {
		t.Fatal("strict must return error when a required branch is empty")
	}
	if !strings.Contains(buf.String(), "EMPTY*") {
		t.Error("report must flag the empty required branch with EMPTY*")
	}
}

// Cross-subsystem branches (bridged-to-issue, sender-resolved, external_key)
// depend on GitHub data + collection ordering / backfills. Their emptiness
// must NOT fail --strict (Phase 4 run #1 finding) — they report as DEFER.
func TestReportCoverageStrictPassesWhenOnlyDeferredEmpty(t *testing.T) {
	cov := fullCoverage()
	cov.BridgedToIssue = 0
	cov.BridgedToPR = 0
	cov.MirrorLinked = 0
	cov.SenderResolved = 0
	cov.ExternalKeyIssues = 0

	var buf bytes.Buffer
	if err := reportMailingListCoverage(&buf, cov, true); err != nil {
		t.Fatalf("strict must pass when only cross-subsystem deferred branches are empty, got: %v", err)
	}
	if !strings.Contains(buf.String(), "DEFER") {
		t.Error("empty cross-subsystem branches must render as DEFER")
	}
}

func TestReportCoverageReportOnlyNeverErrors(t *testing.T) {
	// Empty DB: every count zero. Report-only (strict=false) must not error.
	var cov db.MailingListCoverage
	cov.ByClass = map[string]int64{}
	cov.BySystem = map[string]int64{}
	var buf bytes.Buffer
	if err := reportMailingListCoverage(&buf, cov, false); err != nil {
		t.Fatalf("report-only must never error, got: %v", err)
	}
	if !strings.Contains(buf.String(), "EMPTY*") {
		t.Error("report-only on an empty DB should still flag required branches as EMPTY*")
	}
}

// The lore/public-inbox backend is optional (Anubis-gated enumeration), so
// its absence must NOT fail --strict.
func TestReportCoverageLoreBackendOptional(t *testing.T) {
	cov := fullCoverage()
	delete(cov.BySystem, "lore_public_inbox")
	var buf bytes.Buffer
	if err := reportMailingListCoverage(&buf, cov, true); err != nil {
		t.Fatalf("missing lore backend must not fail strict, got: %v", err)
	}
}
