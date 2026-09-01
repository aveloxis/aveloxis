// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// convergence_contracts_test.go — v0.27.146: SR-19's mechanization.
//
// Anything documented as "rerun until done" / "self-draining" / "the
// <X> is the resume state" is a CONVERGENCE CONTRACT: re-running is
// supposed to skip completed work and reach a stable done state. The
// motivating incident (round 23, v0.27.141): heal-collection-gaps
// promised "rerun until 0 candidates" while healed repos never left
// the candidate set — the contract was prose, not a property.
//
// This tripwire keeps every such contract paired with a test that
// DRIVES its loop to done:
//
//  1. every non-test Go file whose comments/help text carry a
//     convergence marker phrase must be registered below with at
//     least one driving test;
//  2. each driving test must exist as a real test FuncDecl (go/parser,
//     never a source regex — the round-22 lesson: `func Test...(` in a
//     comment or raw-string fixture is not a test);
//  3. staleness both ways: a registered file that no longer carries a
//     marker (contract removed/reworded) fails until deregistered, so
//     the registry can't rot.
//
// Registering a site is NOT the work — the driving test is. A new
// resumable command ships its convergence test first, then adds the
// registry row pointing at it.

package scripts

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// convergenceContract is one registered contract site.
type convergenceContract struct {
	// File is the repo-relative (slash) path of the non-test source
	// file carrying the convergence-marker prose.
	File string
	// DrivingTests name the test(s) that drive this contract's loop TO
	// done — work happens, the marker/predicate updates, and the
	// claim/candidate query stops returning the row.
	DrivingTests []string
}

var convergenceContracts = []convergenceContract{
	{File: "cmd/aveloxis/dedup_repos.go",
		// "re-run until 0 pairs": the e2e asserts the merged pair drops
		// out of the candidate set (findPairByLowerGit → nil).
		DrivingTests: []string{"TestDedupOnePairEndToEnd"}},
	{File: "internal/db/repo_dedup.go",
		// v0.28.18: DedupCaseVariantReposBatch's doc quotes the same
		// "rerun until 0 pairs" contract and names the stall that broke
		// it (a collecting head filling every batch window). The paging
		// test drives a batch PAST a collecting pair to the mergeable one
		// beyond it, then finishes that pair's job and reruns until it
		// too has left the candidate set (scoped to the fixture's pairs —
		// the shared scratch DB may carry residue); the e2e is the
		// drop-out-of-candidate-set half.
		DrivingTests: []string{"TestDedupBatchPagesPastCollectingHead", "TestDedupOnePairEndToEnd"}},
	{File: "internal/collector/jira_worker.go",
		// Copilot round 4 on PR #193: the shutdown claim release's
		// contract — "the per-page checkpoint is the resume state". The
		// driver runs the RESUMED scan (LastUpdated = the mid-corpus
		// checkpoint a killed scan left) to completion: pre-checkpoint
		// work is skipped, the tail is collected, the scan stamps done.
		DrivingTests: []string{"TestJiraResumeFromCheckpointCompletes"}},
	{File: "internal/db/jira_store.go",
		// ReleaseJiraClaim's doc quotes the same contract; its own
		// driver proves the released claim is IMMEDIATELY reclaimable
		// (no stale-window wait) with the checkpoint untouched, and the
		// worker-side driver completes the resumed scan.
		DrivingTests: []string{"TestReleaseJiraClaimOwnershipAndReclaim", "TestJiraResumeFromCheckpointCompletes"}},
	{File: "cmd/aveloxis/rewalk_whitespace.go",
		// "the marker IS the resume state": stamping whitespace_head_hash
		// drains the repo from GetReposForWhitespaceRewalk.
		DrivingTests: []string{"TestWhitespaceRewalkClaimDrainsStampedRepos"}},
	{File: "cmd/aveloxis/heal_messages.go",
		// "re-run until 'nothing pending'": MarkMessagesHealed drains the
		// row from GetMessageHealBatch.
		DrivingTests: []string{"TestMessageHealWorklistDrainsOnStamp"}},
	{File: "internal/collector/message_heal.go",
		// v0.28.8: the cursor walk documents "the worklist IS the resume
		// state" — failed rows stay pending and a fresh run retries them
		// from the bottom. Same drain driver as the command: stamping is
		// what removes a row from GetMessageHealBatch.
		DrivingTests: []string{"TestMessageHealWorklistDrainsOnStamp"}},
	{File: "internal/db/email_message_store.go",
		// Part A (email attribution): BackfillMailingListSenderIDs'
		// doc states "cntrb_id IS NULL is the resume state" / "rerun
		// until the cursor clears the ceiling". The driver walks
		// floor→ceiling windows, proves every resolvable fixture row
		// attributes, and reruns the pass proving idempotence (scoped
		// to the fixture's rows — the shared scratch DB may carry
		// residue).
		DrivingTests: []string{"TestSenderBackfillFullPassConverges"}},
	{File: "cmd/aveloxis/strip_quoted_history.go",
		// Part B: "msg_text_clean IS NULL is the resume state" — the
		// driver proves an unstripped row appears in the strip batch,
		// stamping removes it, and --rule-rerun re-selects stale-rule
		// rows.
		DrivingTests: []string{"TestStripWalkerBatchAndResumeState"}},
	{File: "cmd/aveloxis/resolve_email_identities.go",
		// The one-shot CLI quotes the same contract ("rerun until it
		// reports 0"); same driver — the command is a thin loop over
		// the store method the driver exercises.
		DrivingTests: []string{"TestSenderBackfillFullPassConverges"}},
	{File: "cmd/aveloxis/heal_collection_gaps.go",
		// "rerun until 0 candidates" — the flagship: candidate → fill →
		// RefreshQueueGatheredCounts → candidate set empty.
		DrivingTests: []string{"TestGapHealConvergesToZeroCandidates"}},
	{File: "internal/db/gap_heal_store.go",
		// RefreshQueueGatheredCounts' doc quotes the same contract — the
		// store half of the flagship's loop.
		DrivingTests: []string{"TestGapHealConvergesToZeroCandidates"}},
}

// convergenceExemptFiles carry a marker phrase WITHOUT being a
// contract site, each with the reason. Keep this list short and
// justified — an exemption is a reviewed decision, not an escape.
var convergenceExemptFiles = map[string]string{
	// SR-19's own Statement quotes "rerun until done" — the rule about
	// contracts, not a contract.
	"scripts/standing_rules.go": "the SR-19 rule statement itself",
}

// convergenceMarkerRe is the phrase set that marks a convergence
// contract in prose. Deliberately broad (case-insensitive, hyphen and
// -ing variants) — a false positive costs one registry or exemption
// row; a false negative is an untested contract.
var convergenceMarkerRe = regexp.MustCompile(`(?i)re-?run(?:ning)? until|self-draining|is the resume state`)

// Enforces SR-19 (scripts/standing_rules.go).
func TestConvergenceContractsHaveDrivingTests(t *testing.T) {
	root := srctest.Root(t)

	// Sweep: marker-bearing non-test sources + real test FuncDecls.
	markerFiles := map[string]bool{}
	testFuncs := map[string]bool{}
	scannedSrc, scannedTests := 0, 0
	for _, top := range []string{"cmd", "internal", "scripts"} {
		err := filepath.WalkDir(filepath.Join(root, top), func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			rel, _ := filepath.Rel(root, path)
			relSlash := filepath.ToSlash(rel)
			if strings.HasSuffix(path, "_test.go") {
				scannedTests++
				af, perr := parser.ParseFile(token.NewFileSet(), path, b, parser.SkipObjectResolution)
				if perr != nil {
					return fmt.Errorf("parse %s: %w", path, perr)
				}
				for _, decl := range af.Decls {
					if fd, ok := decl.(*ast.FuncDecl); ok && fd.Recv == nil && strings.HasPrefix(fd.Name.Name, "Test") {
						testFuncs[fd.Name.Name] = true
					}
				}
				return nil
			}
			scannedSrc++
			if convergenceMarkerRe.Match(b) {
				markerFiles[relSlash] = true
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	srctest.MinCount(t, "non-test Go files scanned for convergence markers", scannedSrc, 200)
	srctest.MinCount(t, "test files scanned for driving-test FuncDecls", scannedTests, 300)
	srctest.MinCount(t, "registered convergence contracts", len(convergenceContracts), 1)

	registered := map[string]bool{}
	for _, c := range convergenceContracts {
		if registered[c.File] {
			t.Errorf("%s: registered twice — merge the rows", c.File)
		}
		registered[c.File] = true
		// (3) registry staleness: the file must exist AND still carry
		// a marker; otherwise the row is rot.
		if !markerFiles[c.File] {
			t.Errorf("%s: registered as a convergence contract but carries no marker phrase (file deleted, or contract reworded/removed?) — deregister it or restore the contract prose", c.File)
		}
		// (2) driving tests are real.
		if len(c.DrivingTests) == 0 {
			t.Errorf("%s: a registered contract MUST name at least one driving test — that test is the point of the registry", c.File)
		}
		for _, fn := range c.DrivingTests {
			if !testFuncs[fn] {
				t.Errorf("%s: driving test %q does not exist as a test FuncDecl — the convergence contract's enforcement silently died (renamed test? update the registry)", c.File, fn)
			}
		}
	}
	// Exemption staleness — an exempt file that stopped carrying the
	// phrase no longer needs the exemption.
	for f, reason := range convergenceExemptFiles {
		if registered[f] {
			t.Errorf("%s: both registered and exempt — pick one", f)
		}
		if !markerFiles[f] {
			t.Errorf("%s: exempt (%s) but carries no marker phrase anymore — remove the stale exemption", f, reason)
		}
	}
	// (1) coverage: every marker-bearing file is accounted for.
	for f := range markerFiles {
		if registered[f] || convergenceExemptFiles[f] != "" {
			continue
		}
		t.Errorf("%s: carries a convergence-contract phrase (%s) but is neither registered in convergenceContracts nor exempt — write the test that drives its loop to done, then register it (SR-19)",
			f, convergenceMarkerRe.String())
	}
}
