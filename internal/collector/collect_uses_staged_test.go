// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// v0.26.2 source-contract tests: the one-shot `aveloxis collect` path
// (legacy Collector) must delegate its API phases to the SAME staged
// pipeline production `aveloxis serve` uses (StagedCollector +
// Processor).
//
// Why this is load-bearing — the 2026-07-09 data-test incident: the
// legacy direct-write collectEvents passed events straight from
// ListPREvents/ListIssueEvents to UpsertPREvent/UpsertIssueEvent. The
// platform clients populate only PlatformPRID/PlatformIssueID (the
// number); the local-serial PRID/IssueID field stays 0. Postgres
// rejected EVERY event with FK 23503 (pull_request_id=0 never exists),
// the WARN-and-continue loop dropped them all, and the one-shot collect
// path had NEVER stored a single event. The loss was invisible to
// data-test because it was symmetric across both binaries. Only the
// staged Processor resolves number → serial correctly (production has
// ~25M pull_request_events rows via serve).
//
// The fix consolidates the paths, which also means `aveloxis collect`
// (and therefore data-test) finally honors pr_child_mode / listing_mode
// / issue_child_mode — pre-v0.26.2 it was REST-only and could not
// validate the v0.26.0 GraphQL default flip at all.

import (
	"os"
	"strings"
	"testing"
)

func readCollectorGo(t *testing.T) string {
	t.Helper()
	src, err := os.ReadFile("collector.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(src)
}

// TestCollectRepoDelegatesToStagedPipeline pins that CollectRepo routes
// API collection through collectAndProcess, and that collectAndProcess
// mirrors scheduler.collectAndProcess: StagedCollector (all modes) →
// Processor, with processing gated on a nil collect error (same
// semantics as serve — a hard collect error means incomplete staging).
func TestCollectRepoDelegatesToStagedPipeline(t *testing.T) {
	code := readCollectorGo(t)

	idx := strings.Index(code, "func (c *Collector) CollectRepo(")
	if idx < 0 {
		t.Fatal("cannot find CollectRepo in collector.go")
	}
	body := code[idx:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	if !strings.Contains(body, "c.collectAndProcess(") {
		t.Error("CollectRepo must call c.collectAndProcess — the staged-pipeline " +
			"delegation that replaced the legacy direct-write API phases (v0.26.2)")
	}

	idx = strings.Index(code, "func (c *Collector) collectAndProcess(")
	if idx < 0 {
		t.Fatal("cannot find collectAndProcess in collector.go — the v0.26.2 delegation helper")
	}
	helper := code[idx:]
	if end := strings.Index(helper[1:], "\nfunc "); end > 0 {
		helper = helper[:end+1]
	}
	for _, needle := range []string{
		"NewStagedCollectorWithAllModes(",
		"WithIssueChildMode(",
		"NewProcessor(",
		"ProcessRepo(",
	} {
		if !strings.Contains(helper, needle) {
			t.Errorf("collectAndProcess must contain %q — it must mirror "+
				"scheduler.collectAndProcess so collect and serve cannot diverge", needle)
		}
	}
	// Same gating as the scheduler: process only when staging finished
	// without a hard error.
	if !strings.Contains(helper, "if err == nil") {
		t.Error("collectAndProcess must gate ProcessRepo on `if err == nil` — " +
			"mirroring scheduler.collectAndProcess exactly")
	}
}

// TestLegacyDirectWritePhasesRemoved is the negative pin: the eight
// legacy direct-write phase methods must be GONE from collector.go. If
// any of them returns, the PRID=0 → FK 23503 event-loss class returns
// with it.
func TestLegacyDirectWritePhasesRemoved(t *testing.T) {
	code := readCollectorGo(t)
	for _, fn := range []string{
		"func (c *Collector) collectIssues(",
		"func (c *Collector) collectPullRequests(",
		"func (c *Collector) collectEvents(",
		"func (c *Collector) collectMessages(",
		"func (c *Collector) collectRepoInfo(",
		"func (c *Collector) collectReleases(",
		"func (c *Collector) collectContributors(",
		"func (c *Collector) collectCloneStats(",
	} {
		if strings.Contains(code, fn) {
			t.Errorf("legacy direct-write phase %q must not exist in collector.go — "+
				"the API phases delegate to the staged pipeline (v0.26.2; the legacy "+
				"event path dropped EVERY event via FK 23503 since inception)", fn)
		}
	}
}

// TestCollectorThreadsCollectionModes pins that the Collector carries
// the staged-pipeline mode knobs and that runCollect (main.go) threads
// them from the config — using the same CollectionConfig fields the
// scheduler reads, so the one-shot path and serve see identical modes.
func TestCollectorThreadsCollectionModes(t *testing.T) {
	code := readCollectorGo(t)
	for _, needle := range []string{
		"prChildMode",
		"listingMode",
		"threadingMode",
		"shardSize",
		"issueChildMode",
		"func (c *Collector) WithCollectionModes(",
	} {
		if !strings.Contains(code, needle) {
			t.Errorf("collector.go must contain %q — the mode plumbing that lets "+
				"`aveloxis collect` honor pr_child_mode/listing_mode/issue_child_mode", needle)
		}
	}

	mainSrc, err := os.ReadFile("../../cmd/aveloxis/main.go")
	if err != nil {
		t.Fatal(err)
	}
	m := string(mainSrc)
	if !strings.Contains(m, "WithCollectionModes(") {
		t.Error("main.go runCollect must chain WithCollectionModes(...) so the " +
			"one-shot collect honors the configured collection modes")
	}
	for _, needle := range []string{
		"cfg.Collection.PRChildMode",
		"cfg.Collection.ListingMode",
		"cfg.Collection.ThreadingMode",
		"cfg.Collection.ShardSize",
		"cfg.Collection.IssueChildMode",
	} {
		if !strings.Contains(m, needle) {
			t.Errorf("main.go must pass %s into the collector — same source of truth "+
				"the scheduler reads", needle)
		}
	}
}
