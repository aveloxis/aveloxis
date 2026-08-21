// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Package scripts carries repo-wide tripwire tests and, since
// v0.27.127 (regression-infrastructure Phase 4), the STANDING-RULES
// REGISTRY: the machine-readable table of the project's cross-cutting
// engineering rules, each linked to the tests that enforce it.
//
// Why it exists: PR #184's review rounds repeatedly found FIXES that
// violated the repo's own standing rules (the migration-only-index
// rule was violated in v0.27.115, five rounds after round 10
// re-affirmed it; the lookup-ERROR-is-not-"no" class recurred inside
// its own round-10 detector). Rules that live only as prose get
// re-violated; rules with an ID, a one-line statement, and named
// enforcing tests are checkable — by the meta-test in
// standing_rules_test.go, by reviewers, and by the review bot reading
// test-failure messages.
//
// Conventions:
//   - IDs are never reused. A retired rule keeps its ID forever with
//     Retired: true.
//   - EnforcedBy names must exist as test functions somewhere in the
//     tree (the meta-test verifies). ProcessOnly rules have no
//     mechanical enforcement YET — they are review-time discipline
//     and analyzer candidates (see testing.md's graduation criteria).
//   - The full prose, incident history, and operational context stay
//     PRIVATE in CLAUDE.md (operator decision); every active SR-ID
//     must appear there (the meta-test soft-skips when CLAUDE.md is
//     absent, e.g. in a public checkout).
package scripts

// Rule is one standing engineering rule.
type Rule struct {
	ID          string
	Statement   string
	EnforcedBy  []string
	ProcessOnly bool
	Retired     bool
}

var standingRules = []Rule{
	{ID: "SR-1",
		Statement:  "A unique index whose target data may hold duplicates is created by a migration AFTER dedup drains them — never declared in schema.sql.",
		EnforcedBy: []string{"TestMsgRefMigrationShape"}},
	{ID: "SR-2",
		Statement:  "A newly-introduced index on a fleet-scale table is migration-owned via CONCURRENTLY (fresh installs included); schema.sql declarations are only for shapes every fleet already has.",
		EnforcedBy: []string{"TestPerfWaveIndexesAreMigrationOnly", "TestPlatformRepoIDIndexIsMigrationOnly", "TestHistoryIndexIsMigrationOnlyAndDropIsConcurrent"}},
	{ID: "SR-3",
		Statement:  "A progress/resume marker is stamped only over rows PROVEN written — never over a walk whose writes may have been dropped or whose rows don't exist yet.",
		EnforcedBy: []string{"TestWhitespaceGateIsOperative", "TestWhitespacePhaseValidatesMarkerUpFront", "TestRewalkClaimExcludesNeverCollectedAndRechecks"}},
	{ID: "SR-4",
		Statement:  "A dropped index/object name is never re-created — migrations that DROP by name run forever, so reuse rebuilds on every migrate.",
		EnforcedBy: []string{"TestEmailLookupIndexNamesNotDropTargets", "TestRepoLaborHistoryIndexConvergence"}},
	{ID: "SR-5",
		Statement:  "A lookup ERROR is never treated as 'not found' / 'no' — only the typed not-found sentinel is; every other error propagates.",
		EnforcedBy: []string{"TestForgeIDProbeErrorIsNotSuccess", "TestRenameHealPropagatesLookupErrors"}},
	{ID: "SR-6",
		Statement:  "Identity attributions derive from platform numeric IDs or provably unambiguous matches; ambiguous or cross-platform candidates stay NULL — never fabricate.",
		EnforcedBy: []string{"TestLoginSweepsRejectAmbiguousOwnerLogins", "TestMetaOwnerSweepIsGitHubOnly"}},
	{ID: "SR-7",
		Statement:   "Watchdogs, detectors, and circuit breakers OBSERVE and log; they never auto-kill, auto-requeue, or auto-mutate without explicit operator approval.",
		ProcessOnly: true},
	{ID: "SR-8",
		Statement:  "A schema.sql index referencing a column that is NEW in the same release needs an ALTER TABLE ADD COLUMN IF NOT EXISTS guard ahead of it (existing fleets no-op the CREATE TABLE).",
		EnforcedBy: []string{"TestSchemaGuardsNewIndexedColumnsForExistingFleets"}},
	{ID: "SR-9",
		Statement:  "Integration tests close DB pools via t.Cleanup, never defer — defers run BEFORE t.Cleanup data cleanups, which then execute against a closed pool and strand fixtures.",
		EnforcedBy: []string{"TestNoDeferPoolCloseInTests"}},
	{ID: "SR-10",
		Statement:   "Config knobs get END-TO-END tests (JSON value → observable behavior); every default/clamp lives at exactly one layer, and diagnostics log the EFFECTIVE value at the point of use.",
		ProcessOnly: true},
	{ID: "SR-11",
		Statement:  "Every protected column has ONE registered write policy; every UPDATE SET assignment conforms, or takes a reviewed Exception with a reason.",
		EnforcedBy: []string{"TestColumnWritePolicies"}},
	{ID: "SR-12",
		Statement:  "Source-contract helpers live in internal/srctest (+sqlscan); defining a new duplicate fails the ratchet, and migrated legacy sites only shrink the baseline.",
		EnforcedBy: []string{"TestSrctestMigrationRatchet"}},
	{ID: "SR-13",
		Statement:  "Every column of an audited entity table has a writer in a WRITER POSITION or a documentedEmpty entry with a reason.",
		EnforcedBy: []string{"TestEveryColumnHasWriterOrDocumentedEmpty"}},
	{ID: "SR-14",
		Statement:  "Every ON CONFLICT clause has a REAL arbiter — a bare DO NOTHING on a table with no unique silently duplicates; a named arbiter without a matching unique is a runtime 42P10.",
		EnforcedBy: []string{"TestOnConflictClausesHaveRealArbiters"}},
	{ID: "SR-15",
		Statement:  "Go sources carry ASCII quotes only — typographic quotes next to SQL predicates read as real syntax.",
		EnforcedBy: []string{"TestNoCurlyQuotesInGoSources"}},
}
