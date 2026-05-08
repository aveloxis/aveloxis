package db

import (
	"os"
	"strings"
	"testing"
)

// docPath is the canonical location of the contributor resolution
// architecture document. The Phase A refactor commits to keeping this
// document in sync with the code; these tests fail loudly if the doc
// drifts out of date.
const docPath = "../../docs/architecture/contributor-resolution.md"

// TestContributorResolutionDocExists pins that the architecture doc
// for contributor resolution exists at the expected location. Phase A
// of the v0.19.x contributor refactor (see summary/04-refactoring-plan.md)
// commits to publishing the contract publicly so operators can validate
// data quality against a written spec.
func TestContributorResolutionDocExists(t *testing.T) {
	if _, err := os.Stat(docPath); os.IsNotExist(err) {
		t.Fatalf("docs/architecture/contributor-resolution.md must exist (Phase A deliverable). " +
			"This document is the public contract for contributor resolution and is " +
			"referenced from docs/index.md's architecture toctree.")
	}
}

// TestContributorResolutionDocReferencesCanonicalFunctions pins that the
// doc references the three canonical Go functions that implement the
// contract: ContributorResolver.Resolve, UpsertContributorBatch,
// LinkContributorToGitHubUser. If any of these is renamed or removed
// without updating the doc, this test fails — the doc and code stay
// in lockstep.
func TestContributorResolutionDocReferencesCanonicalFunctions(t *testing.T) {
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", docPath, err)
	}
	doc := string(data)

	// Required function references. If any of these is renamed in code
	// but not in the doc (or vice versa), the test fails.
	for _, name := range []string{
		"ContributorResolver.Resolve",
		"UpsertContributorBatch",
		"LinkContributorToGitHubUser",
	} {
		if !strings.Contains(doc, name) {
			t.Errorf("%s must reference %q so a code rename triggers a doc-update "+
				"signal. Currently missing.", docPath, name)
		}
	}
}

// TestContributorResolutionDocCoversAllRules pins that every rule from
// summary/02-contract.md (R1 through R13) appears in the published doc.
// Without this, a future edit could silently drop a rule from the
// public contract while leaving the source of truth in summary/.
func TestContributorResolutionDocCoversAllRules(t *testing.T) {
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", docPath, err)
	}
	doc := string(data)

	// The doc should reference each rule by ID. The exact phrasing of
	// the heading isn't pinned (operators may want to change the title
	// "R1: Identity-key determinism" → "R1 — Identity Determinism") but
	// the bare rule ID must appear.
	for _, rule := range []string{
		"R1", "R2", "R3", "R4", "R5", "R6",
		"R7", "R8", "R9", "R10", "R11", "R12", "R13",
	} {
		// Use a leading space or open-bracket to avoid matching things
		// like "R10" inside "R100" (none exist today, but defensive).
		if !strings.Contains(doc, rule+":") &&
			!strings.Contains(doc, rule+" ") &&
			!strings.Contains(doc, rule+"\n") {
			t.Errorf("%s must mention contract rule %s. summary/02-contract.md "+
				"has 13 rules and the public doc must cover all of them.", docPath, rule)
		}
	}
}

// TestContributorResolutionDocHasOperatorSections pins the four
// operator-facing sections required by Phase A's definition of done.
// Phrases chosen to be descriptive without locking exact heading text.
func TestContributorResolutionDocHasOperatorSections(t *testing.T) {
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatalf("cannot read %s: %v", docPath, err)
	}
	doc := strings.ToLower(string(data))

	sections := map[string]string{
		"FAQ / data-quality":     "data quality",
		"Diagnostic queries":     "diagnostic",
		"Intentional limitations": "limitation",
		"Contract rules":         "contract",
	}
	for label, needle := range sections {
		if !strings.Contains(doc, needle) {
			t.Errorf("%s should contain a %q section (looking for the substring %q "+
				"in the lowercased text). Phase A's definition of done requires "+
				"FAQ, diagnostic queries, intentional limitations, and the contract.",
				docPath, label, needle)
		}
	}
}
