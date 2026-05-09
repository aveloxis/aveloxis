package db

import (
	"os"
	"strings"
	"testing"
)

// TestParityMatrixDocumented pins that the architecture doc carries
// the v0.20.3 GitLab-vs-GitHub parity matrix. Phase F's deliverable
// is the matrix itself plus filed gaps; this test ensures the matrix
// section exists and the major rows are present.
func TestParityMatrixDocumented(t *testing.T) {
	data, err := os.ReadFile(docPath)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(data)

	if !strings.Contains(doc, "parity matrix") &&
		!strings.Contains(doc, "Parity matrix") &&
		!strings.Contains(doc, "GitLab vs GitHub") {
		t.Error("docs/architecture/contributor-resolution.md must contain " +
			"a v0.20.3 parity-matrix section explicitly comparing each " +
			"gh_* / gl_* column. Operators querying contributor data on " +
			"mixed GitHub+GitLab fleets need this to know what to expect.")
	}

	// Pin a few specific column references that should appear in the
	// matrix — these are the ones operators are most likely to query.
	for _, col := range []string{
		"gl_state",
		"gh_node_id",
		"gh_followers_url",
	} {
		if !strings.Contains(doc, col) {
			t.Errorf("parity matrix should reference %q so operators know "+
				"its parity status (matched / approximate / accepted limit / "+
				"closable gap).", col)
		}
	}
}

// TestContributorIdentityHasStateField pins the v0.20.3 closable-gap
// fix: `ContributorIdentity` gains a `State` field so GitLab's
// `glUser.State` (active / blocked / banned / deactivated) can flow
// through to `contributors.gl_state`. Pre-v0.20.3 the State was
// parsed from JSON but never plumbed downstream.
func TestContributorIdentityHasStateField(t *testing.T) {
	data, err := os.ReadFile("../model/repo.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Find the ContributorIdentity struct.
	idx := strings.Index(src, "type ContributorIdentity struct")
	if idx < 0 {
		t.Fatal("cannot find ContributorIdentity in model/repo.go")
	}
	end := strings.Index(src[idx:], "}")
	if end < 0 {
		t.Fatal("cannot find end of ContributorIdentity")
	}
	body := src[idx : idx+end]

	if !strings.Contains(body, "State") {
		t.Error("ContributorIdentity must include a State field so GitLab's " +
			"user-state (active / blocked / etc.) can flow into " +
			"contributors.gl_state. Phase F closable gap.")
	}
}
