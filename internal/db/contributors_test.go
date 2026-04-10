package db

import (
	"os"
	"strings"
	"testing"
)

// TestResolverInsertUsesCorrectOnConflictSyntax verifies the contributor
// INSERT in ContributorResolver.Resolve uses the correct partial unique index
// syntax: ON CONFLICT (cntrb_login) WHERE cntrb_login != ''
//
// The bug: the original code used ON CONFLICT ON CONSTRAINT idx_contributors_login
// WHERE cntrb_login != '' which is invalid PostgreSQL — ON CONSTRAINT doesn't
// accept a WHERE clause, and idx_contributors_login is an index not a constraint.
// This caused EVERY lazy contributor creation to fail with a syntax error,
// silently producing 131K+ messages with NULL cntrb_id.
func TestResolverInsertUsesCorrectOnConflictSyntax(t *testing.T) {
	data, err := os.ReadFile("contributors.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Must NOT use ON CONFLICT ON CONSTRAINT for the contributor insert.
	// That form doesn't work with partial indexes and idx_contributors_login
	// is an index, not a constraint.
	if strings.Contains(src, "ON CONFLICT ON CONSTRAINT idx_contributors_login") {
		t.Error("Resolve must NOT use ON CONFLICT ON CONSTRAINT idx_contributors_login — " +
			"idx_contributors_login is an index not a constraint, and ON CONSTRAINT " +
			"doesn't accept a WHERE clause. Use: ON CONFLICT (cntrb_login) WHERE cntrb_login != ''")
	}

	// Must use the correct partial index form.
	idx := strings.Index(src, "func (r *ContributorResolver) Resolve(")
	if idx < 0 {
		t.Fatal("cannot find Resolve function")
	}
	fnBody := src[idx:]
	if len(fnBody) > 2000 {
		fnBody = fnBody[:2000]
	}

	if !strings.Contains(fnBody, `ON CONFLICT (cntrb_login) WHERE cntrb_login != ''`) {
		t.Error("Resolve INSERT must use ON CONFLICT (cntrb_login) WHERE cntrb_login != '' " +
			"to match the partial unique index idx_contributors_login")
	}
}
