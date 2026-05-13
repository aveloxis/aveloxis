package db

import (
	"os"
	"strings"
	"testing"
)

// v0.20.12 (Fix H): BackfillCommitAuthorIDs used a case-sensitive
// JOIN between commits.cmt_author_platform_username and
// contributors.gh_login. GitHub treats logins as
// case-insensitive but case-preserving — commit metadata may
// store "NitishT" while the contributors row was inserted as
// "nitisht" (or vice versa) depending on the source endpoint.
// Production diagnostic on the live aveloxis_large DB showed
// 2,511 distinct commits recoverable under a case-insensitive
// JOIN vs only 592 under the case-sensitive comparison.

// TestBackfillCommitAuthorIDsUsesCaseInsensitiveJoin pins the
// SQL shape. The implementation may use LOWER() on both sides,
// ILIKE, or a generated-column index, but the join must be
// case-insensitive.
func TestBackfillCommitAuthorIDsUsesCaseInsensitiveJoin(t *testing.T) {
	data, err := os.ReadFile("commit_resolver_store.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	idx := strings.Index(src, "func (s *PostgresStore) BackfillCommitAuthorIDs(")
	if idx < 0 {
		t.Fatal("cannot find BackfillCommitAuthorIDs")
	}
	tail := src[idx:]
	endRel := strings.Index(tail[1:], "\nfunc ")
	if endRel < 0 {
		t.Fatal("cannot find end of BackfillCommitAuthorIDs")
	}
	body := tail[:1+endRel]

	// The pre-v0.20.12 form was the bare equality:
	//   c.cmt_author_platform_username = cn.gh_login
	// The fix must make that case-insensitive. Acceptable shapes:
	//   LOWER(c.cmt_author_platform_username) = LOWER(cn.gh_login)
	//   c.cmt_author_platform_username ILIKE cn.gh_login   -- not quite right (wildcards)
	//   LOWER variants only — keep this pin conservative.
	if !strings.Contains(body, "LOWER(c.cmt_author_platform_username)") ||
		!strings.Contains(body, "LOWER(cn.gh_login)") {
		t.Error("BackfillCommitAuthorIDs must compare LOWER(c.cmt_author_platform_username) to LOWER(cn.gh_login) so a contributor stored as 'nitisht' matches commit metadata 'NitishT'. Production diagnostic: 1,919 additional commits resolvable under case-insensitive comparison.")
	}

	// Regression pin against the pre-v0.20.12 case-sensitive form.
	// We require the equality to be wrapped in LOWER on both sides;
	// the bare `c.cmt_author_platform_username = cn.gh_login` should
	// no longer appear inside the JOIN/WHERE clause.
	if strings.Contains(body, "AND c.cmt_author_platform_username = cn.gh_login") {
		t.Error("BackfillCommitAuthorIDs still contains the pre-v0.20.12 case-sensitive equality. Must be replaced with LOWER(...) = LOWER(...) so case-mismatched contributor rows match.")
	}
}
