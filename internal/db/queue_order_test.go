package db

// Source-contract tests for the ORDER BY clauses in ListQueue and
// ListQueuePage. Without a stable tiebreaker, LIMIT/OFFSET pagination
// can return overlapping or skipped rows when the sort columns mutate
// during active collection — which is exactly what happens to `status`,
// `priority`, and `due_at` every time a job completes. The symptoms
// users see: Prev/Next appear "broken" because the page changes are
// hidden by concurrent drift, while First/Last still work because
// their page jumps are large enough to visibly succeed.
//
// repo_id is monotonic (SERIAL), non-null, and never changes for a
// given row, so it's the correct tiebreaker.

import (
	"os"
	"strings"
	"testing"
)

// extractOrderBy returns the ORDER BY clause from a function body,
// trimmed so callers can substring-match with confidence. The helper
// stops at LIMIT, the closing backtick, or the end of the function —
// whichever comes first — so trailing SQL like LIMIT/OFFSET and Go
// boilerplate (rows.Scan) don't pollute the match.
func extractOrderBy(t *testing.T, fnBody string) string {
	t.Helper()
	idx := strings.Index(fnBody, "ORDER BY")
	if idx < 0 {
		t.Fatal("ORDER BY not found in function body")
	}
	clause := fnBody[idx:]
	// Stop at the earliest of LIMIT, closing backtick, or 500 chars.
	if cut := strings.Index(clause, "LIMIT"); cut > 0 {
		clause = clause[:cut]
	}
	if cut := strings.Index(clause, "`"); cut > 0 && cut < len(clause) {
		clause = clause[:cut]
	}
	if len(clause) > 500 {
		clause = clause[:500]
	}
	return clause
}

// extractFn returns just the body of the named method from queue.go.
func extractFn(t *testing.T, src, marker string) string {
	t.Helper()
	idx := strings.Index(src, marker)
	if idx < 0 {
		t.Fatalf("cannot find %q", marker)
	}
	body := src[idx:]
	end := strings.Index(body[1:], "\nfunc ")
	if end > 0 {
		body = body[:end+1]
	}
	return body
}

func TestListQueuePageOrderByIncludesRepoIDTiebreaker(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFn(t, string(data), "func (s *PostgresStore) ListQueuePage")
	clause := extractOrderBy(t, body)

	if !strings.Contains(clause, "repo_id") {
		t.Errorf("ListQueuePage ORDER BY must end with q.repo_id as a stable tiebreaker.\n"+
			"Got: %q\n"+
			"Reason: status/priority/due_at all mutate during active collection; "+
			"without a monotonic tiebreaker LIMIT/OFFSET returns overlapping or "+
			"skipped rows between Prev/Next clicks.", clause)
	}
	// Must come AFTER the mutating columns so it's actually a tiebreaker,
	// not a primary sort.
	priIdx := strings.Index(clause, "priority")
	dueIdx := strings.Index(clause, "due_at")
	repoIdx := strings.LastIndex(clause, "repo_id")
	if repoIdx < priIdx || repoIdx < dueIdx {
		t.Errorf("repo_id must appear AFTER priority and due_at in ORDER BY so it acts as tiebreaker.\n"+
			"Got: %q", clause)
	}
}

func TestListQueueOrderByIncludesRepoIDTiebreaker(t *testing.T) {
	data, err := os.ReadFile("queue.go")
	if err != nil {
		t.Fatal(err)
	}
	body := extractFn(t, string(data), "func (s *PostgresStore) ListQueue(")
	clause := extractOrderBy(t, body)

	if !strings.Contains(clause, "repo_id") {
		t.Errorf("ListQueue ORDER BY must end with repo_id as a stable tiebreaker.\n"+
			"Got: %q\n"+
			"Reason: the monitor JSON API /api/queue consumes this; clients "+
			"that diff successive responses (or paginate through a snapshot) "+
			"need deterministic row order.", clause)
	}
	priIdx := strings.Index(clause, "priority")
	dueIdx := strings.Index(clause, "due_at")
	repoIdx := strings.LastIndex(clause, "repo_id")
	if repoIdx < priIdx || repoIdx < dueIdx {
		t.Errorf("repo_id must appear AFTER priority and due_at.\nGot: %q", clause)
	}
}
