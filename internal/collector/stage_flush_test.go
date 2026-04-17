package collector

import (
	"os"
	"strings"
	"testing"
)

// Background
// ==========
// StagingWriter.Stage buffers inserts in an in-memory pgx.Batch and only
// auto-sends to Postgres when the buffer reaches stagingFlushSize (500).
// Any caller that stages fewer than 500 items and then hands control to
// Processor.ProcessRepo WITHOUT calling sw.Flush(ctx) first will silently
// drop every buffered row: ProcessRepo reads from the staging table, which
// is empty; then the StagingWriter goes out of scope and the batch is GC'd.
//
// This bug was observed in production as "a small cluster of repositories
// that show PRs and Issues in their metadata, but don't ever seem to
// collect them" — the fillX/refreshX functions below logged
// "gap fill completed filled=147" while zero rows reached the DB.
//
// These source-contract tests enforce that every caller that builds its
// own StagingWriter and then invokes ProcessRepo flushes first.

// extractFuncBody returns the body of the named method/function from the
// given file (roughly — from the `func X(` line up to the next top-level
// `\nfunc `). Enough for the substring checks below.
func extractFuncBody(t *testing.T, path, sig string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	src := string(data)
	idx := strings.Index(src, sig)
	if idx < 0 {
		t.Fatalf("cannot find %q in %s", sig, path)
	}
	body := src[idx:]
	if end := strings.Index(body, "\nfunc "); end > 0 {
		body = body[:end]
	}
	return body
}

// assertFlushBeforeProcess fails the test if the function body invokes
// proc.ProcessRepo without a preceding sw.Flush( call.
func assertFlushBeforeProcess(t *testing.T, funcLabel, body string) {
	t.Helper()
	procIdx := strings.Index(body, "proc.ProcessRepo(")
	if procIdx < 0 {
		// Some call sites use a different processor variable name; widen the check.
		procIdx = strings.Index(body, ".ProcessRepo(")
		if procIdx < 0 {
			t.Fatalf("%s: no ProcessRepo call found — test premise invalid, update the test", funcLabel)
		}
	}
	preamble := body[:procIdx]
	if !strings.Contains(preamble, "sw.Flush(") {
		t.Errorf("%s: must call sw.Flush(ctx) before invoking ProcessRepo — "+
			"otherwise the in-memory pgx.Batch inside StagingWriter is never sent to Postgres, "+
			"Processor.ProcessRepo reads an empty staging table, and the buffered rows are "+
			"lost when the StagingWriter goes out of scope. Observed symptom: "+
			"'gap fill completed filled=N' with zero rows in aveloxis_data.issues.",
			funcLabel)
	}
}

// TestFillIssueGapsFlushesBeforeProcess — gap_fill.go:fillIssueGaps must flush.
func TestFillIssueGapsFlushesBeforeProcess(t *testing.T) {
	body := extractFuncBody(t, "gap_fill.go", "func (gf *GapFiller) fillIssueGaps(")
	assertFlushBeforeProcess(t, "fillIssueGaps", body)
}

// TestFillPRGapsFlushesBeforeProcess — gap_fill.go:fillPRGaps must flush.
func TestFillPRGapsFlushesBeforeProcess(t *testing.T) {
	body := extractFuncBody(t, "gap_fill.go", "func (gf *GapFiller) fillPRGaps(")
	assertFlushBeforeProcess(t, "fillPRGaps", body)
}

// TestRefreshIssuesFlushesBeforeProcess — refresh_open.go:refreshIssues must flush.
func TestRefreshIssuesFlushesBeforeProcess(t *testing.T) {
	body := extractFuncBody(t, "refresh_open.go", "func (r *OpenItemRefresher) refreshIssues(")
	assertFlushBeforeProcess(t, "refreshIssues", body)
}

// TestRefreshPRsFlushesBeforeProcess — refresh_open.go:refreshPRs must flush.
func TestRefreshPRsFlushesBeforeProcess(t *testing.T) {
	body := extractFuncBody(t, "refresh_open.go", "func (r *OpenItemRefresher) refreshPRs(")
	assertFlushBeforeProcess(t, "refreshPRs", body)
}

// TestStagingWriterContractDocumented — make the "must flush" requirement
// visible in the StagingWriter docs themselves so future callers don't
// reintroduce the bug. Looks for a phrase like "Flush" being explicitly
// required in either the Stage or NewStagingWriter docstring.
func TestStagingWriterContractDocumented(t *testing.T) {
	data, err := os.ReadFile("../db/staging.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// Look at the lead-in comment for Stage. It should mention that callers
	// must Flush, otherwise buffered rows are dropped when the writer goes
	// out of scope.
	stageIdx := strings.Index(src, "func (w *StagingWriter) Stage(")
	if stageIdx < 0 {
		t.Fatal("Stage method not found")
	}
	// Look at the 400 chars preceding the func to grab its doc comment.
	start := stageIdx - 400
	if start < 0 {
		start = 0
	}
	lead := src[start:stageIdx]
	if !strings.Contains(lead, "Flush") {
		t.Error("Stage's doc comment must explicitly mention Flush() as a required call — " +
			"the 'buffer-until-500-or-Flush' contract is invisible otherwise, " +
			"and callers who build their own StagingWriter for small gap fills " +
			"silently drop all buffered rows (see gap_fill.go / refresh_open.go)")
	}
}
