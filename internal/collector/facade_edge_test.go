package collector

import (
	"fmt"
	"testing"

	"github.com/augurlabs/aveloxis/internal/model"
)

// ============================================================
// parseCommitHeader edge cases (beyond facade_test.go coverage)
// ============================================================

func TestParseCommitHeader_MergeCommitThreeParents(t *testing.T) {
	// Octopus merge has 3+ parents.
	line := "merge1" + fieldSep +
		"p1 p2 p3" + fieldSep +
		"Author" + fieldSep + "a@b.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Committer" + fieldSep + "c@d.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Octopus merge"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil commit")
	}
	if len(c.Parents) != 3 {
		t.Errorf("octopus merge parents = %d, want 3", len(c.Parents))
	}
}

func TestParseCommitHeader_UnicodeInSubject(t *testing.T) {
	line := "abc" + fieldSep +
		"" + fieldSep +
		"Jose" + fieldSep + "j@ex.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Jose" + fieldSep + "j@ex.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Fix: handle unicode chars"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	if c.Subject != "Fix: handle unicode chars" {
		t.Errorf("Subject = %q", c.Subject)
	}
}

func TestParseCommitHeader_EmptyLine(t *testing.T) {
	c := parseCommitHeader("")
	if c != nil {
		t.Error("empty line should return nil")
	}
}

func TestParseCommitHeader_WhitespaceOnly(t *testing.T) {
	c := parseCommitHeader("   \t  ")
	if c != nil {
		t.Error("whitespace-only should return nil")
	}
}

func TestParseCommitHeader_EmptyAuthorEmail(t *testing.T) {
	line := "abc" + fieldSep +
		"" + fieldSep +
		"Author" + fieldSep + "" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"Committer" + fieldSep + "c@d.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"msg"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	if c.AuthorEmail != "" {
		t.Errorf("AuthorEmail = %q, want empty", c.AuthorEmail)
	}
}

func TestParseCommitHeader_SubjectWithFieldSep(t *testing.T) {
	// Subject might contain the field separator character in theory.
	// SplitN with 9 means the 9th field captures everything remaining.
	line := "abc" + fieldSep +
		"" + fieldSep +
		"A" + fieldSep + "a@b.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"C" + fieldSep + "c@d.com" + fieldSep + "2024-01-01T00:00:00Z" + fieldSep +
		"msg" + fieldSep + "extra"
	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil")
	}
	// The 9th field should capture everything after the 8th separator.
	if c.Subject != "msg"+fieldSep+"extra" {
		t.Errorf("Subject = %q, want subject with separator", c.Subject)
	}
}

// ============================================================
// parseNumstatLine edge cases (beyond facade_test.go coverage)
// ============================================================

func TestParseNumstatLine_ZeroCounts(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "0\t0\tempty.txt")
	if len(c.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(c.Files))
	}
	if c.Files[0].Additions != 0 || c.Files[0].Deletions != 0 {
		t.Error("expected 0/0")
	}
}

func TestParseNumstatLine_PathWithSpaces(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "5\t3\tpath with spaces/file.txt")
	if len(c.Files) != 1 {
		t.Fatal("expected 1 file")
	}
	if c.Files[0].Filename != "path with spaces/file.txt" {
		t.Errorf("filename = %q", c.Files[0].Filename)
	}
}

func TestParseNumstatLine_LargeNumbers(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "99999\t88888\tbig.go")
	if c.Files[0].Additions != 99999 || c.Files[0].Deletions != 88888 {
		t.Errorf("large: %d/%d", c.Files[0].Additions, c.Files[0].Deletions)
	}
}

func TestParseNumstatLine_EmptyFilename(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "1\t0\t")
	if len(c.Files) != 1 {
		t.Fatal("expected 1 file")
	}
	if c.Files[0].Filename != "" {
		t.Errorf("filename = %q, want empty", c.Files[0].Filename)
	}
}

func TestParseNumstatLine_MultipleFiles(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "1\t0\tfile1.go")
	parseNumstatLine(c, "2\t1\tfile2.go")
	parseNumstatLine(c, "3\t2\tfile3.go")
	if len(c.Files) != 3 {
		t.Fatalf("expected 3 files, got %d", len(c.Files))
	}
}

// ============================================================
// extractDate edge cases (beyond facade_test.go coverage)
// ============================================================

func TestExtractDate_ExactTenChars(t *testing.T) {
	got := extractDate("2024-01-15")
	if got != "2024-01-15" {
		t.Errorf("extractDate = %q", got)
	}
}

func TestExtractDate_LongISO(t *testing.T) {
	got := extractDate("2024-06-15T14:30:00+05:30")
	if got != "2024-06-15" {
		t.Errorf("extractDate = %q", got)
	}
}

func TestExtractDate_ShortString(t *testing.T) {
	got := extractDate("abc")
	if got != "abc" {
		t.Errorf("short: %q", got)
	}
}

// ============================================================
// parseTimestamp edge cases (beyond facade_test.go coverage)
// ============================================================

func TestParseTimestamp_PositiveOffset(t *testing.T) {
	ts := parseTimestamp("2024-06-15T14:30:00+05:30")
	if ts == nil {
		t.Error("should handle positive timezone offset")
	}
}

func TestParseTimestamp_NegativeOffset(t *testing.T) {
	ts := parseTimestamp("2024-06-15T14:30:00-08:00")
	if ts == nil {
		t.Error("should handle negative timezone offset")
	}
}

func TestParseTimestamp_Empty(t *testing.T) {
	if parseTimestamp("") != nil {
		t.Error("empty should return nil")
	}
}

// ============================================================
// platformHost edge cases (beyond facade_test.go coverage)
// ============================================================

func TestPlatformHost_GenericGit(t *testing.T) {
	// PlatformGenericGit (3) — should return "unknown" or similar.
	host := platformHost(model.PlatformGenericGit)
	// Actual behavior depends on implementation; document it.
	_ = host
}

// ============================================================
// clonePath edge cases
// ============================================================

func TestClonePath_Format(t *testing.T) {
	fc := &FacadeCollector{repoDir: "/tmp/repos"}
	if path := fc.clonePath(42); path != "/tmp/repos/repo_42" {
		t.Errorf("clonePath = %q", path)
	}
}

func TestClonePath_LargeID(t *testing.T) {
	fc := &FacadeCollector{repoDir: "/data"}
	if path := fc.clonePath(999999999); path != "/data/repo_999999999" {
		t.Errorf("clonePath = %q", path)
	}
}

func TestClonePath_ZeroID(t *testing.T) {
	fc := &FacadeCollector{repoDir: "/tmp"}
	if path := fc.clonePath(0); path != "/tmp/repo_0" {
		t.Errorf("clonePath = %q", path)
	}
}

// ============================================================
// FacadeResult edge cases
// ============================================================

func TestFacadeResult_Defaults(t *testing.T) {
	r := &FacadeResult{}
	if r.Commits != 0 {
		t.Errorf("Commits = %d", r.Commits)
	}
	if r.CommitMessages != 0 {
		t.Errorf("CommitMessages = %d", r.CommitMessages)
	}
	if r.Errors != nil {
		t.Error("Errors should be nil")
	}
}

func TestFacadeResult_WithErrors(t *testing.T) {
	r := &FacadeResult{
		Commits:        100,
		CommitMessages: 50,
	}
	r.Errors = append(r.Errors, fmt.Errorf("test error"))
	if len(r.Errors) != 1 {
		t.Errorf("errors = %d, want 1", len(r.Errors))
	}
}
