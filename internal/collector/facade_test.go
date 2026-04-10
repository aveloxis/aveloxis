package collector

import (
	"testing"
	"time"

	"github.com/augurlabs/aveloxis/internal/model"
)

func TestParseCommitHeader_ValidLine(t *testing.T) {
	// Simulates a git log line with our field separator format.
	line := "abc123def" + fieldSep +
		"parent1 parent2" + fieldSep +
		"John Doe" + fieldSep +
		"john@example.com" + fieldSep +
		"2024-01-15T10:30:00-06:00" + fieldSep +
		"Jane Smith" + fieldSep +
		"jane@example.com" + fieldSep +
		"2024-01-15T11:00:00-06:00" + fieldSep +
		"Fix the bug"

	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil commit")
	}
	if c.Hash != "abc123def" {
		t.Errorf("Hash = %q, want %q", c.Hash, "abc123def")
	}
	if len(c.Parents) != 2 {
		t.Fatalf("Parents count = %d, want 2", len(c.Parents))
	}
	if c.Parents[0] != "parent1" || c.Parents[1] != "parent2" {
		t.Errorf("Parents = %v, want [parent1, parent2]", c.Parents)
	}
	if c.AuthorName != "John Doe" {
		t.Errorf("AuthorName = %q, want %q", c.AuthorName, "John Doe")
	}
	if c.AuthorEmail != "john@example.com" {
		t.Errorf("AuthorEmail = %q, want %q", c.AuthorEmail, "john@example.com")
	}
	if c.CommitterName != "Jane Smith" {
		t.Errorf("CommitterName = %q, want %q", c.CommitterName, "Jane Smith")
	}
	if c.Subject != "Fix the bug" {
		t.Errorf("Subject = %q, want %q", c.Subject, "Fix the bug")
	}
}

func TestParseCommitHeader_NoParents(t *testing.T) {
	// Initial commit has no parents.
	line := "abc123" + fieldSep +
		"" + fieldSep + // empty parents
		"Author" + fieldSep +
		"a@b.com" + fieldSep +
		"2024-01-01T00:00:00Z" + fieldSep +
		"Committer" + fieldSep +
		"c@d.com" + fieldSep +
		"2024-01-01T00:00:00Z" + fieldSep +
		"Initial commit"

	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil commit")
	}
	if len(c.Parents) != 0 {
		t.Errorf("Parents = %v, want empty", c.Parents)
	}
}

func TestParseCommitHeader_TooFewFields(t *testing.T) {
	c := parseCommitHeader("abc" + fieldSep + "def")
	if c != nil {
		t.Error("expected nil for malformed line")
	}
}

func TestParseCommitHeader_WithCommitSepSuffix(t *testing.T) {
	line := "abc123" + fieldSep +
		"" + fieldSep +
		"A" + fieldSep +
		"a@b.com" + fieldSep +
		"2024-01-01T00:00:00Z" + fieldSep +
		"C" + fieldSep +
		"c@d.com" + fieldSep +
		"2024-01-01T00:00:00Z" + fieldSep +
		"msg" + commitSep

	c := parseCommitHeader(line)
	if c == nil {
		t.Fatal("expected non-nil commit")
	}
	if c.Subject != "msg" {
		t.Errorf("Subject = %q, want %q", c.Subject, "msg")
	}
}

func TestParseNumstatLine(t *testing.T) {
	c := &parsedCommit{}

	parseNumstatLine(c, "10\t5\tsrc/main.go")
	if len(c.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(c.Files))
	}
	if c.Files[0].Additions != 10 {
		t.Errorf("Additions = %d, want 10", c.Files[0].Additions)
	}
	if c.Files[0].Deletions != 5 {
		t.Errorf("Deletions = %d, want 5", c.Files[0].Deletions)
	}
	if c.Files[0].Filename != "src/main.go" {
		t.Errorf("Filename = %q, want %q", c.Files[0].Filename, "src/main.go")
	}
}

func TestParseNumstatLine_Binary(t *testing.T) {
	c := &parsedCommit{}

	// Binary files show "-" for adds/dels in numstat.
	parseNumstatLine(c, "-\t-\timage.png")
	if len(c.Files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(c.Files))
	}
	// strconv.Atoi("-") returns 0, error — we accept 0.
	if c.Files[0].Additions != 0 || c.Files[0].Deletions != 0 {
		t.Errorf("expected 0/0 for binary, got %d/%d", c.Files[0].Additions, c.Files[0].Deletions)
	}
}

func TestParseNumstatLine_InvalidFormat(t *testing.T) {
	c := &parsedCommit{}
	parseNumstatLine(c, "not a numstat line")
	if len(c.Files) != 0 {
		t.Errorf("expected 0 files for invalid line, got %d", len(c.Files))
	}
}

func TestExtractDate(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"2024-01-15T10:30:00-06:00", "2024-01-15"},
		{"2024-12-31", "2024-12-31"},
		{"short", "short"},
		{"", ""},
	}
	for _, tt := range tests {
		got := extractDate(tt.input)
		if got != tt.want {
			t.Errorf("extractDate(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestParseTimestamp(t *testing.T) {
	ts := parseTimestamp("2024-01-15T10:30:00Z")
	if ts == nil {
		t.Fatal("expected non-nil timestamp")
	}
	if ts.Year() != 2024 || ts.Month() != time.January || ts.Day() != 15 {
		t.Errorf("unexpected date: %v", ts)
	}

	// Invalid format.
	if parseTimestamp("not-a-date") != nil {
		t.Error("expected nil for invalid date")
	}
}

func TestPlatformHost(t *testing.T) {
	tests := []struct {
		name string
		plat int
		want string
	}{
		{"github", 1, "github.com"},
		{"gitlab", 2, "gitlab.com"},
		{"unknown", 99, "unknown"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// model.Platform is an int type; we cast here for simplicity.
			got := platformHost(model.Platform(tt.plat))
			if got != tt.want {
				t.Errorf("platformHost(%d) = %q, want %q", tt.plat, got, tt.want)
			}
		})
	}
}
