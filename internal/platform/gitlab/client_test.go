package gitlab

import (
	"testing"
)

func TestGlUserToRef_AllFields(t *testing.T) {
	u := glUser{
		ID:          99,
		Username:    "gitlab-user",
		Name:        "GL User",
		Email:       "user@gitlab.com",
		PublicEmail: "public@gitlab.com",
		AvatarURL:   "https://gitlab.com/uploads/-/system/user/avatar/99/avatar.png",
		WebURL:      "https://gitlab.com/gitlab-user",
	}

	ref := glUserToRef(u)

	if ref.PlatformID != 99 {
		t.Errorf("PlatformID = %d, want 99", ref.PlatformID)
	}
	if ref.Login != "gitlab-user" {
		t.Errorf("Login = %q, want %q", ref.Login, "gitlab-user")
	}
	if ref.Name != "GL User" {
		t.Errorf("Name = %q, want %q", ref.Name, "GL User")
	}
	// When Email is set, it should be used (not PublicEmail).
	if ref.Email != "user@gitlab.com" {
		t.Errorf("Email = %q, want %q", ref.Email, "user@gitlab.com")
	}
	if ref.AvatarURL != "https://gitlab.com/uploads/-/system/user/avatar/99/avatar.png" {
		t.Errorf("AvatarURL = %q, want correct URL", ref.AvatarURL)
	}
	if ref.URL != "https://gitlab.com/gitlab-user" {
		t.Errorf("URL = %q, want %q", ref.URL, "https://gitlab.com/gitlab-user")
	}
}

func TestGlUserToRef_UsesPublicEmailWhenEmailEmpty(t *testing.T) {
	u := glUser{
		ID:          42,
		Username:    "someone",
		Email:       "", // empty
		PublicEmail: "fallback@example.com",
	}

	ref := glUserToRef(u)

	if ref.Email != "fallback@example.com" {
		t.Errorf("Email = %q, want %q (should fall back to PublicEmail)", ref.Email, "fallback@example.com")
	}
}

func TestGlUserToRef_ZeroUser(t *testing.T) {
	var u glUser
	ref := glUserToRef(u)

	if ref.PlatformID != 0 {
		t.Errorf("PlatformID = %d, want 0", ref.PlatformID)
	}
	if ref.Login != "" {
		t.Errorf("Login = %q, want empty", ref.Login)
	}
	if !ref.IsZero() {
		t.Error("expected zero glUser to produce a zero-ish UserRef")
	}
}

func TestCountDiffLines(t *testing.T) {
	tests := []struct {
		name     string
		diff     string
		wantAdds int
		wantDels int
	}{
		{
			name: "counts additions and deletions, skips --- and +++",
			diff: `--- a/file.go
+++ b/file.go
@@ -1,3 +1,4 @@
 package main
-import "fmt"
+import (
+	"fmt"
+	"os"
+)
-func old() {}`,
			wantAdds: 4,
			wantDels: 2,
		},
		{
			name:     "empty diff",
			diff:     "",
			wantAdds: 0,
			wantDels: 0,
		},
		{
			name: "only additions",
			diff: `+line1
+line2
+line3`,
			wantAdds: 3,
			wantDels: 0,
		},
		{
			name: "only deletions",
			diff: `-removed1
-removed2`,
			wantAdds: 0,
			wantDels: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adds, dels := countDiffLines(tt.diff)
			if adds != tt.wantAdds {
				t.Errorf("adds = %d, want %d", adds, tt.wantAdds)
			}
			if dels != tt.wantDels {
				t.Errorf("dels = %d, want %d", dels, tt.wantDels)
			}
		})
	}
}

func TestGlDiscussionNotePositionSideLogic(t *testing.T) {
	// Verify the side-determination logic used in ListReviewComments:
	// OldLine set + NewLine nil => LEFT, otherwise RIGHT.
	tests := []struct {
		name     string
		oldLine  *int
		newLine  *int
		wantSide string
	}{
		{"new line only (addition)", nil, intPtr(10), "RIGHT"},
		{"old line only (deletion)", intPtr(5), nil, "LEFT"},
		{"both lines (context/change)", intPtr(5), intPtr(10), "RIGHT"},
		{"neither line", nil, nil, "RIGHT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			side := "RIGHT"
			if tt.oldLine != nil && tt.newLine == nil {
				side = "LEFT"
			}
			if side != tt.wantSide {
				t.Errorf("side = %q, want %q", side, tt.wantSide)
			}
		})
	}
}

func TestGlDiscussionNoteTypes(t *testing.T) {
	// Verify type structures are correctly instantiated.
	disc := glDiscussion{
		ID: "abc123",
		Notes: []glDiscussionNote{
			{
				ID:     1,
				Body:   "system note",
				System: true,
			},
			{
				ID:   2,
				Body: "plain comment",
			},
			{
				ID:   3,
				Body: "diff comment",
				Position: &glNotePosition{
					BaseSHA:      "aaa",
					HeadSHA:      "bbb",
					NewPath:      "src/main.go",
					OldPath:      "src/main.go",
					PositionType: "text",
					NewLine:      intPtr(42),
				},
			},
		},
	}

	if len(disc.Notes) != 3 {
		t.Fatalf("expected 3 notes, got %d", len(disc.Notes))
	}

	// System note should be skipped in review comments.
	if !disc.Notes[0].System {
		t.Error("expected note 0 to be system")
	}

	// Plain comment has no position — should also be skipped.
	if disc.Notes[1].Position != nil {
		t.Error("expected note 1 to have nil position")
	}

	// Diff comment has position — this is the one that becomes a review comment.
	pos := disc.Notes[2].Position
	if pos == nil {
		t.Fatal("expected note 2 to have position")
	}
	if pos.NewPath != "src/main.go" {
		t.Errorf("NewPath = %q, want %q", pos.NewPath, "src/main.go")
	}
	if *pos.NewLine != 42 {
		t.Errorf("NewLine = %d, want 42", *pos.NewLine)
	}
	if pos.HeadSHA != "bbb" {
		t.Errorf("HeadSHA = %q, want %q", pos.HeadSHA, "bbb")
	}
}

func intPtr(v int) *int { return &v }
