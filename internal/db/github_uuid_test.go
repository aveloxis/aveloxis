package db

import (
	"testing"

	"github.com/google/uuid"
)

func TestGithubUUID_Deterministic(t *testing.T) {
	// Same user ID always produces the same UUID.
	a := GithubUUID(12345)
	b := GithubUUID(12345)
	if a != b {
		t.Errorf("GithubUUID(12345) produced different UUIDs: %s vs %s", a, b)
	}
}

func TestGithubUUID_DifferentUsers(t *testing.T) {
	a := GithubUUID(1)
	b := GithubUUID(2)
	if a == b {
		t.Error("different user IDs should produce different UUIDs")
	}
}

func TestGithubUUID_PlatformByte(t *testing.T) {
	u := GithubUUID(0)
	// First byte should be 1 (GitHub platform).
	if u[0] != 1 {
		t.Errorf("first byte = %d, want 1 (GitHub)", u[0])
	}
}

func TestGithubUUID_CompatibleWithAugur(t *testing.T) {
	// Verify against a known Augur-generated UUID.
	// Augur's GithubUUID for gh_user_id=1 produces:
	// platform=1 (byte 0), user=1 (bytes 1-4 big-endian), rest zeros.
	// Bytes: [01, 00, 00, 00, 01, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00, 00]
	// UUID groups: bytes 0-3 | 4-5 | 6-7 | 8-9 | 10-15
	// = 01000000-0100-0000-0000-000000000000
	u := GithubUUID(1)
	expected := uuid.MustParse("01000000-0100-0000-0000-000000000000")
	if u != expected {
		t.Errorf("GithubUUID(1) = %s, want %s", u, expected)
	}
}

func TestGithubUUID_LargeUserID(t *testing.T) {
	// gh_user_id can be up to ~2 billion (fits in uint32).
	u := GithubUUID(29740296) // CHAOSS org ID
	if u == uuid.Nil {
		t.Error("expected non-nil UUID for large user ID")
	}
	// Should be deterministic.
	if u != GithubUUID(29740296) {
		t.Error("not deterministic for large user ID")
	}
}

func TestGitLabUUID_PlatformByte(t *testing.T) {
	u := GitLabUUID(42)
	if u[0] != 2 {
		t.Errorf("first byte = %d, want 2 (GitLab)", u[0])
	}
}

func TestPlatformUUID_MatchesSpecific(t *testing.T) {
	gh := PlatformUUID(1, 12345)
	ghDirect := GithubUUID(12345)
	if gh != ghDirect {
		t.Errorf("PlatformUUID(1, 12345) = %s, GithubUUID(12345) = %s", gh, ghDirect)
	}

	gl := PlatformUUID(2, 42)
	glDirect := GitLabUUID(42)
	if gl != glDirect {
		t.Errorf("PlatformUUID(2, 42) = %s, GitLabUUID(42) = %s", gl, glDirect)
	}
}
