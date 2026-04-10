package db

import (
	"encoding/binary"

	"github.com/google/uuid"
)

// GithubUUID generates deterministic UUIDs compatible with Augur's scheme.
// The UUID encodes platform ID (byte 0) and GitHub user ID (bytes 1-4).
// This ensures that the same GitHub user always gets the same cntrb_id,
// regardless of which system created it.
//
// Layout (16 bytes):
//   [0]     platform ID (1 = GitHub)
//   [1:5]   gh_user_id (big-endian uint32)
//   [5:16]  zeros (reserved for repo/issue/event in other Augur UUID types)
func GithubUUID(ghUserID int64) uuid.UUID {
	var b [16]byte
	b[0] = 1 // platform = GitHub
	binary.BigEndian.PutUint32(b[1:5], uint32(ghUserID))
	return uuid.UUID(b)
}

// GitLabUUID generates deterministic UUIDs for GitLab users.
// Same layout as GithubUUID but with platform ID = 2.
func GitLabUUID(glUserID int64) uuid.UUID {
	var b [16]byte
	b[0] = 2 // platform = GitLab
	binary.BigEndian.PutUint32(b[1:5], uint32(glUserID))
	return uuid.UUID(b)
}

// PlatformUUID generates a deterministic UUID for a platform user ID.
func PlatformUUID(platformID int, userID int64) uuid.UUID {
	var b [16]byte
	b[0] = byte(platformID)
	binary.BigEndian.PutUint32(b[1:5], uint32(userID))
	return uuid.UUID(b)
}
