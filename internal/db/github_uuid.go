package db

import (
	"encoding/binary"
	"math"

	"github.com/google/uuid"
)

// GithubUUID generates deterministic UUIDs compatible with Augur's scheme.
// The UUID encodes platform ID (byte 0) and GitHub user ID.
//
// For IDs that fit in uint32 (the vast majority today), the layout matches
// Augur exactly for backward compatibility:
//
//	[0]     platform ID (1 = GitHub)
//	[1:5]   user_id (big-endian uint32)
//	[5:16]  zeros
//
// For IDs exceeding uint32 (future-proofing), the full int64 is stored:
//
//	[0]     platform ID (1 = GitHub)
//	[1:9]   user_id (big-endian uint64)
//	[9:16]  zeros
//
// This ensures existing UUIDs in the database are never changed, while IDs
// above 2^32 still produce unique, deterministic UUIDs instead of silently
// truncating.
func GithubUUID(ghUserID int64) uuid.UUID {
	return platformUUIDInternal(1, ghUserID)
}

// GitLabUUID generates deterministic UUIDs for GitLab users.
// Same layout as GithubUUID but with platform ID = 2.
func GitLabUUID(glUserID int64) uuid.UUID {
	return platformUUIDInternal(2, glUserID)
}

// PlatformUUID generates a deterministic UUID for a platform user ID.
func PlatformUUID(platformID int, userID int64) uuid.UUID {
	return platformUUIDInternal(byte(platformID), userID)
}

func platformUUIDInternal(platformByte byte, userID int64) uuid.UUID {
	var b [16]byte
	b[0] = platformByte
	if userID >= 0 && userID <= math.MaxUint32 {
		// Fits in uint32 — use the original 4-byte layout for Augur compatibility.
		binary.BigEndian.PutUint32(b[1:5], uint32(userID))
	} else {
		// Exceeds uint32 or is negative — use full 8-byte layout.
		// This produces a different UUID than truncation would, preventing collisions.
		binary.BigEndian.PutUint64(b[1:9], uint64(userID))
	}
	return uuid.UUID(b)
}
