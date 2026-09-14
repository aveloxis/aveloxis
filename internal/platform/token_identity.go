// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"crypto/sha256"
	"encoding/hex"
)

// KeyIDDomain separates key fingerprints from any other sha256 use of the
// same bytes; the version lets the derivation change without colliding.
// Exported for the one SQL spelling (internal/db keyIDSQL), which a DB-tier
// test pins byte-equal to KeyID.
const KeyIDDomain = "aveloxis-key-fp:v1:"

// KeyID is the stable, log-safe identity of an API token (v0.30.0 Phase C):
// the first 16 hex characters of sha256(KeyIDDomain + token). The key
// report, the admin page, the stored worker_oauth.key_id and pool log lines
// all use it, so one key is recognisable everywhere without its secret. It
// is not reversible for a random token, and every stored token already sits
// in plaintext in worker_oauth, readable by the same database role.
func KeyID(token string) string {
	sum := sha256.Sum256([]byte(KeyIDDomain + token))
	return hex.EncodeToString(sum[:])[:16]
}

// MaskMinLen is the shortest token MaskToken shows any part of: eight
// revealed characters of a token under 12 would be most of it. Exported for
// the SQL spelling (internal/db keyMaskSQL).
const MaskMinLen = 12

// MaskToken is the one display form of a token: its first and last four
// characters, or "(hidden)" when it is too short to reveal any of it.
func MaskToken(token string) string {
	if len(token) < MaskMinLen {
		return "(hidden)"
	}
	return token[:4] + "..." + token[len(token)-4:]
}
