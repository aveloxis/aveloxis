// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"crypto/sha256"
	"encoding/hex"
)

// MatviewsDefinitionVersion is the release that last changed a materialized
// view's DEFINITION in matviews.sql (worklist item 36, option 2 — operator
// decision 2026-09-26). Only a plain `aveloxis migrate` re-creates the 8Knot
// views from that file: `migrate --skip-views` skips them, `refresh-views` and
// the weekly rebuild refresh the DATA under the definition a view already
// has, and serve never re-creates one. So the named release's deploy
// checklist must carry the plain migrate (cmd/aveloxis pins it), and a change
// to matviews.sql fails TestMatviewsDefinitionDigestTracksTheFile until this
// constant and the digest below are moved to the release that ships it.
// v0.29.57 changed explorer_libyear_summary's ordering (NULLS LAST).
const MatviewsDefinitionVersion = "0.29.57"

// MatviewsDefinitionDigest is the sha256 of matviews.sql as shipped by
// MatviewsDefinitionVersion (updated alone for a removal-only change).
const MatviewsDefinitionDigest = "8fb0f3d26ac7af0914d8b6c7d35d060196cdcf1dc6f8fdfd39fc40880f96c5f0"

// MatviewsSQLDigest is the sha256 of the embedded matviews.sql, hex-encoded.
func MatviewsSQLDigest() string {
	sum := sha256.Sum256([]byte(matviewsSQL))
	return hex.EncodeToString(sum[:])
}
