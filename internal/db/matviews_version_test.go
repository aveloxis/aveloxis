// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"
)

// TestMatviewsDefinitionDigestTracksTheFile is worklist item 36, option 2
// (operator decision 2026-09-26): the standard deploy ladder (`migrate
// --skip-views` → `refresh-views`) never applies a changed materialized-view
// DEFINITION — only a plain `aveloxis migrate` re-creates the 8Knot views
// from matviews.sql. v0.29.57's `NULLS LAST` ordering would have shipped
// under the ladder without ever reaching production. So matviews.sql is
// pinned by digest: a change to it fails here until MatviewsDefinitionVersion
// names the release that ships it, whose deploy checklist must then carry a
// plain migrate (cmd/aveloxis's TestMatviewsChangeIsDeployedByAPlainMigrate).
func TestMatviewsDefinitionDigestTracksTheFile(t *testing.T) {
	if got := MatviewsSQLDigest(); got != MatviewsDefinitionDigest {
		t.Errorf("matviews.sql changed (sha256 %s, pinned %s). A changed or added VIEW DEFINITION ships only through a plain `aveloxis migrate`: set MatviewsDefinitionVersion to this release (ToolVersion %s), MatviewsDefinitionDigest to the new digest, and give that version's deploy checklist a plain `aveloxis migrate` step (worklist item 36). A removal-only change updates the digest alone and says so in the changelog.",
			got, MatviewsDefinitionDigest, ToolVersion)
	}
	if MatviewsDefinitionVersion == "" || MatviewsDefinitionDigest == "" {
		t.Error("MatviewsDefinitionVersion and MatviewsDefinitionDigest must both be set")
	}
	// A core.autocrlf checkout hashes the same (review round 1).
	crlf := strings.ReplaceAll(matviewsSQL, "\n", "\r\n")
	if crlf == matviewsSQL {
		t.Fatal("the fixture has no line endings to fold")
	}
	if sqlDigest(crlf) != MatviewsDefinitionDigest {
		t.Error("a CRLF checkout of matviews.sql hashes differently — the digest must fold line endings")
	}
}
