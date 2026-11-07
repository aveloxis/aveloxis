// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/admin"
)

// Copilot PR #252, fourteenth review: the api process does not migrate; an
// adoption on a database behind the binary answers 503 with the deploy
// advice, as the route's spec says.
func TestRefusalAnswerForNotMigratedIs503(t *testing.T) {
	code, msg := refusalAnswer(admin.Refusal{Kind: admin.NotMigrated, Detail: "the database schema is behind this binary: run the steps"})
	if code != http.StatusServiceUnavailable || !strings.Contains(msg, "behind this binary") {
		t.Errorf("not migrated = %d %q; want 503 with the detail", code, msg)
	}
}
