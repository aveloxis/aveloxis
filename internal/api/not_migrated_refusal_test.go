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
	code, msg := refusalAnswer(admin.Refusal{Kind: admin.NotMigrated, Detail: "it has no aveloxis_data.repo_gone_history"})
	if code != http.StatusServiceUnavailable || !strings.Contains(msg, "not been migrated") || !strings.Contains(msg, "deploy-checklist") {
		t.Errorf("not migrated = %d %q; want 503 with the deploy advice", code, msg)
	}
	// A fixed body: the store's operator text (schema objects) is not sent.
	if strings.Contains(msg, "aveloxis_data") {
		t.Errorf("the API body names schema objects: %q", msg)
	}
}
