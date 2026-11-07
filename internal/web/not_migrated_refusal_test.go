// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package web

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
)

// Copilot PR #252, fourteenth review: an adoption on a database behind the
// binary (the web process does not migrate) answers 503 with the deploy
// advice — a temporary condition of the service, not the caller's request,
// and not a 500.
func TestRefusePageAnswersNotMigratedWith503(t *testing.T) {
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/admin/forge-id-changes/1/adopt", nil)
	err := fmt.Errorf("adopt: %w: it has no aveloxis_data.repo_gone_history", db.ErrSchemaNotMigrated)
	if !(&Server{}).refusePage(w, r, err) {
		t.Fatal("refusePage did not classify db.ErrSchemaNotMigrated")
	}
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "deploy-checklist") || strings.Contains(w.Body.String(), "aveloxis_data") {
		t.Errorf("not migrated = %d %q; want 503 with the deploy advice and no schema object", w.Code, strings.TrimSpace(w.Body.String()))
	}
}
