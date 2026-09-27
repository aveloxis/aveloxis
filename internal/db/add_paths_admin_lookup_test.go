// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestAddReposToGroupReturnsAdminLookupError pins worklist follow-up 6 (the
// PR #207 review) at the two add paths: `isAdmin, _ := s.IsUserAdmin(...)`
// sent an admin's add to the approval queue when the lookup failed. The
// store is concrete, so the pin is the control flow: the lookup binds its
// error and returns it before the admin/non-admin split (SR-5). The
// repo-wide discard ban is scripts/admin_lookup_errors_test.go.
func TestAddReposToGroupReturnsAdminLookupError(t *testing.T) {
	for file, sig := range map[string]string{
		"internal/db/add_requests.go": "func (s *PostgresStore) AddReposToGroup(",
		"internal/db/web_store.go":    "func (s *PostgresStore) AddOrgToGroup(",
	} {
		body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, file), sig))
		i := strings.Index(body, "IsUserAdmin(")
		if i < 0 {
			t.Fatalf("%s: no IsUserAdmin call", sig)
		}
		after := body[i:]
		callLine := body[strings.LastIndex(body[:i], "\n")+1 : i]
		// The call binds its error, and an error arm follows it before the
		// answer is used (the next 200 bytes: the arm is the next statement).
		if strings.Contains(callLine, ", _ :=") || strings.Contains(callLine, ", _ =") || !strings.Contains(after[:min(len(after), 200)], "err != nil") {
			t.Errorf("%s must bind IsUserAdmin's error and return it before deciding the admin path", sig)
		}
	}
}
