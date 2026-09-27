// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scripts

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestIsUserAdminErrorsAreNeverDiscarded is the ratchet for worklist
// follow-up 6 (the PR #207 review): `isAdmin, _ := IsUserAdmin(...)` sent an
// admin's add to the approval queue when the lookup failed, silently. A
// lookup error is not "no" (SR-5): every non-test call binds the error and
// handles it — the add paths return it, the login logs it and creates a
// non-admin session, the API answers 503. What THIS test enforces is only
// the call line: it bans the two shapes that read an error as "no" there —
// the discard (`, _ :=`) and the fused test (`err == nil && admin`). A
// bound-and-ignored error, or a call split over lines, passes it; each
// site's handling is pinned by its own test
// (TestAddReposToGroupReturnsAdminLookupError for the two add paths,
// TestLoginLogsAFailedAdminLookup for the login,
// TestResolveTokenDistinguishesAStoreErrorFromABadToken for the API).
func TestIsUserAdminErrorsAreNeverDiscarded(t *testing.T) {
	examined := 0
	for _, dir := range []string{"internal/db", "internal/web", "internal/api", "cmd/aveloxis"} {
		for name, src := range srctest.PackageFiles(t, dir, 1) {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			for i, line := range strings.Split(srctest.StripGoComments(src), "\n") {
				if !strings.Contains(line, "IsUserAdmin(") || strings.Contains(line, "func (") || strings.Contains(line, "IsUserAdmin(ctx context.Context") {
					continue
				}
				examined++
				if strings.Contains(line, ", _ :=") || strings.Contains(line, ", _ =") || strings.Contains(line, "err == nil &&") {
					t.Errorf("%s:%d reads IsUserAdmin's error as \"not an admin\" (SR-5); bind the error and handle it on its own arm", name, i+1)
				}
			}
		}
	}
	srctest.MinCount(t, "IsUserAdmin call sites", examined, 4)
}
