// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestEnrichThinContributorsMarksFailuresOnlyWhenDefinitive is the wiring
// pin for chaoss.tv 2026-09-17: ~500 GET /users/{login} lookups failed with
// "exhausted 10 retries … transient" and EnrichThinContributors stamped every
// one enriched (30-day cooldown, no data, Debug only). The error arm must
// consult platform.IsDefinitiveAnswer (its behaviour is pinned by
// TestIsDefinitiveAnswer) and end the iteration before it can reach
// MarkContributorEnriched.
func TestEnrichThinContributorsMarksFailuresOnlyWhenDefinitive(t *testing.T) {
	src := srctest.Read(t, "internal/collector/enrich.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func EnrichThinContributors("))
	once := func(hay, needle string) int {
		t.Helper()
		if n := strings.Count(hay, needle); n != 1 {
			t.Fatalf("%q appears %d times, want exactly 1", needle, n)
		}
		return strings.Index(hay, needle)
	}
	start := once(body, "client.EnrichContributor(ctx, login)")
	errArm := body[start:]
	errArm = errArm[:once(errArm, "enriched = append(")]
	// The gate must be a guard whose body ends the iteration before the
	// mark: `if !platform.IsDefinitiveAnswer(err) {` ... `continue` ...
	// then the only MarkContributorEnriched of the arm.
	gate := once(errArm, "if !platform.IsDefinitiveAnswer(err) {")
	mark := once(errArm, "MarkContributorEnriched(")
	cont := strings.Index(errArm[gate:], "continue")
	if cont < 0 || gate+cont > mark {
		t.Fatal("the non-definitive branch does not end the iteration before MarkContributorEnriched")
	}
}
