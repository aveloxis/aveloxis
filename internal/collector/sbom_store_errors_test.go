// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// TestSBOMGenerationDiscardsNoStoreError — SR-5 (a lookup ERROR is not
// "no data"): every store read behind an SBOM returns its error. The
// scancode evidence read discarded it (`scanData, _ :=`), so a transient
// database error produced a document without the concluded license and
// copyrights that nothing explained — and since v0.29.73 the API keeps an
// SBOM until the repository changes, so that degraded document would be
// served until the next scancode run (review round 1, finding 3). Never
// scanned is not an error (GetScancodeForSBOM answers it with an empty
// result).
func TestSBOMGenerationDiscardsNoStoreError(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/sbom.go"), "func GenerateSBOMWithOptions("))
	reads := regexp.MustCompile(`store\.\w+\(`).FindAllString(body, -1)
	srctest.MinCount(t, "store reads in GenerateSBOMWithOptions", len(reads), 5)
	if m := regexp.MustCompile(`, _ :?= store\.\w+\(|_ = store\.\w+\(`).FindString(body); m != "" {
		t.Errorf("GenerateSBOMWithOptions discards a store error (%s): return it", strings.TrimSpace(m))
	}
}

// The collection step's SBOM generation failure is logged where an operator
// sees it (review round 2: since the scancode lookup error became fatal, a
// transient database error stored no SBOM and said so only at Debug).
// Shutdown and a repository deleted mid-collection stay quiet.
func TestSBOMCollectionStepLogsGenerationFailures(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t, srctest.Read(t, "internal/collector/sbom.go"), "func GenerateAndStoreSBOMs("))
	i := strings.Index(body, "GenerateSBOM(ctx, store, repoID, spec.format)")
	if i < 0 {
		t.Fatal("cannot find the generation call")
	}
	arm := body[i:]
	if j := strings.Index(arm, "continue"); j >= 0 {
		arm = arm[:j]
	}
	if !strings.Contains(arm, "logger.Warn(") {
		t.Error("a failed SBOM generation in the collection step must be logged at Warn")
	}
	for _, quiet := range []string{"context.Canceled", "db.ErrRepoNotFound"} {
		if !strings.Contains(arm, quiet) {
			t.Errorf("the generation-failure arm must keep %s quiet", quiet)
		}
	}
}
