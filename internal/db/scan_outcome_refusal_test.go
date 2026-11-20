// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The refusal of an unknown scan outcome names every outcome that is
// accepted (review round 2: it listed five after a sixth was added).
func TestTheScanOutcomeRefusalNamesEveryOutcome(t *testing.T) {
	err := (&PostgresStore{}).RecordScanCoverage(context.Background(), 1, model.ScanCoverage{Outcome: "no-such-outcome"})
	if err == nil {
		t.Fatal("an unknown outcome was accepted")
	}
	for _, o := range []string{model.ScanSent, model.ScanNothingToSend, model.ScanUnsendable, model.ScanUnanswered, model.ScanInventoryIncomplete, model.ScanFailed} {
		if !scanOutcomes[o] {
			t.Errorf("%q is not an accepted outcome", o)
		}
		if !strings.Contains(err.Error(), o) {
			t.Errorf("the refusal does not name %q: %v", o, err)
		}
	}
	if len(scanOutcomes) != 6 {
		t.Errorf("%d accepted outcomes; this test names six: add the new one here and to docs/schema.md and schema.sql's comment", len(scanOutcomes))
	}
}
