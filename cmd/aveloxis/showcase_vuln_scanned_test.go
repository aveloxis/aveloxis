// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// TestShowcaseVulnScannedEvidence — review round 6 F2: any current finding
// proves a scan ran (pre-v0.28.1 rows carry no stamp), unknown-version
// advisories included; without them a repository with only those read
// "scan pending".
func TestShowcaseVulnScannedEvidence(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		stamp *time.Time
		c     db.VulnClassCounts
		want  bool
	}{
		{nil, db.VulnClassCounts{}, false},
		{&now, db.VulnClassCounts{}, true},
		{nil, db.VulnClassCounts{Exposure: 1}, true},
		{nil, db.VulnClassCounts{UnknownVersion: 1}, true},
	} {
		if got := showcaseVulnScanned(tc.stamp, tc.c); got != tc.want {
			t.Errorf("showcaseVulnScanned(%v, %+v) = %v, want %v", tc.stamp != nil, tc.c, got, tc.want)
		}
	}
}
