// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import "testing"

// ResetResponseSizeMarksForTest forgets the named sources' maximums for one
// test and restores them after it. TEST-ONLY seam (like
// SetGraphQLSleepForTest): the marks are process-wide, so a repeat run
// (-count), a shuffled order, or an earlier test in another package's
// binary that set a mark first left a test seeing no new maximum — it
// failed, or (the deps.dev 404 test, fix review V2) passed vacuously.
func ResetResponseSizeMarksForTest(t testing.TB, sources ...string) {
	t.Helper()
	responseSizeMu.Lock()
	saved := map[string]int64{}
	for _, s := range sources {
		if v, ok := responseSizeMax[s]; ok {
			saved[s] = v
		}
		delete(responseSizeMax, s)
	}
	responseSizeMu.Unlock()
	t.Cleanup(func() {
		responseSizeMu.Lock()
		defer responseSizeMu.Unlock()
		for _, s := range sources {
			delete(responseSizeMax, s)
			if v, ok := saved[s]; ok {
				responseSizeMax[s] = v
			}
		}
	})
}
