// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// Closing review W5: the per-IP daily quota resets at 00:00 UTC (its count
// is keyed by the UTC date), so its Retry-After says when — not a flat
// 86,400 seconds — through the one rounding rule.
func TestPerIPDailyRefusalRetriesAtUTCMidnight(t *testing.T) {
	h, rl := tokenChain(t, &fakeSessionStore{}, Options{RateLimitRPS: 1000, RateLimitBurst: 1000, RateLimitDaily: 1})
	now := time.Date(2026, 10, 10, 23, 0, 0, 0, time.UTC)
	rl.now = func() time.Time { return now }
	h.ServeHTTP(httptest.NewRecorder(), tokenReq("203.0.113.40:1", ""))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, tokenReq("203.0.113.40:1", ""))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") != "3600" {
		t.Fatalf("past the daily quota at 23:00 UTC = %d, Retry-After %q; want 429 and 3600", w.Code, w.Header().Get("Retry-After"))
	}
}
