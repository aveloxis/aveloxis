// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestAPICacheMaxAgeFollowsTheEnrichmentInterval — SR-10, end to end from
// the JSON: the API's per-repository cache (O11 option 4, v0.29.71) may
// reuse an answer within a collection generation for at most one contributor
// enrichment interval, the cadence at which what it shows can change outside
// a collection. The default layer is the config accessor's (30 min).
func TestAPICacheMaxAgeFollowsTheEnrichmentInterval(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	for json, want := range map[string]time.Duration{
		`{"collection": {"enrich_interval_minutes": 45}}`: 45 * time.Minute,
		`{}`: 30 * time.Minute,
	} {
		cfg := loadConfigJSON(t, json)
		if got := apiOptions(cfg, logger).ResponseCacheMaxAge; got != want {
			t.Errorf("%s: ResponseCacheMaxAge = %v, want %v", json, got, want)
		}
	}
}
