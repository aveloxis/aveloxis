// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

// TestPartialErrorsAreAggregated (chaoss.tv log, 2026-09-05): a too-expensive
// contributionsCollection query emits hundreds of per-path INTERNAL errors —
// ~470 such queries produced 87,728 WARNs in 90 minutes. parseGraphQLResponse
// must log ONE aggregate line per response, not one per failed path.
func TestPartialErrorsAreAggregated(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	body := []byte(`{"data":{"user":{}},"errors":[
		{"type":"INTERNAL","path":["user","contributionsCollection","x",0],"message":"boom1"},
		{"type":"INTERNAL","path":["user","contributionsCollection","x",1],"message":"boom2"},
		{"type":"INTERNAL","path":["user","contributionsCollection","y",0],"message":"boom3"}]}`)
	var dst struct {
		User map[string]any `json:"user"`
	}
	if err := parseGraphQLResponse(body, &dst, logger); err != nil {
		t.Fatalf("per-path errors must not fail the query: %v", err)
	}
	log := buf.String()
	// Exactly ONE aggregate line, carrying the count.
	if n := strings.Count(log, "graphql per-path error"); n != 1 {
		t.Errorf("expected exactly 1 aggregate per-path log line, got %d:\n%s", n, log)
	}
	if !strings.Contains(log, "count=3") {
		t.Errorf("the aggregate line must carry the failed-path count; got:\n%s", log)
	}
}
