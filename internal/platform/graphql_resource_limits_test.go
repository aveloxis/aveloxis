// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package platform

// graphql_resource_limits_test.go — pins the v0.27.81
// ErrResourceLimits sentinel. Subdivision callers
// (FetchContributorActivity) gate on errors.Is(err, ErrResourceLimits)
// rather than the broad ClassTransient, because RLE is the only
// in-body GraphQL condition where halving the query provably helps —
// 500-exhausted and RATE_LIMITED keep fail-and-retry semantics.

import (
	"errors"
	"io"
	"log/slog"
	"testing"
)

func TestResourceLimitsErrorWrapsSentinel(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	body := []byte(`{"data":{"u0":null},"errors":[
		{"type":"RESOURCE_LIMITS_EXCEEDED","path":["u0","contributionsCollection"],"message":"Resource limits for this query exceeded."}
	]}`)
	err := parseGraphQLResponse(body, nil, logger)
	if err == nil {
		t.Fatal("RLE body must produce an error")
	}
	if !errors.Is(err, ErrResourceLimits) {
		t.Errorf("RLE error must wrap ErrResourceLimits for subdivision gating, got %v", err)
	}
	if ClassifyError(err) != ClassTransient {
		t.Errorf("RLE must classify ClassTransient, got %v", ClassifyError(err))
	}
}
