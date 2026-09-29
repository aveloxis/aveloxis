// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
)

// TestStampFailures — old problem O2: the sender and contributor search
// resolvers discarded every attempt-stamp error. A failed stamp is
// harmless (the candidate is retried next pass) but was invisible; the
// counter reports one WARN per pass with the count and the first error,
// and a shutdown's cancellations are not failures.
func TestStampFailures(t *testing.T) {
	var logs strings.Builder
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	var f stampFailures
	f.record(nil)
	f.record(fmt.Errorf("stamp: %w", context.Canceled))
	f.warn(logger, "mailing-list sender", 10)
	if logs.Len() != 0 {
		t.Errorf("no failures (nil, a cancellation) must log nothing:\n%s", logs.String())
	}
	f.record(errors.New("conn refused"))
	f.record(errors.New("timeout"))
	f.warn(logger, "mailing-list sender", 10)
	l := logs.String()
	if strings.Count(l, "level=WARN") != 1 || !strings.Contains(l, "failed=2") || !strings.Contains(l, "of=10") || !strings.Contains(l, "conn refused") {
		t.Errorf("two failures must be one WARN with the count and the first error:\n%s", l)
	}
}
