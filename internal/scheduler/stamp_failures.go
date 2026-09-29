// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"log/slog"
)

// stampFailures counts the attempt stamps a resolver pass could not write
// (old problem O2: they were discarded). A failed stamp only means the
// candidate is retried next pass, so it is reported once per pass — the
// count and the first error — not once per candidate; a shutdown's
// cancellations are not failures.
type stampFailures struct {
	n     int
	first error
}

func (f *stampFailures) record(err error) {
	if err == nil || errors.Is(err, context.Canceled) {
		return
	}
	f.n++
	if f.first == nil {
		f.first = err
	}
}

func (f *stampFailures) warn(logger *slog.Logger, what string, of int) {
	if f.n == 0 {
		return
	}
	logger.Warn(what+" attempt stamps could not be written — those candidates are retried next pass",
		"failed", f.n, "of", of, "first_error", f.first)
}
