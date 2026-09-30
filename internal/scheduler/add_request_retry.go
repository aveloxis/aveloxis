// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// hourlyMaintenanceInterval is the period of serve's hourly maintenance
// tick (staging cleanup, the transaction-ID line, the add-request retry).
// The retry also uses it as its minimum age, so a request is retried only
// after a whole tick in which the pass that approved it could finish.
const hourlyMaintenanceInterval = time.Hour

// retryApprovedAddRequests re-runs the approved add requests whose pass
// stopped with items unprocessed (O17, v0.29.71): nothing else would — the
// admin page lists pending requests only, so no administrator sees an
// approved request to re-approve (re-approving would resume it). It is the ordinary approved pass, so a retryable failure
// leaves its item for the next tick (nothing is dropped) and a permanent
// one is stamped processed-with-error as always. One WARN per tick names
// the requests still unfinished, each with its own error.
func (s *Scheduler) retryApprovedAddRequests(ctx context.Context) {
	ids, err := s.store.ApprovedAddRequestsToRetry(ctx, hourlyMaintenanceInterval)
	if errors.Is(err, context.Canceled) {
		return // a stop, not a failure
	}
	if err != nil {
		s.logger.Warn("approved add requests could not be listed for retry", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}
	processed, failed := 0, 0
	var unfinished []int64
	// One "id: error" per unfinished request (whole-branch review D4): the
	// pass returns its error without logging it, so a single last_error
	// lost every other request's cause. Bounded by the listing, like the
	// ids beside it.
	var causes []string
	for _, id := range ids {
		p, f, err := s.store.ProcessApprovedAddRequest(ctx, id)
		processed += p
		failed += f
		if errors.Is(err, context.Canceled) {
			return
		}
		if errors.Is(err, db.ErrAddRequestInProgress) {
			continue // a pass in this process is finishing it
		}
		if errors.Is(err, db.ErrGroupRejected) {
			continue // rejected between the listing and the pass: nothing to retry
		}
		if err != nil {
			unfinished = append(unfinished, id)
			causes = append(causes, fmt.Sprintf("%d: %v", id, err))
		}
	}
	if len(unfinished) > 0 {
		s.logger.Warn("approved add requests retried — some items are still unprocessed and are retried next hour",
			"requests", len(ids), "processed", processed, "failed", failed,
			"unfinished_request_ids", unfinished, "errors", causes)
		return
	}
	s.logger.Info("approved add requests retried",
		"requests", len(ids), "processed", processed, "failed", failed)
}
