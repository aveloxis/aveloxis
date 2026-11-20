// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// packageKeyFillPause is the rest between two steps of the package-key fill.
// With the default step of 1,000 rows it bounds the fill at 1,000 single-row
// updates by primary key per second, which a collecting fleet does not
// notice; ten million rows take under three hours.
const packageKeyFillPause = time.Second

// packageKeyFillStore is the store half of the loop.
type packageKeyFillStore interface {
	FillPackageKeysStep(ctx context.Context, table db.PackageKeyFillTable, batch int) (bool, error)
	GetPackageKeyFillProgress(ctx context.Context, table db.PackageKeyFillTable) (db.PackageKeyFillProgress, error)
}

// runPackageKeyFill gives a package key to the inventory and lockfile rows
// written before 0.29.172 (summary/70, 4.2). Spawned once by Run; it walks
// each table to its end and exits. A stopped serve resumes at the stored
// cursor, and a later start with both fills complete does nothing: each
// table's first step answers "done" from its marker.
//
// rows is collection.package_key_fill_rows (the rows of one step); 0 turns
// the fill off. A failed step ends this run; the next start resumes it.
func runPackageKeyFill(ctx context.Context, store packageKeyFillStore, logger interface {
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
}, rows int, pause time.Duration) {
	if rows <= 0 {
		logger.Info("package-key fill is off (collection.package_key_fill_rows is 0)")
		return
	}
	for _, table := range db.PackageKeyFillTables {
		steps := 0
		for {
			if ctx.Err() != nil {
				return // shutdown, not a failure
			}
			done, err := store.FillPackageKeysStep(ctx, table, rows)
			if errors.Is(err, context.Canceled) || ctx.Err() != nil {
				return
			}
			if err != nil {
				// Not retried here (declined, review round 1). The step
				// already retries what retrying can cure (the store's
				// withRetry: deadlocks, serialization failures, a dropped
				// connection). An error that comes back through that is
				// one a loop here would only repeat, and log, for as long
				// as serve runs. The fill is a convenience for the fleet
				// readers, which work without it; it resumes at its cursor
				// at the next start.
				logger.Warn("package-key fill: a step failed; the fill resumes at its cursor at the next start",
					"table", string(table), "error", err)
				return
			}
			if done {
				break
			}
			steps++
			select {
			case <-ctx.Done():
				return
			case <-time.After(pause):
			}
		}
		// Reported only by the run that did work: a start that finds the
		// fill complete says nothing.
		if steps == 0 {
			continue
		}
		p, err := store.GetPackageKeyFillProgress(ctx, table)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Warn("package-key fill: complete, but its marker could not be read", "table", string(table), "error", err)
			continue
		}
		logger.Info("package-key fill complete", "table", string(table),
			"rows_keyed_or_already_keyed", p.Filled, "rows_that_cannot_be_keyed", p.Unkeyable, "rows_per_step", rows)
	}
}
