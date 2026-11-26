// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"

	"github.com/aveloxis/aveloxis/internal/model"
)

// LockfileReading is how one analysis's reading of a repository's lockfiles
// ended (0.29.214). It is the one record of that, made in one place
// (AnalysisCollector.lockfileReadingEnded), for whatever keeps it: the
// coverage record is to say when the lockfiles were last read and why the
// last reading did not finish.
//
// Exactly one of the two is set:
//
//   - Completed: the lockfiles were read and the stored snapshot was
//     replaced with what they hold. Lockfiles says whether the clone has
//     any (a parsed one or one that failed to parse); false is a repository
//     with no lockfile, for which there is nothing to be behind on.
//   - NotCompleted: they were not read, and the stored rows stand as they
//     were. The value says why, in the words the inventory's own failed
//     attempts use: model.InventoryAttemptReadCPULimit,
//     InventoryAttemptReadMemoryLimit, InventoryAttemptReadTimedOut or
//     InventoryAttemptReadFailed, or InventoryAttemptRootUnreadable when the
//     clone's root itself could not be read.
//
// A shutdown is neither, and is not recorded. Nor is a reading that
// finished whose snapshot could not then be stored: the store's error is
// the phase's, and the rows are as old as they were.
//
// The type is model's, so that the store can keep it without importing this
// package. For a reading that did not complete, Lockfiles is true only when
// the clone is KNOWN to hold a lockfile (see lockfilesNotRead).
type LockfileReading = model.LockfileReading

// LockfileReadingRecorder keeps a LockfileReading. When the analysis's
// store (*db.PostgresStore) has this method it is told; AnalysisCollector's
// LockfileReadings field, when set, is told instead. The store keeps it on
// the coverage record (lockfiles_read_at, lockfiles_unread_reason), from
// which the repository's pages draw their caution.
type LockfileReadingRecorder interface {
	RecordLockfileReading(ctx context.Context, repoID int64, reading LockfileReading) error
}

// lockfilesNotRead is the reading for lockfiles that went unread for the
// given reason, by what the name pass learned of the clone before the reader
// stopped:
//
//   - it listed the clone's lockfiles and there are some: not completed,
//     with lockfiles known to be there;
//   - it listed them and there are NONE: there was nothing to read, which is
//     a completed reading of a repository with no lockfile (a caution about
//     lockfiles that do not exist would be false, and one left by an earlier
//     reading must be cleared);
//   - it did not get that far: not completed, and whether the clone holds a
//     lockfile is not known.
func lockfilesNotRead(reason string, locks lockfileSet) LockfileReading {
	switch {
	case !locks.PresentKnown:
		return LockfileReading{NotCompleted: reason}
	case len(locks.Present) == 0:
		return LockfileReading{Completed: true}
	}
	return LockfileReading{NotCompleted: reason, Lockfiles: true}
}

// lockfileReadingEnded is the one place an analysis says how its reading of
// the lockfiles ended.
func (ac *AnalysisCollector) lockfileReadingEnded(ctx context.Context, repoID int64, reading LockfileReading) {
	rec := ac.LockfileReadings
	if rec == nil {
		var ok bool
		if rec, ok = any(ac.store).(LockfileReadingRecorder); !ok {
			return
		}
	}
	err := rec.RecordLockfileReading(ctx, repoID, reading)
	if err == nil {
		return
	}
	if errors.Is(err, context.Canceled) {
		// A `stop serve`: not a failure, and said all the same, since the
		// coverage record now says nothing of this reading (0.29.215).
		ac.logger.Warn("how the reading of the lockfiles ended was not recorded: the analysis was stopped first", "repo_id", repoID)
		return
	}
	ac.logger.Warn("how the reading of the lockfiles ended could not be recorded", "repo_id", repoID, "error", err)
}
