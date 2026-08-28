// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"encoding/json"
	"reflect"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// ScancodeStore is the database surface the scancode worker uses —
// every method, and nothing else.
//
// v0.28.19. The worker held a concrete *db.PostgresStore, which meant
// its behaviour could only ever be asserted by reading its source.
// That is how the shutdown contract came to be pinned by an AST
// tripwire that reviewers escaped in six consecutive passes (43–48),
// and why pass 49 could runtime-test the extracted awaitBookkeeping
// but not Run itself. The DistributionWorker has had exactly this
// shape since v0.24.0; this brings scancode into line, so the paths
// that only touch the DB — the shutdown lock clear, the strike and
// timeout bookkeeping, the health gate — can be driven by a fake and
// OBSERVED rather than described.
//
// Deliberately not an interface over the whole store: a narrow one
// documents what this subsystem may do, and widening it is a visible
// decision rather than an accident.
type ScancodeStore interface {
	// Claim + lock lifecycle.
	ClaimNextScancodeRepo(ctx context.Context, cadence, staleLockWindow time.Duration) (*db.ScancodeJob, error)
	RecordScancodeLockState(ctx context.Context, repoID int64, pid int, bootID, outputPath, host string) error
	ClearScancodeLock(ctx context.Context, repoID int64) error
	ClearStaleNullPidLocks(ctx context.Context, olderThan time.Duration) (int64, error)
	ListLockedScancodeRows(ctx context.Context) ([]db.ScancodeLockedRow, error)

	// Outcome bookkeeping. The distinction between these three is the
	// v0.23.8 contract: a wall-clock timeout stretches the next
	// attempt, a real failure counts toward the 10-strike sideline,
	// and a shutdown is neither (passes 36/37).
	MarkScancodeComplete(ctx context.Context, repoID int64, version string) error
	MarkScancodeSkipped(ctx context.Context, repoID int64, reason string) error
	RecordScancodeFailure(ctx context.Context, repoID int64) error
	RecordScancodeTimeout(ctx context.Context, repoID int64, sideline bool) error

	// Toolchain health, surfaced to operators in aveloxis_ops.aveloxis_status.
	SetAveloxisStatus(ctx context.Context, statusName, status, detail, dataSource string) error

	// Ingest of a completed scan's JSON output.
	RotateScancodeToHistory(ctx context.Context, repoID int64) error
	InsertScancodeScan(ctx context.Context, repoID int64, scancodeVersion string, filesScanned, filesWithFindings int, durationSecs float64, scanErrors json.RawMessage) (int64, error)
	InsertScancodeFileResultBatch(ctx context.Context, repoID int64, rows []*db.ScancodeFileRow) error
}

// The production implementation, checked at compile time so a
// signature change in db is a build error here rather than a runtime
// surprise at the one call site that uses it.
var _ ScancodeStore = (*db.PostgresStore)(nil)

// storeAvailable reports whether the worker has a usable store.
//
// A plain `w.store == nil` was correct while the field was a concrete
// *db.PostgresStore, and is a trap now: an interface holding a typed
// nil pointer is itself NON-nil, so the guard would pass and the first
// call would panic. That hazard is introduced BY the interface, so it
// is handled here rather than noted in a comment (v0.28.19).
func (w *ScancodeWorker) storeAvailable() bool {
	if w.store == nil {
		return false
	}
	v := reflect.ValueOf(w.store)
	return v.Kind() != reflect.Pointer || !v.IsNil()
}
