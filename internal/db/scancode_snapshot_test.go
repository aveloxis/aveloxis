// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The scancode snapshot is replaced in ONE transaction (v0.28.19).
//
// The ingest used to rotate, then insert the scan row, then insert the
// file rows — three independent transactions, guarded only by a ctx
// check BEFORE the first. That guard prevents STARTING the sequence
// under a done ctx and does nothing about a failure landing between
// them; the window is widest across the per-finding file inserts. The
// repo was then left with its previous snapshot rotated away and
// nothing current, until a re-scan 180 days later. Same fix
// ReplaceRepoLaborSnapshot got in v0.27.7.
func TestScancodeSnapshotReplacedInOneTransaction(t *testing.T) {
	src := srctest.Read(t, "internal/db/scancode_store.go")
	body := srctest.StripGoComments(srctest.FuncBody(t, src, "func (s *PostgresStore) ReplaceScancodeSnapshot("))

	for _, needle := range []string{"s.pool.Begin(ctx)", "defer tx.Rollback(ctx)", "tx.Commit(ctx)"} {
		if !strings.Contains(body, needle) {
			t.Errorf("ReplaceScancodeSnapshot must own one transaction — missing %q. Without it the rotation can commit while an insert fails, which is the half-applied snapshot this function exists to prevent.", needle)
		}
	}
	if !strings.Contains(body, "rotateScancodeRows(ctx, tx, repoID)") {
		t.Error("the rotation must run inside ReplaceScancodeSnapshot's OWN transaction, through the shared rotateScancodeRows helper — calling the exported RotateScancodeToHistory would open a second transaction and restore the window")
	}
	if strings.Contains(body, "s.pool.Exec") || strings.Contains(body, "s.pool.QueryRow") {
		t.Error("every write in ReplaceScancodeSnapshot must go through tx, not the pool — a pool write commits independently of the rotation")
	}
}

// The ingest must use the fused operation, not the three separate
// store calls it replaced.
func TestIngestUsesTheFusedSnapshotReplace(t *testing.T) {
	body := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/collector/scancode.go"), "func ingestScancodeOutput("))
	if !strings.Contains(body, "store.ReplaceScancodeSnapshot(") {
		t.Error("ingestScancodeOutput must call store.ReplaceScancodeSnapshot")
	}
	// Round 8: banning the three calls inside ONE function body was
	// scoped too narrowly — a new call site anywhere else would reopen
	// the half-applied-snapshot window. The three writers are DELETED
	// instead (remove-don't-deprecate), so this asserts they cannot
	// come back as exported store methods at all.
	dbSrc := srctest.Read(t, "internal/db/scancode_store.go") + srctest.Read(t, "internal/db/history.go")
	for _, banned := range []string{
		"func (s *PostgresStore) RotateScancodeToHistory(",
		"func (s *PostgresStore) InsertScancodeScan(",
		"func (s *PostgresStore) InsertScancodeFileResultBatch(",
	} {
		if strings.Contains(dbSrc, banned) {
			t.Errorf("%s is back — the rotate-then-insert sequence is what left a rotated-but-unwritten "+
				"snapshot when anything failed in the middle; ReplaceScancodeSnapshot is the only writer", banned)
		}
	}
}

// One copy of the rotation statements (SR-17). Round 8 deleted the
// exported RotateScancodeToHistory once the fusion left it with zero
// production callers, so the "both callers delegate" half of this pin
// is gone with it — what remains is that the four-statement rotation
// has exactly ONE definition, and that it is the unexported helper the
// fused writer calls inside its transaction.
func TestRotationStatementsHaveOneDefinition(t *testing.T) {
	src := srctest.Read(t, "internal/db/history.go")
	if !strings.Contains(src, "func rotateScancodeRows(ctx context.Context, tx pgx.Tx, repoID int64) error") {
		t.Fatal("rotateScancodeRows must exist as the single definition of the scancode rotation (SR-17)")
	}
	if strings.Count(src, "scancode_file_results_history") != 1 {
		t.Errorf("the scancode rotation statements appear %d times in history.go, want 1 — a second copy is the drift SR-17 exists to prevent", strings.Count(src, "scancode_file_results_history"))
	}
	// It must run inside the fused writer's transaction, not open its own.
	fused := srctest.StripGoComments(srctest.FuncBody(t,
		srctest.Read(t, "internal/db/scancode_store.go"), "func (s *PostgresStore) ReplaceScancodeSnapshot("))
	if !strings.Contains(fused, "rotateScancodeRows(ctx, tx, repoID)") {
		t.Error("ReplaceScancodeSnapshot must rotate via rotateScancodeRows on its OWN tx — a rotation that commits separately reopens the half-applied-snapshot window")
	}
}
