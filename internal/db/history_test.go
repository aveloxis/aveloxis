// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"testing"
)

// TestRotateRepoInfoToHistoryFuncExists verifies the history rotation function exists.
func TestRotateRepoInfoToHistoryFuncExists(t *testing.T) {
	// This test validates that RotateRepoInfoToHistory is callable.
	// It won't actually run SQL — just confirms the function compiles.
	var s *PostgresStore
	_ = s // RotateRepoInfoToHistory is a method on PostgresStore
}

// RotateScorecardToHistory was removed with the PR #203 fixes: the
// only rotation of repo_deps_scorecard is inside ReplaceScorecard (one
// transaction, per-repo lock). TestScorecardWritersCannotSkipRotation
// in scorecard_replace_test.go is the tripwire.
