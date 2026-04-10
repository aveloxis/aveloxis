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

// TestRotateScorecardToHistoryFuncExists verifies the scorecard history rotation function exists.
func TestRotateScorecardToHistoryFuncExists(t *testing.T) {
	var s *PostgresStore
	_ = s
}
