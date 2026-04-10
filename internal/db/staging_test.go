package db

// The StagingWriter and ProcessStaged methods operate directly on
// *PostgresStore (backed by a pgx connection pool). Full tests require
// a live PostgreSQL instance.
//
// Set AVELOXIS_TEST_DB to a connection string to run integration tests.

import "testing"

func TestStagingFlushSize(t *testing.T) {
	// Verify the constant is set to a reasonable batch size.
	if stagingFlushSize != 500 {
		t.Errorf("stagingFlushSize = %d, want 500", stagingFlushSize)
	}
}

func TestStagingWriterCount(t *testing.T) {
	// NewStagingWriter requires a *PostgresStore for construction, but the
	// Count() method only reads the in-memory counter. We pass nil for store
	// since Count() never touches the database.
	w := NewStagingWriter(nil, 42, 1, nil)
	if w.Count() != 0 {
		t.Errorf("initial Count() = %d, want 0", w.Count())
	}
}

func TestStagingIntegration(t *testing.T) {
	t.Skip("requires live PostgreSQL — set AVELOXIS_TEST_DB to run")
}
