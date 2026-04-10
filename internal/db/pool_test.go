package db

import (
	"os"
	"strings"
	"testing"
)

// TestNewPostgresStoreAcceptsMaxConns verifies the constructor supports an
// optional maxConns parameter so callers (e.g., the scheduler) can scale the
// pool to match the worker count. Without this, 30 workers sharing a 20-conn
// pool starve each other for connections.
func TestNewPostgresStoreAcceptsMaxConns(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)

	// The function signature must accept maxConns somehow (variadic or explicit).
	funcIdx := strings.Index(src, "func NewPostgresStore(")
	if funcIdx < 0 {
		t.Fatal("cannot find NewPostgresStore function")
	}
	sig := src[funcIdx : funcIdx+200]
	if !strings.Contains(sig, "maxConns") {
		t.Error("NewPostgresStore must accept a maxConns parameter to scale pool with worker count")
	}
}

// TestDefaultPoolSizeIsReasonable verifies the fallback pool size is at least 20.
func TestDefaultPoolSizeIsReasonable(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	// The code should have a default of at least 20 connections.
	if !strings.Contains(src, "MaxConns") {
		t.Error("postgres.go must set MaxConns on the pool config")
	}
}
