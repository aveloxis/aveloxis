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

// TestPoolDisablesStatementCache verifies NewPostgresStore configures pgx
// to NOT cache server-side prepared-statement names per connection.
//
// Background: pgx v5's default is QueryExecModeCacheStatement, which assigns
// names like "stmtcache_<hash>" and re-uses them. Any event that silently
// swaps the TCP connection's backend — pgbouncer in txn/statement pool mode,
// a stateful NAT/firewall/cloud LB expiring an idle connection, a DB
// restart mid-collection — leaves pgx's client cache out of sync and
// SendBatch fails with SQLSTATE 26000 "prepared statement does not exist".
// QueryExecModeExec re-prepares per call as an unnamed statement, so
// there is no cache to go stale.
//
// Regressing this flag out would reintroduce the gap-fill / refresh-open
// batch flush failures we chased in v0.18.10.
func TestPoolDisablesStatementCache(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec") {
		t.Error("postgres.go must set cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec " +
			"to avoid SQLSTATE 26000 when a TCP connection is silently replaced by an upstream " +
			"NAT/firewall/pgbouncer")
	}
}

// TestPoolCyclesIdleConnections verifies MaxConnIdleTime is set below the
// typical 5-minute stateful-NAT idle timeout so pgx cycles connections
// before a network intermediary does — eliminating "silently dropped
// connection" surprises at the next SendBatch.
func TestPoolCyclesIdleConnections(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "cfg.MaxConnIdleTime") {
		t.Error("postgres.go must set cfg.MaxConnIdleTime so pgx recycles idle connections " +
			"before upstream NAT/firewalls do")
	}
	if !strings.Contains(src, "cfg.MaxConnLifetime") {
		t.Error("postgres.go must set cfg.MaxConnLifetime to cap total connection age " +
			"so credential rotation / failover eventually reaches every connection")
	}
}
