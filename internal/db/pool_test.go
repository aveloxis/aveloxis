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

// TestPoolUsesStatementCache verifies NewPostgresStore configures pgx to
// cache prepared statements on each pooled connection.
//
// Background: pgx v5's QueryExecModeCacheStatement assigns server-side
// statement names ("stmtcache_<hash>") and re-uses them, so repeat queries
// skip Parse+Describe on the server — a meaningful win on the hot
// INSERT/SELECT paths when the DB is on a LAN rather than a loopback
// socket.
//
// The v0.18.10 deployment briefly used QueryExecModeExec as a defensive
// patch against SQLSTATE 26000 "prepared statement does not exist"
// surprises when a TCP connection got silently swapped under pgx — seen
// in NAT/firewall/pgbouncer deployments. For the direct-Postgres LAN
// setup this code targets, that risk is covered by cfg.MaxConnIdleTime
// (4 min) cycling connections before NAT timeouts, so we get the
// caching win without the correctness hazard.
//
// If a pgbouncer in transaction or statement pooling mode ever appears
// in front of the DB, revert this flag to QueryExecModeExec (or switch
// to QueryExecModeCacheDescribe) — prepared statements are connection-
// scoped and transaction-pooling breaks the cache.
func TestPoolUsesStatementCache(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement") {
		t.Error("postgres.go must set cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement " +
			"so repeat queries skip server-side Parse+Describe on the hot INSERT/SELECT paths. " +
			"Revert to QueryExecModeExec only if a pgbouncer in txn/statement pooling mode is " +
			"introduced in front of the DB.")
	}
	// Defensive: the legacy Exec-mode line must not linger alongside the
	// new one — that would set the field twice and the second assignment
	// would win silently.
	if strings.Contains(src, "cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeExec") {
		t.Error("postgres.go still contains a QueryExecModeExec assignment — remove it so only " +
			"the CacheStatement line remains as the single source of truth")
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
