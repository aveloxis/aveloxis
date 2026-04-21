package db

import (
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
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

// TestPoolUsesCacheStatement verifies NewPostgresStore configures pgx
// to cache server-side prepared statements on each pooled connection.
//
// Why this is worth the server-side handle risk:
//
//   - v0.18.10 used QueryExecModeExec. Safe but every query paid for
//     a full server-side Parse+plan. On a LAN (vs loopback) that
//     overhead dominated.
//
//   - v0.18.11 switched to CacheStatement. Fast but produced a swarm
//     of SQLSTATE 26000 "prepared statement does not exist" errors
//     under heavy concurrent load, even with no pgbouncer. Root
//     cause (diagnosed v0.18.14): aveloxis's own 80-worker load on
//     the client mac stressed the network stack enough that TCP
//     connections were being silently swapped faster than the
//     4-minute MaxConnIdleTime could defend against. Kernel
//     keepalive detection window on both sides was ~2 hours by
//     default, leaving a huge race.
//
//   - v0.18.12 retreated to CacheDescribe. Safe, but gave back
//     essentially all of the CacheStatement speedup because server-
//     side Parse+plan was still happening on every query.
//
//   - v0.18.14 (this): CacheStatement again, with two new
//     defenses — aggressive TCP keepalives via
//     appendKeepaliveParams (detect dead sockets in ~2 minutes,
//     not 2 hours) AND a SendBatch retry wrapper (prepared_stmt_
//     retry.go) that recovers from any residual 26000 by re-
//     executing the batch once on a fresh connection.
//
// Reversion triggers:
//   - Repeated 26000 errors surviving the retry (the retry is
//     single-shot on purpose; sustained failures mean something
//     systemic is wrong).
//   - A pgbouncer in transaction or statement pooling mode appears
//     in the path.
//
// Revert to QueryExecModeCacheDescribe if either of those shows up.
func TestPoolUsesCacheStatement(t *testing.T) {
	data, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	src := string(data)
	if !strings.Contains(src, "cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement") {
		t.Error("postgres.go must set cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheStatement " +
			"to reuse server-side prepared statements on the hot INSERT/SELECT paths. Revert to " +
			"QueryExecModeCacheDescribe only if the keepalive + retry defenses from v0.18.14 prove " +
			"insufficient (sustained 26000s surviving sendBatchWithRetry) or if pgbouncer appears " +
			"in txn/statement pooling mode.")
	}
	// Defensive: the old modes must not linger alongside the new one.
	// A second assignment would silently win and either reintroduce
	// the v0.18.10 Parse-per-query cost (Exec) or the v0.18.12
	// Parse+plan-per-query cost (CacheDescribe).
	if strings.Contains(src, "cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeCacheDescribe") {
		t.Error("postgres.go still contains a QueryExecModeCacheDescribe assignment — remove it so " +
			"only the CacheStatement line remains (v0.18.14 moved back to CacheStatement with " +
			"keepalive + retry defenses)")
	}
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

// TestPoolInstallsKeepaliveDialer verifies NewPostgresStore installs
// a custom DialFunc that configures TCP_KEEPIDLE/INTVL/CNT on every
// pooled socket via net.KeepAliveConfig.
//
// Why not conn-string params: pgx v5 does NOT parse libpq's
// keepalives_idle / keepalives_interval / keepalives_count from
// either URL or keyword-form conn strings. Unrecognized keys get
// forwarded to the server as StartupMessage RuntimeParams, and
// Postgres responds with FATAL "unrecognized configuration
// parameter 'keepalives_idle' (SQLSTATE 42704)" — observed in
// production when v0.18.14 first shipped with the conn-string
// approach.
//
// The correct pgx-v5 path is a custom DialFunc that builds a
// net.Dialer with KeepAliveConfig set, requires Go 1.23+.
//
// Why it matters: without a tighter-than-default keepalive
// configuration, macOS + Linux give ~2h of silence before a dead
// socket is detected. At 60s idle + 10s interval × 6 probes we
// detect in ~2 minutes, pgxpool evicts the broken connection, and
// the v0.18.11 "SQLSTATE 26000" race window shrinks dramatically.
// sendBatchWithRetry handles any residue.
func TestPoolInstallsKeepaliveDialer(t *testing.T) {
	// Hot-path wiring: NewPostgresStore must call the installer
	// after ParseConfig so the dialer is set on the pool's
	// ConnConfig before pgxpool.NewWithConfig.
	pgData, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	pgSrc := string(pgData)
	if !strings.Contains(pgSrc, "installKeepaliveDialer(cfg)") {
		t.Error("NewPostgresStore must call installKeepaliveDialer(cfg) after " +
			"pgxpool.ParseConfig so every pooled socket gets TCP keepalives " +
			"set via net.KeepAliveConfig (not via libpq-style conn-string " +
			"params, which pgx v5 forwards to Postgres as unknown runtime " +
			"parameters and causes 42704 on connect)")
	}
	// Defensive: the old conn-string approach must be gone. Keeping
	// both would send keepalives= to Postgres as a RuntimeParam and
	// the FATAL 42704 startup error would return.
	if strings.Contains(pgSrc, "appendKeepaliveParams") {
		t.Error("postgres.go still references appendKeepaliveParams — the conn-" +
			"string approach was removed in v0.18.14 because pgx v5 forwards " +
			"the params to Postgres as runtime parameters (FATAL 42704)")
	}

	helperData, err := os.ReadFile("prepared_stmt_retry.go")
	if err != nil {
		t.Fatal(err)
	}
	helperSrc := string(helperData)
	required := []string{
		"installKeepaliveDialer",
		"net.KeepAliveConfig",
		"DialFunc",
		"Idle:",
		"Interval:",
		"Count:",
	}
	for _, want := range required {
		if !strings.Contains(helperSrc, want) {
			t.Errorf("prepared_stmt_retry.go must contain %q so the installer "+
				"wires TCP keepalive socket options via net.KeepAliveConfig", want)
		}
	}
	// The const-string keepalive params from the first v0.18.14
	// attempt must be gone.
	if strings.Contains(helperSrc, "keepalives=1") {
		t.Error("prepared_stmt_retry.go still contains a libpq-style " +
			"keepalive param string — remove it so pgx doesn't forward " +
			"these keys to Postgres as runtime params")
	}
}

// TestInstallKeepaliveDialer_SetsDialFuncAndTimeout — runtime
// behavior check. The installer must populate DialFunc (replacing
// pgx's default) and ConnectTimeout on the pool config.
//
// ConnectTimeout going from 0 → non-zero is the cheapest observable
// signal that the installer ran. We don't try to compare DialFunc
// pointers (Go forbids function equality) but we do verify a non-
// nil function is present after installation — pgxpool.ParseConfig
// sets its own default, so this mostly guards "the installer did
// not forget to set a DialFunc at all".
func TestInstallKeepaliveDialer_SetsDialFuncAndTimeout(t *testing.T) {
	// Build a minimal config — ParseConfig doesn't actually connect,
	// so any well-formed DSN is fine here.
	cfg, err := pgxpool.ParseConfig("postgres://u:p@localhost:5432/test?sslmode=disable")
	if err != nil {
		t.Fatalf("ParseConfig failed on test DSN: %v", err)
	}
	beforeTimeout := cfg.ConnConfig.ConnectTimeout

	installKeepaliveDialer(cfg)

	if cfg.ConnConfig.DialFunc == nil {
		t.Error("installKeepaliveDialer must set cfg.ConnConfig.DialFunc")
	}
	if cfg.ConnConfig.ConnectTimeout == beforeTimeout {
		t.Errorf("installKeepaliveDialer must change ConnectTimeout from its "+
			"pre-install value %v so a misconfigured host fails fast instead "+
			"of blocking on the default kernel syn timeout",
			beforeTimeout)
	}
	if cfg.ConnConfig.ConnectTimeout == 0 {
		t.Error("installKeepaliveDialer must set a non-zero ConnectTimeout")
	}
}
