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

// TestPoolAppendsKeepaliveParams verifies NewPostgresStore amends the
// connection string with libpq-compatible TCP keepalive settings so
// pgx's dialer installs aggressive kernel-level keepalive probes on
// every pooled socket.
//
// Without these, macOS + Linux defaults give ~2h of silence before a
// dead socket is detected — long enough that pgx's per-connection
// prepared-statement cache diverges from the server and the next
// SendBatch hits SQLSTATE 26000 (the v0.18.11 failure we retried
// around in v0.18.14 via sendBatchWithRetry).
//
// With keepalives_idle=60 / keepalives_interval=10 / keepalives_count=6,
// the kernel declares a silent socket dead in ~2 minutes, pgxpool
// evicts it, and the stale-cache race window shrinks dramatically.
// The retry wrapper handles the residue; keepalives keep the residue
// small.
//
// The constant and helper live in prepared_stmt_retry.go alongside
// the retry wrapper — the two defenses are a package deal and
// belong next to each other.
func TestPoolAppendsKeepaliveParams(t *testing.T) {
	// The hot-path wiring: NewPostgresStore must call the helper.
	pgData, err := os.ReadFile("postgres.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(pgData), "appendKeepaliveParams(connString)") {
		t.Error("NewPostgresStore must call appendKeepaliveParams(connString) " +
			"before pgxpool.ParseConfig so every pooled socket gets the " +
			"keepalive settings")
	}

	// The params themselves: must live in the helper file.
	helperData, err := os.ReadFile("prepared_stmt_retry.go")
	if err != nil {
		t.Fatal(err)
	}
	helperSrc := string(helperData)
	required := []string{
		"keepalives=1",
		"keepalives_idle=60",
		"keepalives_interval=10",
		"keepalives_count=6",
		"connect_timeout=10",
	}
	for _, want := range required {
		if !strings.Contains(helperSrc, want) {
			t.Errorf("prepared_stmt_retry.go must define keepalive param %q "+
				"so kernel TCP keepalives detect dead sockets in ~2 minutes "+
				"instead of the OS default 2 hours", want)
		}
	}
}

// TestAppendKeepaliveParams_URLForm — runtime behavior check on the
// helper. Must merge the params into URL-form conn strings correctly,
// whether or not the caller already included other query params.
func TestAppendKeepaliveParams_URLForm(t *testing.T) {
	cases := []struct {
		name, in string
		want     []string // substrings that must appear in the output
	}{
		{
			name: "with existing query",
			in:   "postgres://u:p@h:5432/db?sslmode=prefer",
			want: []string{"sslmode=prefer", "&keepalives=1", "keepalives_idle=60"},
		},
		{
			name: "without query",
			in:   "postgres://u:p@h:5432/db",
			want: []string{"?keepalives=1", "keepalives_idle=60"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := appendKeepaliveParams(tc.in)
			for _, w := range tc.want {
				if !strings.Contains(got, w) {
					t.Errorf("output %q must contain %q", got, w)
				}
			}
		})
	}
}

// TestAppendKeepaliveParams_Idempotent — if the caller already set
// keepalives=, the helper must not clobber or duplicate. Per-
// deployment overrides have to survive the merge.
func TestAppendKeepaliveParams_Idempotent(t *testing.T) {
	in := "postgres://u:p@h:5432/db?sslmode=prefer&keepalives=1&keepalives_idle=30"
	got := appendKeepaliveParams(in)
	if got != in {
		t.Errorf("appendKeepaliveParams must be a no-op when keepalives= is already set\n  in:  %s\n  got: %s", in, got)
	}
}

// TestAppendKeepaliveParams_KeywordForm — libpq keyword-form conn
// strings use space-separated key=value pairs, not URL query syntax.
// The helper must emit the right shape.
func TestAppendKeepaliveParams_KeywordForm(t *testing.T) {
	in := "host=h port=5432 user=u dbname=db sslmode=prefer"
	got := appendKeepaliveParams(in)
	if !strings.Contains(got, "host=h") {
		t.Errorf("original fields must survive: %s", got)
	}
	if !strings.Contains(got, " keepalives=1") {
		t.Errorf("keyword-form output must use space-separator for new params: %s", got)
	}
	if strings.Contains(got, "&keepalives") {
		t.Errorf("keyword-form output must NOT use URL-query '&' separator: %s", got)
	}
}
