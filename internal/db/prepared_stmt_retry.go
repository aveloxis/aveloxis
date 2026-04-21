package db

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// keepaliveParams are the libpq-compatible TCP keepalive settings
// appended to the pgx connection string by appendKeepaliveParams.
// pgx's dialer reads these and sets TCP_KEEPIDLE / TCP_KEEPINTVL /
// TCP_KEEPCNT on every pooled socket.
//
// Values tuned for dead-socket detection in ~2 minutes (60 idle + 6
// probes × 10s interval), which is fast enough that pgxpool evicts
// the broken connection before the per-connection prepared-statement
// cache can fire many queries at a swapped backend. The
// sendBatchWithRetry wrapper handles any residual race.
//
// connect_timeout=10 caps initial dial time so a misconfigured host
// fails fast instead of blocking the scheduler's startup-Migrate
// path for the default 2-minute kernel syn timeout.
const keepaliveParams = "keepalives=1&keepalives_idle=60&keepalives_interval=10&keepalives_count=6&connect_timeout=10"

// appendKeepaliveParams merges keepaliveParams into a pgx connection
// string. Idempotent: if the caller already set keepalives=, returns
// the string unchanged so per-deployment overrides survive.
//
// Handles both URL-form ("postgres://user:pass@host:port/db?sslmode=prefer")
// and libpq keyword-form ("host=... port=... sslmode=prefer") conn
// strings. ConnectionString() in internal/config/config.go emits
// URL-form today, but pgx accepts both.
func appendKeepaliveParams(connString string) string {
	if strings.Contains(connString, "keepalives=") {
		return connString
	}
	if strings.Contains(connString, "://") {
		// URL form — query string gets an extra param. If there's
		// no query string yet, start one with '?'; otherwise extend
		// with '&'.
		sep := "&"
		if !strings.Contains(connString, "?") {
			sep = "?"
		}
		return connString + sep + keepaliveParams
	}
	// Keyword form — space-separated key=value pairs. Convert the
	// '&'-joined keepalive string to space-joined.
	kw := strings.ReplaceAll(keepaliveParams, "&", " ")
	if connString == "" {
		return kw
	}
	return connString + " " + kw
}

// staleStatementSQLSTATE is the PostgreSQL error code for "prepared
// statement does not exist" — the symptom when pgx's per-connection
// prepared-statement cache falls out of sync with the backend after
// a TCP connection is silently swapped under load.
const staleStatementSQLSTATE = "26000"

// isStalePreparedStatement returns true when err (possibly wrapped)
// represents SQLSTATE 26000. This is the single retry signal the
// sendBatchWithRetry wrapper uses.
//
// Why only 26000: other SQL errors (constraint violations, bad JSON,
// etc.) are not transient — retrying them would waste time and mask
// real data problems. 26000 specifically means "this cache went
// stale; a fresh connection will succeed", which is the one case
// where a blind retry is correct.
func isStalePreparedStatement(err error) bool {
	if err == nil {
		return false
	}
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}
	return pgErr.Code == staleStatementSQLSTATE
}

// sendBatchWithRetry sends a pgx.Batch through the pool and retries
// ONCE on SQLSTATE 26000. Under heavy concurrent load on a LAN link,
// a TCP connection can be silently replaced out from under pgx while
// the client still thinks the server-side prepared-statement cache
// holds "stmtcache_<hash>". The next SendBatch fires statements that
// the new backend has never seen, and Postgres rejects the whole
// batch with 26000.
//
// On the retry, pgxpool is very likely to hand us a different pooled
// connection, and even if it returns the same one, pgx's cache
// invalidation on 26000 causes the statements to be re-prepared
// before the second SendBatch executes. Either way a fresh prepare
// cycle runs and the batch succeeds.
//
// A single retry is deliberate: if the retry also 26000s, something
// more systemic is wrong (network is thrashing, or pgbouncer has
// appeared in the path) and looping would just amplify the problem.
// The caller sees the second error, which surfaces in the monitor.
func (s *PostgresStore) sendBatchWithRetry(ctx context.Context, batch *pgx.Batch) error {
	err := s.pool.SendBatch(ctx, batch).Close()
	if err == nil || !isStalePreparedStatement(err) {
		return err
	}
	s.logger.Warn("prepared statement cache miss on SendBatch — retrying once",
		"sqlstate", staleStatementSQLSTATE, "rows", batch.Len(), "error", err)
	return s.pool.SendBatch(ctx, batch).Close()
}
