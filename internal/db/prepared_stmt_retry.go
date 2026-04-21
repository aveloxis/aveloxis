package db

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

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

// keepaliveIdle, keepaliveInterval, keepaliveCount tune dead-socket
// detection. At 60s idle + 6 probes × 10s interval the kernel
// declares a silent socket dead in ~2 minutes, pgxpool evicts it,
// and the stale-prepared-statement race window shrinks dramatically.
// Values are deliberately conservative; tighten further if a
// deployment has a very flaky link.
const (
	keepaliveIdle     = 60 * time.Second
	keepaliveInterval = 10 * time.Second
	keepaliveCount    = 6
	connectTimeout    = 10 * time.Second
)

// installKeepaliveDialer configures a custom pgconn DialFunc that
// sets TCP_KEEPIDLE / TCP_KEEPINTVL / TCP_KEEPCNT on every pooled
// socket via net.KeepAliveConfig (Go 1.23+). Also sets
// ConnectTimeout so a misconfigured host fails fast instead of
// blocking the scheduler's startup-Migrate path on the default
// kernel syn timeout.
//
// WHY NOT CONN-STRING PARAMS: pgx v5 does NOT parse libpq's
// keepalives / keepalives_idle / keepalives_interval / keepalives_count
// keys from the connection string. Unrecognized keys get forwarded to
// the server as StartupMessage RuntimeParams, and Postgres responds
// with FATAL "unrecognized configuration parameter (SQLSTATE 42704)"
// — observed in production when v0.18.14 first shipped with a
// conn-string approach. The DialFunc path below is the pgx-v5
// idiomatic way to achieve what libpq accepts as conn-string params.
func installKeepaliveDialer(cfg *pgxpool.Config) {
	dialer := &net.Dialer{
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     keepaliveIdle,
			Interval: keepaliveInterval,
			Count:    keepaliveCount,
		},
	}
	cfg.ConnConfig.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return dialer.DialContext(ctx, network, addr)
	}
	cfg.ConnConfig.ConnectTimeout = connectTimeout
}
