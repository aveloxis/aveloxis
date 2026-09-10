// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
)

// AppNameBackends is what `aveloxis stop` sees of one application_name
// on the database: the backend PIDs that belong to THIS host, how many
// belong to other hosts, and how many it cannot place at all.
type AppNameBackends struct {
	// ThisHost are the backends whose client address is the probing
	// session's own host — the only ones a local `stop` may call
	// orphans or offer to terminate.
	ThisHost []int
	// OtherHosts counts the backends carrying the same tag from a
	// different client address: another machine running the same
	// aveloxis component against this database (a dedicated scancode
	// host, or the primary seen from one). They are reported, never
	// touched.
	OtherHosts int
	// Hidden counts the backends of ANOTHER database role whose client
	// address this role cannot see: pg_stat_activity shows a session's
	// address (and its state, query, …) only to roles that HOLD that
	// session's role's privileges and to roles that hold
	// pg_read_all_stats's — everyone else reads NULL, the same NULL a
	// unix socket shows (round-7 finding 1: with a least-privilege
	// runner role every primary worker read as local). Neither this
	// host's nor another's: reported with the way to a verdict, never a
	// recipe.
	Hidden int
}

// clientAddrVisibleSQL is "this role can see this row's client
// address": pg_stat_activity renders client_addr, state, query,
// backend_type and the rest of a session's detail as NULL for every
// backend whose role's privileges the viewer does not hold. Total —
// a NULL usesysid (every background worker: an autovacuum worker
// carries NULL usename AND NULL usesysid, measured on PG 18.4) and a
// LEFT-JOINed absent row both read false, never NULL. Every this-host
// verdict is gated on it, because that NULL address is
// indistinguishable from a unix socket's.
//
// BOTH arms ask what privileges the role EFFECTIVELY HOLDS, because
// that is the question the server asks. pg_stat_activity's gate is
// HAS_PGSTAT_PERMISSIONS ==
//
//	has_privs_of_role(GetUserId(), st_userid) ||
//	has_privs_of_role(GetUserId(), ROLE_PG_READ_ALL_STATS)
//
// and has_privs_of_role(a, b) is exactly pg_has_role(a, b, 'USAGE').
// This predicate has now been wrong on each arm in turn, both times in
// the unsafe direction, so it is spelled once and reasoned here.
//
// Arm 2 said 'MEMBER' through round 7 (round-8 finding 1, live-probed):
// a NOSUPERUSER NOINHERIT role granted pg_read_all_stats — and any role
// granted it WITH INHERIT FALSE — reads MEMBER=true while the server
// still renders another role's client_addr as NULL. That NULL then
// passed localClientAddrSQL and the backend read as THIS host's:
// round-7's own defect, one word away.
//
// Arm 1 was string identity with current_user until round-8 finding 2
// (found independently by both fresh-context reviewers, then
// live-probed): a role that INHERITS the backend's role HOLDS its
// privileges, so the server SHOWS the address while `usename IS NOT
// DISTINCT FROM current_user` said hidden. Measured with a viewer that
// inherits the backend's role: client_addr IS NOT NULL = t, usename
// identity = f, pg_read_all_stats 'USAGE' = f, and
// pg_has_role(current_user, usesysid, 'USAGE') = t. The failure is
// silent and fleet-wide for the ordinary least-privilege deployment
// (serve runs as one role, `stop` as an operator role that inherits
// it): every one of that deployment's OWN serve backends buckets into
// Hidden, pollBackends returns the moment ThisHost is empty, and the
// v0.20.0 orphan report is disabled — while the printed note tells the
// operator to re-check as a role whose privileges they already hold.
//
// usesysid, not usename: it is the exact mirror of the server's
// st_userid, and the two spellings differ on a role the name cannot
// resolve — measured, pg_has_role with a bogus NAME raises `ERROR:
// role "…" does not exist` while the OID form returns a bool. A
// watcher must not be able to FAIL on a row it is only classifying.
func clientAddrVisibleSQL(row string) string {
	return fmt.Sprintf(`(COALESCE(pg_has_role(current_user, %[1]s.usesysid, 'USAGE'), FALSE) OR pg_has_role(current_user, 'pg_read_all_stats', 'USAGE'))`, row)
}

// localClientAddrSQL is "this client address belongs to the database
// host itself": NULL is a unix socket (only reachable ON the host), and
// the loopback forms are one bucket because a dual-stack `localhost`
// resolves to ::1 for one process and 127.0.0.1 for the next (observed
// locally: psql lands on ::1 while pgx dials 127.0.0.1). The
// IPv4-mapped loopback is the shape a dual-stack listener reports for a
// v4 client.
func localClientAddrSQL(col string) string {
	return fmt.Sprintf(`(%[1]s IS NULL OR %[1]s <<= inet '127.0.0.0/8' OR %[1]s = inet '::1' OR %[1]s <<= inet '::ffff:127.0.0.0/104')`, col)
}

// sameClientHostSQL is the ONE spelling (SR-17) of "these two
// pg_stat_activity client addresses are the same host": equal
// addresses, or both local to the database host. Cross-family
// containment (`::1 <<= 127.0.0.0/8`) is false, not an error, so the
// two loopback tests compose. Under a transaction pooler every client
// collapses onto the pooler's address and everything reads as one
// host — the pre-v0.29.4 behavior, never a regression. It compares
// ADDRESSES only and is composed solely by backendOnThisHostSQL, which
// adds the visibility gate a NULL address needs.
func sameClientHostSQL(a, b string) string {
	return fmt.Sprintf(`((%[1]s IS NOT DISTINCT FROM %[2]s) OR (%[3]s AND %[4]s))`,
		a, b, localClientAddrSQL(a), localClientAddrSQL(b))
}

// probingSessionSQL is the ONE spelling (SR-17) of the probing session's
// own row, aliased `me` for backendOnThisHostSQL: its client address,
// overridable through the given parameter placeholder (NULL in
// production — the session's real address; a foreign address in the
// behavioral tests, the only way one machine can drive the
// other-address arm). Scoped by pid, which is cluster-unique, so it
// needs no datname filter.
func probingSessionSQL(param string) string {
	return fmt.Sprintf(`(SELECT COALESCE(%s::inet, client_addr) AS client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid()) me`, param)
}

// backendOnThisHostSQL is THE this-host verdict (SR-17, SR-18): row's
// client address is visible to this role AND the same host as the
// probing session's (probingSessionSQL's `me`). A hidden address is
// never this host's — round-7 finding 1: without the gate a
// least-privilege runner role read every primary worker as local, and
// the incident's 64 terminate recipes were back.
func backendOnThisHostSQL(row string) string {
	return fmt.Sprintf(`(%s AND %s)`, clientAddrVisibleSQL(row), sameClientHostSQL(row+".client_addr", "me.client_addr"))
}

// backgroundBackendSQL is the ONE spelling (SR-17) of "this
// pg_stat_activity row is a PostgreSQL background worker, not a client
// session". backend_type decides whenever it is VISIBLE; usename
// decides only when it is not.
//
// The two halves are masked independently. backend_type (like
// client_addr, state and query) is rendered NULL for a viewer that does
// not hold the row's role's privileges — see clientAddrVisibleSQL — so
// it cannot decide alone. usename is masked the same way, but a
// background worker carries NULL usename for EVERY viewer (an
// autovacuum worker has NULL usename AND NULL usesysid — measured on PG
// 18.4), so it is the fallback when backend_type says nothing.
//
// usename alone is NOT sufficient, which is round-8's own L10 finding:
// a client backend still in `state = 'starting'` — mid-authentication —
// carries backend_type = 'client backend' VISIBLE while usename,
// usesysid and datname are all NULL (reproduced deliberately with a
// 400-connection hammer: pid 29663, client backend, NULL user, NULL
// datname, state starting, client_addr ::1). The round-8 first draft
// read `usename IS NULL` first and mislabeled that row a background
// worker. Ordering the halves as below fixes it: a VISIBLE
// 'client backend' is never background, whatever its usename says.
//
// Truth table (row = (backend_type, usename)):
//
//	('client backend', set)        -> false  ordinary client
//	('client backend', NULL)       -> false  the mid-auth row above
//	('autovacuum worker', NULL)    -> true   visible background worker
//	('logical replication launcher', set) -> true
//	(NULL, NULL)                   -> true   masked background worker
//	(NULL, set)                    -> false  masked client of another role
//
// The last two are the residuals a restricted viewer is left with. A
// masked row with NULL usename is treated as background: that is the
// case this fallback exists for, and a masked mid-auth client landing
// there is safe — the background arm carries no terminate recipe and a
// backend that has not authenticated holds no lock. A masked row with a
// usename falls through to the `hidden` bucket, which is the documented
// no-verdict arm.
func backgroundBackendSQL(row string) string {
	return fmt.Sprintf(`((%[1]s.backend_type IS NOT NULL AND %[1]s.backend_type <> 'client backend') OR (%[1]s.backend_type IS NULL AND %[1]s.usename IS NULL))`, row)
}

// BackendsByAppName returns the backends tagged appName on THIS
// database, split by whether they belong to the probing session's own
// host. The 2026-09-09 scancode-runner incident: `aveloxis stop serve`
// on a second host matched application_name alone, found the primary's
// 64 pool backends, and printed pg_terminate_backend recipes for the
// production serve. pg_stat_activity is cluster-wide (the datname
// filter is load-bearing — two aveloxis databases share the cluster).
// A row that fails to scan is an error, not an absent backend (SR-5).
func (s *PostgresStore) BackendsByAppName(ctx context.Context, appName string) (AppNameBackends, error) {
	return s.backendsByAppNameFrom(ctx, appName, nil)
}

// backendsByAppNameFrom is BackendsByAppName with the probing session's
// own address overridable: production passes nil (the real
// client_addr of this session); the behavioral test passes a foreign
// address so the other-host arm — which one machine cannot otherwise
// produce — is driven through the real query.
func (s *PostgresStore) backendsByAppNameFrom(ctx context.Context, appName string, asIfFrom *string) (AppNameBackends, error) {
	var out AppNameBackends
	rows, err := s.pool.Query(ctx, `
		SELECT a.pid, `+clientAddrVisibleSQL("a")+` AS visible, `+backendOnThisHostSQL("a")+` AS this_host
		FROM pg_stat_activity a
		CROSS JOIN `+probingSessionSQL("$2")+`
		WHERE a.datname = current_database() AND a.application_name = $1
		ORDER BY a.pid`, appName, asIfFrom)
	if err != nil {
		return out, fmt.Errorf("backends tagged %q: %w", appName, err)
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var visible, thisHost bool
		if err := rows.Scan(&pid, &visible, &thisHost); err != nil {
			return out, fmt.Errorf("backends tagged %q: scan: %w", appName, err)
		}
		switch {
		case !visible:
			out.Hidden++
		case thisHost:
			out.ThisHost = append(out.ThisHost, pid)
		default:
			out.OtherHosts++
		}
	}
	if err := rows.Err(); err != nil {
		return out, fmt.Errorf("backends tagged %q: %w", appName, err)
	}
	return out, nil
}
