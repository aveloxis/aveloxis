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
//
// Round-11 finding 9: the derived table has NO FROM clause, so it is
// always EXACTLY ONE ROW and the CROSS JOIN in each reader can never
// empty the result. The pre-v0.29.4 form selected FROM
// pg_stat_activity directly, and a zero-row `me` would have silently
// emptied every reader — `stop` reporting all-clear while backends were
// live, the blocker poll reporting no blockers mid-outage. Unreachable
// in practice (a session always sees its own row) but a silent
// fail-open in a release whose theme is fail-closed probes.
//
// `present` carries the other half. Without it a missing own row would
// yield a NULL client address, which localClientAddrSQL reads as
// "unix socket, therefore this host" — fail-open in the WORSE
// direction: EVERY backend reads as local, which is the incident's own
// shape. Every reader selects it and refuses to render a verdict when
// it is false. With an override supplied the address IS the datum, so
// present is trivially true.
func probingSessionSQL(addrParam, hostParam string) string {
	return fmt.Sprintf(`(SELECT COALESCE(%[1]s::inet, (SELECT client_addr FROM pg_stat_activity WHERE pid = pg_backend_pid())) AS client_addr,
		       NULLIF(%[2]s::text, '') AS host_tag,
		       (%[1]s::inet IS NOT NULL OR EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = pg_backend_pid())) AS present) me`, addrParam, hostParam)
}

// backendOnThisHostSQL is THE this-host verdict (SR-17, SR-18): row's
// client address is visible to this role AND the same host as the
// probing session's (probingSessionSQL's `me`). A hidden address is
// never this host's — round-7 finding 1: without the gate a
// least-privilege runner role read every primary worker as local, and
// the incident's 64 terminate recipes were back.
func backendOnThisHostSQL(row string) string {
	return fmt.Sprintf(`(%s AND %s)`, clientAddrVisibleSQL(row), sameHostAsProbeSQL(row))
}

// sameHostAsProbeSQL is the ONE spelling (SR-17) of "this backend runs
// on the probing session's host": the client-address rule decides, and
// the host marker can only VETO it.
//
// THE RULE, in one sentence: a marker can only SEPARATE hosts, never
// MERGE them.
//
// Two failures, one predicate. The address rule alone is not host
// identity — behind a transaction pooler, a database proxy or shared
// NAT every client collapses onto a single client_addr, so `aveloxis
// stop` on a scancode runner reads the PRIMARY's backends as this
// host's and offers pg_terminate_backend recipes for production: the
// 2026-09-09 incident, through a topology no in-database signal can
// detect. A marker carried in application_name is immune to that
// collapse, because the server stores what the client sent and never
// rewrites it, so two hosts behind one pooler still disagree — and the
// veto turns that disagreement into OTHER.
//
// But the marker alone is not host identity EITHER, which is what
// round 12 got wrong (Copilot round 3; an L10 hit on round 12's own
// fix). Round 12 made marker equality DECIDE, so a matching marker
// overrode a differing address. os.Hostname() is not unique across
// machines: two Docker/Podman Compose stacks running the same compose
// file on different hosts report the same container hostname, and
// Compose is a documented aveloxis deployment. Round 12 promoted the
// remote backend to ThisHost and handed it a terminate recipe — the
// incident restored by the fix meant to prevent it. Composing the two
// with AND is monotonic in the safe direction: this predicate never
// says THIS where the pre-round-12 address rule said OTHER, so marker
// quality is a non-safety property. A collision (a shared hostname, a
// sanitizer collision, anything) degrades to the address rule, never
// to a terminate recipe.
//
// The fallback arms are what keep this additive. A row with no marker
// is a pre-round-12 binary, or a host whose kernel would not name it
// (hostid.HostTag() == ""); a probing session with no marker of its own
// is the same on our side. Either way the veto abstains and the answer
// is the pre-v0.29.4 one, so a mixed-version fleet keeps working.
//
// RESIDUAL, stated rather than hidden: one host reached over two
// addresses now reads as two. A serve that dialed a non-loopback DSN
// host while `stop` dials loopback (or a serve dialed direct while
// `stop` goes through a pooler) is a false OTHER, and pollBackends
// returns the moment ThisHost is empty — so `stop` skips the drain
// wait. Round 12 fixed that case incidentally; round 13 gives it back,
// because from inside the database it is INDISTINGUISHABLE from the
// two-Compose-hosts case, and only one of the two directions can print
// `pg_terminate_backend` for a machine we are not on. The operator rule
// is already documented and unchanged: run `stop` with the same `-c`
// config the component was started with.
//
// The marker is compared, NOT trusted as a privilege: the visibility
// gate in backendOnThisHostSQL is unchanged and still runs first, so a
// backend whose address this role cannot see is Hidden regardless of
// what its marker says. Round 7/8 bought that gate with two
// live-probed defects.
func sameHostAsProbeSQL(row string) string {
	marker := appNameHostSQL(row + ".application_name")
	return fmt.Sprintf(`(%[1]s AND (%[2]s IS NULL OR me.host_tag IS NULL OR %[2]s = me.host_tag))`,
		sameClientHostSQL(row+".client_addr", "me.client_addr"), marker)
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
	return s.backendsByAppNameFrom(ctx, appName, nil, nil)
}

// backendsByAppNameFrom is BackendsByAppName with the probing session's
// own address AND host marker overridable: production passes nil for
// both (the real client_addr of this session, and this machine's
// hostid.HostTag()); the behavioral tests pass a foreign address or a
// foreign marker so the other-host arm — which one machine cannot
// otherwise produce — is driven through the real query.
//
// appName is the component's MATCH PREFIX (`aveloxis-serve`), not the
// full tag: the tag carries this host's marker after '@' and every
// host's differs, so the WHERE compares prefixes. split_part returns
// the whole string when the separator is absent, so a backend tagged
// by a pre-round-12 binary still matches.
func (s *PostgresStore) backendsByAppNameFrom(ctx context.Context, appName string, asIfFrom, asIfHost *string) (AppNameBackends, error) {
	var out AppNameBackends
	host := HostMarker()
	if asIfHost != nil {
		host = *asIfHost
	}
	rows, err := s.pool.Query(ctx, `
		SELECT a.pid, me.present, `+clientAddrVisibleSQL("a")+` AS visible, `+backendOnThisHostSQL("a")+` AS this_host
		FROM pg_stat_activity a
		CROSS JOIN `+probingSessionSQL("$2", "$3")+`
		WHERE a.datname = current_database() AND `+appNamePrefixSQL("a.application_name")+` = $1
		ORDER BY a.pid`, appName, asIfFrom, host)
	if err != nil {
		return out, fmt.Errorf("backends tagged %q: %w", appName, err)
	}
	defer rows.Close()
	for rows.Next() {
		var pid int
		var present, visible, thisHost bool
		if err := rows.Scan(&pid, &present, &visible, &thisHost); err != nil {
			return out, fmt.Errorf("backends tagged %q: scan: %w", appName, err)
		}
		if !present {
			return AppNameBackends{}, fmt.Errorf("backends tagged %q: this session has no pg_stat_activity row, so no backend can be placed on this host — refusing to report a verdict", appName)
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
