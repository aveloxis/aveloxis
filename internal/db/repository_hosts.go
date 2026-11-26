// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostguard"
)

// RepositoryHostImpact is what depends on one package repository host
// (v0.29.187; summary/70, 4.5, D14): what an operator is shown before
// deciding to distrust it, and what the distrust then does.
type RepositoryHostImpact struct {
	// StoredResults are the poms (and misses) stored from the host: a
	// distrust deletes them.
	StoredResults int
	// Repositories have a dependency whose version came through the host,
	// or a Maven tree whose parent or BOM chain did.
	Repositories int
	// Findings are current findings on dependencies whose version came
	// through the host. At each repository's next analysis the version is
	// no longer known and they close as version_lost, which is not a fix.
	Findings int
	// Trees are Maven trees whose chain used the host. They stop being
	// read whole, so their managed entries go back into the inventory and
	// the never_applied findings on those reopen.
	Trees int
}

// RepositoryHost is one host of the administrator's list.
type RepositoryHost struct {
	Host           string
	FirstFetchedAt *time.Time // nil: never answered (a configured host not yet asked, or one only distrusted)
	LastAnsweredAt *time.Time
	DistrustedAt   *time.Time // nil: not distrusted
	DistrustedBy   string
	Note           string
	Impact         RepositoryHostImpact
	// Signals are what the periodic check last saw, one per kind checked.
	Signals []RepositoryHostSignal
}

// RepositoryHostSignal is one kind of check's standing for a host.
type RepositoryHostSignal struct {
	Kind   string
	State  string // the last definitive answer: signal or clear; unknown when there has been none
	Detail string
	Since  time.Time
	// UnknownSince is set while the most recent check could not tell.
	UnknownSince  *time.Time
	UnknownDetail string
	DismissedAt   *time.Time
}

// NeedsAttention: a signal that began after it was last dismissed.
func (g RepositoryHostSignal) NeedsAttention() bool {
	return g.State == "signal" && (g.DismissedAt == nil || g.DismissedAt.Before(g.Since))
}

// HostObservation is one check's answer, as the store takes it.
type HostObservation struct {
	Kind   string
	State  string // signal, clear or unknown
	Detail string
}

// RecordRepositoryHostObservations stores one check of one host. A
// definitive answer (signal, clear) replaces the standing one, and since
// moves only when the answer changes. An unknown never replaces a
// definitive answer: the row keeps its state and notes that the last look
// could not tell.
//
// A check that is switched off records nothing, so what it last saw stands,
// with its date (declined as a defect, review round 1). A signal is an
// observation, and nothing here discards one because the check that made
// it no longer runs: the page shows since when, and an administrator
// dismisses it.
func (s *PostgresStore) RecordRepositoryHostObservations(ctx context.Context, host string, obs []HostObservation) error {
	host = hostguard.Normalize(host)
	if host == "" {
		return errors.New("no host")
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		for _, o := range obs {
			switch o.State {
			case "signal", "clear":
				_, err = tx.Exec(ctx, `
					INSERT INTO aveloxis_ops.repository_host_signals (host, kind, state, detail)
					VALUES ($1, $2, $3, $4)
					ON CONFLICT (host, kind) DO UPDATE SET
					    since = CASE WHEN repository_host_signals.state = EXCLUDED.state
					                 THEN repository_host_signals.since ELSE NOW() END,
					    state = EXCLUDED.state, detail = EXCLUDED.detail, checked_at = NOW(),
					    unknown_since = NULL, unknown_detail = ''`, host, o.Kind, o.State, o.Detail)
			case "unknown":
				_, err = tx.Exec(ctx, `
					INSERT INTO aveloxis_ops.repository_host_signals (host, kind, state, unknown_since, unknown_detail)
					VALUES ($1, $2, 'unknown', NOW(), $3)
					ON CONFLICT (host, kind) DO UPDATE SET
					    unknown_since = COALESCE(repository_host_signals.unknown_since, NOW()),
					    unknown_detail = EXCLUDED.unknown_detail, checked_at = NOW()`, host, o.Kind, o.Detail)
			default:
				err = fmt.Errorf("repository host observation: state %q is not signal, clear or unknown", o.State)
			}
			if err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	})
}

// DismissRepositoryHostSignals marks every signal the host has now as seen.
// A signal that begins later is on the attention list again. n is the
// number of signals dismissed.
func (s *PostgresStore) DismissRepositoryHostSignals(ctx context.Context, host, by string) (n int, err error) {
	host = hostguard.Normalize(host)
	if host == "" {
		return 0, errors.New("no host")
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		tag, err := s.pool.Exec(ctx, `
			UPDATE aveloxis_ops.repository_host_signals SET dismissed_at = NOW(), dismissed_by = $2
			 WHERE host = $1 AND state = 'signal' AND (dismissed_at IS NULL OR dismissed_at < since)`, host, by)
		n = int(tag.RowsAffected())
		return err
	})
	return n, err
}

// repositoryHostCheckMarker names the check's row of program_markers: when
// a pass last finished, so a restart does not look at every host again.
const repositoryHostCheckMarker = "repository_host_check"

// SinceRepositoryHostCheck is how long ago a pass of the check last
// finished, by the database's clock. ever is false when none has.
func (s *PostgresStore) SinceRepositoryHostCheck(ctx context.Context) (since time.Duration, ever bool, err error) {
	var secs *float64
	err = s.pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM NOW() - MAX(completed_at))::float8
		FROM aveloxis_ops.program_markers WHERE marker = $1`, repositoryHostCheckMarker).Scan(&secs)
	if err != nil || secs == nil {
		return 0, false, err
	}
	return time.Duration(*secs * float64(time.Second)), true, nil
}

// MarkRepositoryHostCheckDone records that a pass finished now, having
// looked at hosts hosts.
func (s *PostgresStore) MarkRepositoryHostCheckDone(ctx context.Context, hosts int) error {
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.program_markers (marker, rows_done, completed_at)
			VALUES ($1, $2, NOW())
			ON CONFLICT (marker) DO UPDATE SET rows_done = EXCLUDED.rows_done, updated_at = NOW(), completed_at = NOW()`,
			repositoryHostCheckMarker, hosts)
		return err
	})
}

// RepositoryHostFirstFetches is when each host that has answered was first
// fetched from, for the hosts that are not distrusted: what the
// registration-date check compares a domain's registration with.
func (s *PostgresStore) RepositoryHostFirstFetches(ctx context.Context) (map[string]time.Time, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT h.host, h.first_fetched_at FROM aveloxis_ops.package_repository_hosts h
		 WHERE NOT EXISTS (SELECT 1 FROM aveloxis_ops.distrusted_repository_hosts d WHERE d.host = h.host)`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]time.Time{}
	for rows.Next() {
		var h string
		var at time.Time
		if err := rows.Scan(&h, &at); err != nil {
			return nil, err
		}
		out[h] = at
	}
	return out, rows.Err()
}

// repositoryHostImpact computes one host's impact. host is normalized.
func (s *PostgresStore) repositoryHostImpact(ctx context.Context, host string) (RepositoryHostImpact, error) {
	var im RepositoryHostImpact
	urls, err := s.repositoryURLsOfHost(ctx, host)
	if err != nil {
		return im, err
	}
	if err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_data.maven_pom_results WHERE repository_url = ANY($1)`, urls).Scan(&im.StoredResults); err != nil {
		return im, err
	}
	// Rows by the indexed containment; trees from the coverage record.
	if err := s.pool.QueryRow(ctx, `
		SELECT
		  (SELECT COUNT(*) FROM (
		      SELECT repo_id FROM aveloxis_data.repo_deps_libyear WHERE version_hosts @> ARRAY[$1]::text[]
		      UNION
		      SELECT c.repo_id FROM aveloxis_data.repo_dependency_coverage c,
		             jsonb_array_elements(c.maven_tree_hosts) t
		       WHERE c.maven_tree_hosts IS NOT NULL AND t->'hosts' ? $1) r),
		  (SELECT COUNT(*) FROM aveloxis_data.repo_deps_vulnerabilities v
		     JOIN aveloxis_data.repo_deps_libyear l ON l.repo_id = v.repo_id AND l.purl = v.package_purl
		    WHERE v.resolved_at IS NULL AND l.version_hosts @> ARRAY[$1]::text[]),
		  (SELECT COUNT(*) FROM aveloxis_data.repo_dependency_coverage c,
		          jsonb_array_elements(c.maven_tree_hosts) t
		    WHERE c.maven_tree_hosts IS NOT NULL AND t->'hosts' ? $1)`, host).
		Scan(&im.Repositories, &im.Findings, &im.Trees); err != nil {
		return im, err
	}
	return im, nil
}

// repositoryURLsOfHost are the stored repository URLs whose host is host.
// The stored key is the URL as configured or declared; its host is derived
// here, by the one normalization the distrust list uses.
func (s *PostgresStore) repositoryURLsOfHost(ctx context.Context, host string) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT DISTINCT repository_url FROM aveloxis_data.maven_pom_results`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var u string
		if err := rows.Scan(&u); err != nil {
			return nil, err
		}
		if repositoryHost(u) == host {
			out = append(out, u)
		}
	}
	return out, rows.Err()
}

// PreviewRepositoryHostDistrust says what distrusting host would do.
func (s *PostgresStore) PreviewRepositoryHostDistrust(ctx context.Context, host string) (RepositoryHostImpact, error) {
	host = hostguard.Normalize(host)
	if host == "" {
		return RepositoryHostImpact{}, errors.New("no host")
	}
	return s.repositoryHostImpact(ctx, host)
}

// DistrustAndPurgeRepositoryHost puts host on the distrusted list and
// deletes every result stored from it, in one transaction. changed is false
// when it was already distrusted (nothing is done). The impact is counted
// just before the transaction begins, for the confirmation the operator
// was shown: a result written between that count and the delete is deleted
// and is not in it.
func (s *PostgresStore) DistrustAndPurgeRepositoryHost(ctx context.Context, host, by, note string) (im RepositoryHostImpact, changed bool, err error) {
	host = hostguard.Normalize(host)
	if host == "" {
		return im, false, errors.New("no host to distrust")
	}
	if im, err = s.repositoryHostImpact(ctx, host); err != nil {
		return im, false, err
	}
	urls, err := s.repositoryURLsOfHost(ctx, host)
	if err != nil {
		return im, false, err
	}
	err = s.withRetry(ctx, func(ctx context.Context) error {
		changed = false
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		tag, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_ops.distrusted_repository_hosts (host, distrusted_by, note)
			VALUES ($1, $2, $3) ON CONFLICT (host) DO NOTHING`, host, by, note)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return tx.Commit(ctx) // already distrusted: the earlier entry stands
		}
		// The list row is written first, in this transaction: from its
		// commit on, the results' writer refuses the host. A write already
		// under way when this commits does not see the row and can land
		// after this delete; nothing serves it while the host is
		// distrusted, and RestoreRepositoryHost deletes it before the host
		// is used again.
		if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url = ANY($1)`, urls); err != nil {
			return err
		}
		changed = true
		return tx.Commit(ctx)
	})
	return im, changed, err
}

// ListRepositoryHosts lists every package repository host that is known:
// one that answered, one with stored results, and one that is distrusted,
// each with what depends on it. also names hosts to include whatever is
// stored (the configured repositories).
func (s *PostgresStore) ListRepositoryHosts(ctx context.Context, also []string) ([]RepositoryHost, error) {
	hosts := map[string]*RepositoryHost{}
	get := func(h string) *RepositoryHost {
		h = hostguard.Normalize(h)
		if h == "" {
			return nil
		}
		if hosts[h] == nil {
			hosts[h] = &RepositoryHost{Host: h}
		}
		return hosts[h]
	}
	for _, h := range also {
		get(h)
	}
	rows, err := s.pool.Query(ctx, `SELECT host, first_fetched_at, last_answered_at FROM aveloxis_ops.package_repository_hosts`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var h string
		var first, last time.Time
		if err := rows.Scan(&h, &first, &last); err != nil {
			rows.Close()
			return nil, err
		}
		if r := get(h); r != nil {
			r.FirstFetchedAt, r.LastAnsweredAt = &first, &last
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.pool.Query(ctx, `SELECT host, distrusted_at, distrusted_by, note FROM aveloxis_ops.distrusted_repository_hosts`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var h, by, note string
		var at time.Time
		if err := rows.Scan(&h, &at, &by, &note); err != nil {
			rows.Close()
			return nil, err
		}
		if r := get(h); r != nil {
			r.DistrustedAt, r.DistrustedBy, r.Note = &at, by, note
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	rows, err = s.pool.Query(ctx, `
		SELECT host, kind, state, detail, since, unknown_since, unknown_detail, dismissed_at
		  FROM aveloxis_ops.repository_host_signals ORDER BY host, kind`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var h string
		var g RepositoryHostSignal
		if err := rows.Scan(&h, &g.Kind, &g.State, &g.Detail, &g.Since, &g.UnknownSince, &g.UnknownDetail, &g.DismissedAt); err != nil {
			rows.Close()
			return nil, err
		}
		if r := get(h); r != nil {
			r.Signals = append(r.Signals, g)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// One impact reading per host (declined as a defect, review round 1,
	// and owed a measurement): the hosts are the configured repositories
	// and the declared ones that answered, a handful; each reading is an
	// indexed containment and one pass over the coverage records that hold
	// a Maven tree. The columns do not exist on a fleet-scale database
	// until this release is deployed, so the page's cost there is measured
	// after the deploy, not guessed now.
	out := make([]RepositoryHost, 0, len(hosts))
	for _, r := range hosts {
		im, err := s.repositoryHostImpact(ctx, r.Host)
		if err != nil {
			return nil, err
		}
		r.Impact = im
		out = append(out, *r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Host < out[j].Host })
	return out, nil
}
