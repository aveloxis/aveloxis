// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"time"

	"github.com/aveloxis/aveloxis/internal/hostguard"
	"github.com/jackc/pgx/v5"
)

// MavenPomResult is one stored answer of the Maven resolver (v0.29.182;
// summary/70, 4.5): what one repository said about one pom.
type MavenPomResult struct {
	// Found is true for a pom that was obtained; Extract is then what was
	// extracted from it (manifest.RemotePom, as JSON).
	Found   bool
	Extract []byte
	// Expired is true when the answer is due to be asked again. A due
	// answer is still the stored answer: it is what is used when asking
	// again fails.
	Expired bool
}

// GetMavenPomResult reads the stored answer for one pom on one repository.
// stored is false when nothing was ever stored for it.
func (s *PostgresStore) GetMavenPomResult(ctx context.Context, repository, group, artifact, version string) (res MavenPomResult, stored bool, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT found, extract, expires_at IS NOT NULL AND expires_at <= NOW()
		FROM aveloxis_data.maven_pom_results
		WHERE repository_url = $1 AND group_id = $2 AND artifact_id = $3 AND version = $4`,
		repository, group, artifact, version).Scan(&res.Found, &res.Extract, &res.Expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return MavenPomResult{}, false, nil
	}
	if err != nil {
		return MavenPomResult{}, false, err
	}
	return res, true, nil
}

// PutMavenPomResult stores an ANSWER: a pom that was obtained (found, with
// its extract) or a repository's statement that it has none. ttl is how long
// the answer holds before it is due again; zero or less means it does not
// expire. A lookup that got no answer is never stored.
func (s *PostgresStore) PutMavenPomResult(ctx context.Context, repository, group, artifact, version string, found bool, extract []byte, ttl time.Duration) error {
	var seconds *float64
	if ttl > 0 {
		v := ttl.Seconds()
		seconds = &v
	}
	// v0.29.186: nothing is stored for a distrusted host, and that is
	// decided in the statement, so a fetch that was in flight when the host
	// was distrusted cannot put back what a purge removed.
	host := repositoryHost(repository)
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_data.maven_pom_results
				(repository_url, group_id, artifact_id, version, found, extract, fetched_at, expires_at)
			SELECT $1, $2, $3, $4, $5, $6::jsonb, NOW(), NOW() + make_interval(secs => $7::float8)
			WHERE NOT EXISTS (SELECT 1 FROM aveloxis_ops.distrusted_repository_hosts d WHERE d.host = $8)
			ON CONFLICT (repository_url, group_id, artifact_id, version) DO UPDATE SET
				found = EXCLUDED.found,
				extract = EXCLUDED.extract,
				fetched_at = EXCLUDED.fetched_at,
				expires_at = EXCLUDED.expires_at`,
			repository, group, artifact, version, found, extract, seconds, host)
		return err
	})
}

// NotePackageRepositoryHostAnswered records that a package repository host
// answered a request for a pom (v0.29.185): the first time, when; and each
// time, that it still does.
func (s *PostgresStore) NotePackageRepositoryHostAnswered(ctx context.Context, host string) error {
	if host == "" {
		return nil
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		_, err := s.pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.package_repository_hosts (host) VALUES ($1)
			ON CONFLICT (host) DO UPDATE SET last_answered_at = NOW()`, host)
		return err
	})
}

// repositoryHost is a repository URL's host as the distrust list spells it
// (hostguard.Normalize: lower case, with its port).
func repositoryHost(repository string) string { return hostguard.OfURL(repository) }

// GetDistrustedRepositoryHosts reads the distrusted list.
func (s *PostgresStore) GetDistrustedRepositoryHosts(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT host FROM aveloxis_ops.distrusted_repository_hosts ORDER BY host`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var h string
		if err := rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// DistrustRepositoryHost puts a host on the distrusted list; changed is
// false when it was there already (the earlier entry stands). by is who
// decided it.
func (s *PostgresStore) DistrustRepositoryHost(ctx context.Context, host, by, note string) (changed bool, err error) {
	host = hostguard.Normalize(host)
	if host == "" {
		return false, errors.New("no host to distrust")
	}
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.distrusted_repository_hosts (host, distrusted_by, note)
		VALUES ($1, $2, $3) ON CONFLICT (host) DO NOTHING`, host, by, note)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// RestoreRepositoryHost takes a host off the distrusted list; changed is
// false when it was not on it.
//
// It first deletes whatever is stored from the host (0.29.208). The purge
// at distrust removes what was stored then, but a write that began just
// before that decision committed does not see it (its NOT EXISTS reads its
// own snapshot) and lands after the purge. Nothing serves such a row while
// the host is distrusted; removing it here, in the transaction that ends
// the distrust and while writes for the host are still refused, is what
// keeps a restore from putting it back into use. A restored host starts
// from nothing stored.
func (s *PostgresStore) RestoreRepositoryHost(ctx context.Context, host string) (changed bool, err error) {
	host = hostguard.Normalize(host)
	err = s.withRetry(ctx, func(ctx context.Context) error {
		changed = false
		urls, err := s.repositoryURLsOfHost(ctx, host)
		if err != nil {
			return err
		}
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		tag, err := tx.Exec(ctx, `DELETE FROM aveloxis_ops.distrusted_repository_hosts WHERE host = $1`, host)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			return tx.Commit(ctx) // not distrusted: nothing is touched
		}
		if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.maven_pom_results WHERE repository_url = ANY($1)`, urls); err != nil {
			return err
		}
		changed = true
		return tx.Commit(ctx)
	})
	return changed, err
}
