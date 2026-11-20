// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/model"
)

// replaceDeclaredRepositories replaces, inside the inventory's replace, what
// is recorded of the package repositories a repository's build files declare
// (v0.29.184; summary/70, 4.5): the previous rows go and declared take their
// place.
func replaceDeclaredRepositories(ctx context.Context, tx pgx.Tx, repoID int64, declared []model.DeclaredRepository) error {
	if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.repo_declared_repositories WHERE repo_id = $1`, repoID); err != nil {
		return err
	}
	batch := &pgx.Batch{}
	for _, d := range declared {
		batch.Queue(`
			INSERT INTO aveloxis_data.repo_declared_repositories
				(repo_id, manifest_path, repository_url, host, used_for, not_fetched_reason)
			VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))
			ON CONFLICT (repo_id, manifest_path, repository_url, used_for) DO NOTHING`,
			repoID, d.ManifestPath, d.URL, d.Host, d.Kind, d.NotFetched)
	}
	if batch.Len() == 0 {
		return nil
	}
	return tx.SendBatch(ctx, batch).Close()
}

// GetRepoDeclaredRepositories reads them back, by manifest and address.
func (s *PostgresStore) GetRepoDeclaredRepositories(ctx context.Context, repoID int64) ([]model.DeclaredRepository, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT manifest_path, repository_url, host, used_for, COALESCE(not_fetched_reason, '')
		FROM aveloxis_data.repo_declared_repositories WHERE repo_id = $1
		ORDER BY manifest_path, repository_url, used_for`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.DeclaredRepository
	for rows.Next() {
		var d model.DeclaredRepository
		if err := rows.Scan(&d.ManifestPath, &d.URL, &d.Host, &d.Kind, &d.NotFetched); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}
