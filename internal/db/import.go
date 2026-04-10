package db

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// AugurRepo represents a row from augur_data.repo.
type AugurRepo struct {
	RepoID   int64
	RepoGit  string
	RepoName string
	// Augur stores the platform implicitly via URL convention, not a column.
}

// LoadAugurRepos reads all repos from augur_data.repo.
func LoadAugurRepos(ctx context.Context, pool *pgxpool.Pool) ([]AugurRepo, error) {
	rows, err := pool.Query(ctx, `
		SELECT repo_id, repo_git, COALESCE(repo_name, '')
		FROM augur_data.repo
		WHERE repo_git IS NOT NULL AND repo_git != ''
		ORDER BY repo_id`)
	if err != nil {
		return nil, fmt.Errorf("querying augur_data.repo: %w", err)
	}
	defer rows.Close()

	var repos []AugurRepo
	for rows.Next() {
		var r AugurRepo
		if err := rows.Scan(&r.RepoID, &r.RepoGit, &r.RepoName); err != nil {
			return nil, err
		}
		repos = append(repos, r)
	}
	return repos, rows.Err()
}
