package db

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// BreadthContributor is a contributor needing breadth collection.
type BreadthContributor struct {
	ID    string // cntrb_id
	Login string // gh_login
}

// ContributorRepoRow is a row to insert into contributor_repo.
type ContributorRepoRow struct {
	CntrbID   string
	RepoGit   string
	RepoName  string
	GHRepoID  int64
	Category  string // event type (PushEvent, PullRequestEvent, etc.)
	EventID   int64
	CreatedAt time.Time
}

// GetContributorsForBreadth returns contributors that need breadth collection,
// prioritizing those that have never been processed, then oldest.
func (s *PostgresStore) GetContributorsForBreadth(ctx context.Context, limit int) ([]BreadthContributor, error) {
	if limit <= 0 {
		limit = 100
	}
	// Use a subquery to avoid the DISTINCT + ORDER BY mismatch.
	// PostgreSQL requires ORDER BY columns in the SELECT list with DISTINCT.
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_id, gh_login FROM (
			SELECT DISTINCT ON (c.cntrb_id) c.cntrb_id::text, c.gh_login, cr.last_collected
			FROM aveloxis_data.contributors c
			LEFT JOIN (
				SELECT cntrb_id, MAX(data_collection_date) AS last_collected
				FROM aveloxis_data.contributor_repo
				GROUP BY cntrb_id
			) cr ON cr.cntrb_id = c.cntrb_id
			WHERE c.gh_login IS NOT NULL AND c.gh_login != ''
			ORDER BY c.cntrb_id, cr.last_collected ASC NULLS FIRST
		) sub
		ORDER BY last_collected ASC NULLS FIRST
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []BreadthContributor
	for rows.Next() {
		var c BreadthContributor
		if err := rows.Scan(&c.ID, &c.Login); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// GetNewestContributorRepoEvent returns the most recent event timestamp
// for a contributor in contributor_repo. Returns zero time if none exist.
func (s *PostgresStore) GetNewestContributorRepoEvent(ctx context.Context, cntrbID string) (time.Time, error) {
	var t time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(created_at), '0001-01-01'::timestamptz)
		FROM aveloxis_data.contributor_repo
		WHERE cntrb_id = $1::uuid`, cntrbID).Scan(&t)
	if err != nil {
		return time.Time{}, nil
	}
	if t.Year() < 1970 {
		return time.Time{}, nil
	}
	return t, nil
}

// InsertContributorRepoBatch inserts multiple contributor-repo events in a single
// round-trip. Breadth collection can generate hundreds of events per contributor,
// so batching provides a significant speedup over individual inserts.
func (s *PostgresStore) InsertContributorRepoBatch(ctx context.Context, rows []*ContributorRepoRow) error {
	if len(rows) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, row := range rows {
		batch.Queue(`
			INSERT INTO aveloxis_data.contributor_repo
				(cntrb_id, repo_git, repo_name, gh_repo_id, cntrb_category,
				 event_id, created_at,
				 tool_source, tool_version, data_source, data_collection_date)
			VALUES ($1::uuid, $2, $3, $4, $5, $6, $7,
				'aveloxis-breadth', $8, 'GitHub API', NOW())
			ON CONFLICT (event_id, tool_version) DO NOTHING`,
			row.CntrbID, row.RepoGit, row.RepoName, row.GHRepoID,
			row.Category, row.EventID, row.CreatedAt, ToolVersion)
	}
	return s.pool.SendBatch(ctx, batch).Close()
}

// InsertContributorRepo inserts a contributor-repo event. Returns nil on
// duplicate (ON CONFLICT DO NOTHING).
func (s *PostgresStore) InsertContributorRepo(ctx context.Context, row *ContributorRepoRow) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributor_repo
			(cntrb_id, repo_git, repo_name, gh_repo_id, cntrb_category,
			 event_id, created_at,
			 tool_source, tool_version, data_source, data_collection_date)
		VALUES ($1::uuid, $2, $3, $4, $5, $6, $7,
			'aveloxis-breadth', $8, 'GitHub API', NOW())
		ON CONFLICT (event_id, tool_version) DO NOTHING`,
		row.CntrbID, row.RepoGit, row.RepoName, row.GHRepoID,
		row.Category, row.EventID, row.CreatedAt, ToolVersion)
	return err
}
