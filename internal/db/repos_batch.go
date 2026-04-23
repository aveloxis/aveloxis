package db

import (
	"context"

	"github.com/augurlabs/aveloxis/internal/model"
)

// GetReposBatch returns the repos for a slice of IDs in a single round-trip.
// Used by the monitor dashboard to avoid an N+1 GetRepoByID loop: prior to
// this, a large fleet (thousands+ of queue rows) fired one query per row
// on every 10-second meta-refresh, which both slowed the dashboard and
// competed with collection workers for pgx pool connections.
//
// Unknown IDs are simply absent from the returned map — callers are
// expected to look up by ID and skip missing entries. The result map is
// always non-nil, even for empty input, so callers don't need a nil check.
func (s *PostgresStore) GetReposBatch(ctx context.Context, repoIDs []int64) (map[int64]*model.Repo, error) {
	result := make(map[int64]*model.Repo, len(repoIDs))
	if len(repoIDs) == 0 {
		return result, nil
	}

	rows, err := s.pool.Query(ctx, `
		SELECT repo_id, platform_id, repo_git, repo_name, repo_owner
		FROM aveloxis_data.repos
		WHERE repo_id = ANY($1)`, repoIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var id int64
		var platID int16
		r := &model.Repo{}
		if err := rows.Scan(&id, &platID, &r.GitURL, &r.Name, &r.Owner); err != nil {
			return nil, err
		}
		r.ID = id
		r.Platform = model.Platform(platID)
		result[id] = r
	}
	return result, rows.Err()
}
