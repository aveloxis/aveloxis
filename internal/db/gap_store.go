package db

import (
	"context"
	"sort"
)

// GetCollectedIssueNumbers returns all issue numbers collected for a repo,
// sorted ascending. Used by gap detection to compare against API listing.
func (s *PostgresStore) GetCollectedIssueNumbers(ctx context.Context, repoID int64) ([]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT issue_number FROM aveloxis_data.issues WHERE repo_id = $1 ORDER BY issue_number`,
		repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var numbers []int
	for rows.Next() {
		var n int
		if rows.Scan(&n) == nil {
			numbers = append(numbers, n)
		}
	}
	sort.Ints(numbers)
	return numbers, rows.Err()
}

// GetCollectedPRNumbers returns all PR numbers collected for a repo,
// sorted ascending. Used by gap detection to compare against API listing.
func (s *PostgresStore) GetCollectedPRNumbers(ctx context.Context, repoID int64) ([]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT pr_number FROM aveloxis_data.pull_requests WHERE repo_id = $1 ORDER BY pr_number`,
		repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var numbers []int
	for rows.Next() {
		var n int
		if rows.Scan(&n) == nil {
			numbers = append(numbers, n)
		}
	}
	sort.Ints(numbers)
	return numbers, rows.Err()
}

// GetRepoMetaCounts returns the metadata issue and PR counts from repo_info.
// Used by gap detection to compare gathered vs expected counts.
// Must be called after collectAndProcess — repo_info is populated during
// staged processing from the GraphQL/REST metadata API call.
func (s *PostgresStore) GetRepoMetaCounts(ctx context.Context, repoID int64) (issues, prs int64, err error) {
	err = s.pool.QueryRow(ctx, `
		SELECT COALESCE(issues_count, 0), COALESCE(pr_count, 0)
		FROM aveloxis_data.repo_info
		WHERE repo_id = $1
		ORDER BY data_collection_date DESC
		LIMIT 1`, repoID).Scan(&issues, &prs)
	return
}

// GetOpenIssueNumbers returns issue numbers for all open issues in a repo.
// Used by the open item refresh to re-fetch issues whose status/labels/assignees
// may have changed since last collection.
func (s *PostgresStore) GetOpenIssueNumbers(ctx context.Context, repoID int64) ([]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT issue_number FROM aveloxis_data.issues WHERE repo_id = $1 AND issue_state = 'open' ORDER BY issue_number`,
		repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var numbers []int
	for rows.Next() {
		var n int
		if rows.Scan(&n) == nil {
			numbers = append(numbers, n)
		}
	}
	return numbers, rows.Err()
}

// GetOpenPRNumbers returns PR numbers for all open PRs in a repo.
// Used by the open item refresh to re-fetch PRs whose status/labels/assignees/
// reviews may have changed since last collection.
func (s *PostgresStore) GetOpenPRNumbers(ctx context.Context, repoID int64) ([]int, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT pr_number FROM aveloxis_data.pull_requests WHERE repo_id = $1 AND pr_state = 'open' ORDER BY pr_number`,
		repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var numbers []int
	for rows.Next() {
		var n int
		if rows.Scan(&n) == nil {
			numbers = append(numbers, n)
		}
	}
	return numbers, rows.Err()
}
