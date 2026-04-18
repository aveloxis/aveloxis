package db

import (
	"context"
	"fmt"
)

// GetUserIDByGHLogin looks up an aveloxis_ops.users row by GitHub login so
// `aveloxis import-foundations` can attach imported repos to the operator's
// dashboard groups. Returns an error (not zero) if no matching user exists
// so callers can decide whether to create the user or abort.
func (s *PostgresStore) GetUserIDByGHLogin(ctx context.Context, ghLogin string) (int, error) {
	var userID int
	err := s.pool.QueryRow(ctx,
		`SELECT user_id FROM aveloxis_ops.users WHERE gh_login = $1 LIMIT 1`,
		ghLogin).Scan(&userID)
	if err != nil {
		return 0, fmt.Errorf("no aveloxis user with gh_login=%q (run `aveloxis web`, log in once, then retry): %w", ghLogin, err)
	}
	return userID, nil
}

// UpsertFoundationMembership records that a repo belongs to a foundation
// (CNCF / Apache) at a particular maturity level (graduated / incubating /
// sandbox). The primary key (foundation, project_name, repo_url) means
// a second import with the same triple is a no-op on the identity but
// refreshes the homepage URL and imported_at timestamp so operators can
// see when membership was last reconciled.
//
// Used by `aveloxis import-foundations` after it enqueues a repo. The
// membership table is queryable independently of the queue: even if a
// repo is archived or removed from the queue, the membership row stays
// as a historical record of what the foundation listed and when.
func (s *PostgresStore) UpsertFoundationMembership(ctx context.Context,
	foundation, status, projectName, homepageURL, repoURL string,
) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.foundation_membership
		    (foundation, status, project_name, homepage_url, repo_url)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (foundation, project_name, repo_url) DO UPDATE
		    SET status       = EXCLUDED.status,
		        homepage_url = EXCLUDED.homepage_url,
		        imported_at  = NOW()
	`, foundation, status, projectName, homepageURL, repoURL)
	return err
}
