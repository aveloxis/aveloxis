package db

import (
	"context"
)

// UnresolvedCommit is a commit needing author resolution.
type UnresolvedCommit struct {
	Hash  string
	Email string // cmt_author_raw_email
}

// GetUnresolvedCommits returns distinct (hash, email) pairs for commits where
// cmt_author_platform_username is NULL in the given repo.
func (s *PostgresStore) GetUnresolvedCommits(ctx context.Context, repoID int64) ([]UnresolvedCommit, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT cmt_commit_hash, cmt_author_raw_email
		FROM aveloxis_data.commits
		WHERE repo_id = $1
		  AND (cmt_author_platform_username IS NULL OR cmt_author_platform_username = '')
		ORDER BY cmt_commit_hash`, repoID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []UnresolvedCommit
	for rows.Next() {
		var c UnresolvedCommit
		if err := rows.Scan(&c.Hash, &c.Email); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// SetCommitAuthorLogin sets cmt_author_platform_username on all commit rows
// matching the given repo + hash.
func (s *PostgresStore) SetCommitAuthorLogin(ctx context.Context, repoID int64, hash, login string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.commits
		SET cmt_author_platform_username = $3
		WHERE repo_id = $1
		  AND cmt_commit_hash = $2
		  AND (cmt_author_platform_username IS NULL OR cmt_author_platform_username = '')`,
		repoID, hash, login)
	return err
}

// FindLoginByEmail looks up a GitHub login from a commit email.
// Checks contributors table (cntrb_email, cntrb_canonical) and aliases.
func (s *PostgresStore) FindLoginByEmail(ctx context.Context, email string) (string, error) {
	var login string

	// Check contributors by email.
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(gh_login, cntrb_login)
		FROM aveloxis_data.contributors
		WHERE (cntrb_email = $1 OR cntrb_canonical = $1)
		  AND (gh_login IS NOT NULL AND gh_login != '')
		LIMIT 1`, email).Scan(&login)
	if err == nil && login != "" {
		return login, nil
	}

	// Check aliases.
	err = s.pool.QueryRow(ctx, `
		SELECT COALESCE(c.gh_login, c.cntrb_login)
		FROM aveloxis_data.contributors_aliases a
		JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id
		WHERE a.alias_email = $1
		  AND (c.gh_login IS NOT NULL AND c.gh_login != '')
		LIMIT 1`, email).Scan(&login)
	if err == nil && login != "" {
		return login, nil
	}

	return "", nil
}

// UpsertContributorFull creates or updates a contributor with the deterministic
// GithubUUID and sets gh_login. Returns true if a new row was created.
func (s *PostgresStore) UpsertContributorFull(ctx context.Context, cntrbID, login string, ghUserID int64, commitEmail string) (bool, string, error) {
	var created bool
	actualID := cntrbID
	err := s.withRetry(ctx, func(ctx context.Context) error {
		// Check by cntrb_id first.
		var existing int
		err := s.pool.QueryRow(ctx,
			`SELECT 1 FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid`, cntrbID,
		).Scan(&existing)

		if err != nil {
			// Not found by ID. Check if login already exists (different cntrb_id).
			var existingID string
			loginErr := s.pool.QueryRow(ctx,
				`SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
				login).Scan(&existingID)
			if loginErr == nil {
				// Login exists under a different cntrb_id — update that row instead.
				actualID = existingID
				_, err = s.pool.Exec(ctx, `
					UPDATE aveloxis_data.contributors
					SET gh_user_id = COALESCE(gh_user_id, $2),
					    gh_login = $3,
					    cntrb_canonical = COALESCE(NULLIF(cntrb_canonical,''), $4),
					    data_collection_date = NOW()
					WHERE cntrb_id = $1::uuid`,
					existingID, ghUserID, login, commitEmail)
				created = false
				return err
			}

			// Truly new — insert. If another worker raced us and the login now
			// exists, catch the error and look up their row instead.
			_, insertErr := s.pool.Exec(ctx, `
				INSERT INTO aveloxis_data.contributors
					(cntrb_id, cntrb_login, gh_login, gh_user_id, cntrb_canonical,
					 tool_source, data_source, data_collection_date)
				VALUES ($1::uuid, $2, $2, $3, $4,
					'aveloxis-commit-resolver', 'GitHub API', NOW())
				ON CONFLICT (cntrb_id) DO UPDATE SET
					gh_login = COALESCE(NULLIF(EXCLUDED.gh_login,''), contributors.gh_login),
					cntrb_login = COALESCE(NULLIF(EXCLUDED.cntrb_login,''), contributors.cntrb_login),
					gh_user_id = COALESCE(EXCLUDED.gh_user_id, contributors.gh_user_id)`,
				cntrbID, login, ghUserID, commitEmail)
			if insertErr != nil {
				// Race: another worker inserted this login. Look it up.
				var raceID string
				if lookupErr := s.pool.QueryRow(ctx,
					`SELECT cntrb_id FROM aveloxis_data.contributors WHERE cntrb_login = $1`,
					login).Scan(&raceID); lookupErr == nil {
					actualID = raceID
					created = false
					return nil
				}
				return insertErr
			}
			actualID = cntrbID
			created = true
			return nil
		}

		// Row exists by ID — update login (may have changed) and backfill gh_user_id.
		_, err = s.pool.Exec(ctx, `
			UPDATE aveloxis_data.contributors
			SET gh_login = $2,
			    cntrb_login = $2,
			    gh_user_id = COALESCE(gh_user_id, $3),
			    cntrb_canonical = COALESCE(NULLIF(cntrb_canonical,''), $4),
			    data_collection_date = NOW()
			WHERE cntrb_id = $1::uuid`,
			cntrbID, login, ghUserID, commitEmail)
		actualID = cntrbID
		created = false
		return err
	})
	// Backfill gh_* columns from contributor_identities if not already set.
	if err == nil && actualID != "" {
		s.backfillGHColumns(ctx, actualID)
	}
	return created, actualID, err
}

// backfillGHColumns copies GitHub identity data to the denormalized gh_* columns
// on the contributors row if they're empty.
func (s *PostgresStore) backfillGHColumns(ctx context.Context, cntrbID string) {
	s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors c SET
			gh_node_id = COALESCE(NULLIF(c.gh_node_id,''), ci.node_id),
			gh_avatar_url = COALESCE(NULLIF(c.gh_avatar_url,''), ci.avatar_url),
			gh_url = COALESCE(NULLIF(c.gh_url,''), ci.profile_url),
			gh_html_url = COALESCE(NULLIF(c.gh_html_url,''), ci.profile_url),
			gh_type = COALESCE(NULLIF(c.gh_type,''), ci.user_type)
		FROM aveloxis_data.contributor_identities ci
		WHERE ci.cntrb_id = c.cntrb_id
		  AND c.cntrb_id = $1::uuid
		  AND ci.platform_id = 1
		  AND (c.gh_node_id IS NULL OR c.gh_node_id = ''
		    OR c.gh_avatar_url IS NULL OR c.gh_avatar_url = '')`,
		cntrbID)
}

// InsertUnresolvedEmail records a commit email that could not be resolved to
// any platform user. Stored for future resolution attempts or manual review.
// Uses a single INSERT with a duplicate check in the WHERE clause to avoid
// the race condition inherent in check-then-insert.
func (s *PostgresStore) InsertUnresolvedEmail(ctx context.Context, email string) {
	s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.unresolved_commit_emails
			(email, tool_source, tool_version, data_source, data_collection_date)
		SELECT $1, 'aveloxis-commit-resolver', $2, 'git', NOW()
		WHERE NOT EXISTS (
			SELECT 1 FROM aveloxis_data.unresolved_commit_emails WHERE email = $1
		)`,
		email, ToolVersion)
}

// EnsureContributorAlias creates an alias linking a commit email to a contributor.
// The canonical_email is looked up from the contributor row; alias_email is the
// commit email that differs from the canonical.
func (s *PostgresStore) EnsureContributorAlias(ctx context.Context, cntrbID, aliasEmail string) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO aveloxis_data.contributors_aliases
			(cntrb_id, canonical_email, alias_email, cntrb_active,
			 tool_source, data_source, data_collection_date)
		VALUES (
			$1::uuid,
			COALESCE(
				(SELECT cntrb_canonical FROM aveloxis_data.contributors WHERE cntrb_id = $1::uuid),
				$2
			),
			$2, 1,
			'aveloxis-commit-resolver', 'GitHub API', NOW())
		ON CONFLICT (alias_email) DO NOTHING`,
		cntrbID, aliasEmail)
	return err
}

// FindContributorIDByLogin returns the cntrb_id for a given gh_login, or "" if not found.
func (s *PostgresStore) FindContributorIDByLogin(ctx context.Context, login string) (string, error) {
	var id string
	err := s.pool.QueryRow(ctx,
		`SELECT cntrb_id::text FROM aveloxis_data.contributors WHERE gh_login = $1 LIMIT 1`,
		login).Scan(&id)
	if err != nil {
		return "", nil
	}
	return id, nil
}

// BackfillCommitAuthorIDs sets cmt_ght_author_id from contributor gh_login matches.
// This is a pure SQL operation — no API calls.
func (s *PostgresStore) BackfillCommitAuthorIDs(ctx context.Context, repoID int64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.commits c
		SET cmt_ght_author_id = cn.cntrb_id
		FROM aveloxis_data.contributors cn
		WHERE c.repo_id = $1
		  AND c.cmt_author_platform_username = cn.gh_login
		  AND c.cmt_ght_author_id IS NULL
		  AND c.cmt_author_platform_username IS NOT NULL`,
		repoID)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ContributorMissingCanonical is a contributor needing email enrichment.
type ContributorMissingCanonical struct {
	ID    string // cntrb_id
	Login string // gh_login
}

// GetContributorsMissingCanonical returns contributors that have gh_login
// but no cntrb_canonical email.
func (s *PostgresStore) GetContributorsMissingCanonical(ctx context.Context) ([]ContributorMissingCanonical, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT cntrb_id::text, gh_login
		FROM aveloxis_data.contributors
		WHERE gh_login IS NOT NULL AND gh_login != ''
		  AND (cntrb_canonical IS NULL OR length(cntrb_canonical) < 2)
		ORDER BY gh_login`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ContributorMissingCanonical
	for rows.Next() {
		var c ContributorMissingCanonical
		if err := rows.Scan(&c.ID, &c.Login); err != nil {
			return nil, err
		}
		result = append(result, c)
	}
	return result, rows.Err()
}

// SetContributorCanonical sets cntrb_canonical on a contributor.
func (s *PostgresStore) SetContributorCanonical(ctx context.Context, cntrbID, email string) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_data.contributors
		SET cntrb_canonical = $2
		WHERE cntrb_id = $1::uuid
		  AND (cntrb_canonical IS NULL OR length(cntrb_canonical) < 2)`,
		cntrbID, email)
	return err
}
