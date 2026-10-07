// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"fmt"
	"time"
)

// repo_commit_daily (summary/49, 2026-10-06): one row per repository, UTC
// day of the author timestamp and author email, with the number of distinct
// commits. The facade folds it from the walk it already makes of the whole
// default branch and replaces the repository's rows after a completed walk;
// `aveloxis heal-commit-daily` fills repositories collected before that.
//
// Why: the commits table is one row per FILE per commit, and one
// repository's rows lie about one per page across the whole table (forty
// workers insert interleaved), so the two commit answers on the repository
// page — the weekly series and the commits arm of top contributors — read
// millions of scattered pages for a kernel fork (NVIDIA/nova: 118 s). The
// daily table is a few thousand rows for the same window; author identity
// is resolved as the next paragraph says (the alias rule R5 is its LAST
// step: a noreply address, the most common web-UI author, gets no alias).
//
// The readers use it only when the repository is filled AND the window is
// UTC-day aligned (a day bucket cannot be split); otherwise they read the
// commits table as before. Day buckets are exact for the API's windows,
// which are UTC days by construction (parseDayParam, parseWindow; the
// open-ended uppers and /contributors/elsewhere's default are UTC
// midnights too — review round 1 F5).
//
// Author identity (review rounds 1 F2 and 2 F1/F4): the old commits arm
// counted by cmt_ght_author_id, which the resolver's strategy 1 derives from
// a GitHub noreply address WITHOUT writing an alias row, so an alias-only
// join dropped every web-UI and squash-merge commit. A daily row therefore
// carries what its writer KNOWS, never a guess: the backfill carries the
// commits table's stored cmt_ght_author_id (cntrb_id; an ambiguous bucket
// carries none), the facade carries the login AND GitHub's numeric user id
// a noreply address names (author_login, author_gh_user_id — the id
// survives an account rename, the login does not; the contributor id the
// resolver WANTS for the login, PlatformUUID, is not the id it keeps when a
// legacy row already holds the login, so no contributor id is stored), and
// a replace keeps an id the table already knows. The reader resolves the
// rest at read time, each only when unambiguous among live contributors:
// the stored id, else the numeric id (contributors.gh_user_id), else the
// login through the backfill's own rule (LOWER(gh_login)), else the house
// email rule (ResolveContributorIDByEmail: the unambiguous direct match
// over the contributor's own emails, then the alias joined to a live
// contributor).

// CommitDailyRow is one (day, author) bucket of a repository's walk.
type CommitDailyRow struct {
	Day            time.Time // a UTC midnight
	AuthorEmail    string
	Commits        int
	AuthorLogin    string // the login a GitHub noreply address names; "" otherwise (the facade's writer)
	AuthorGhUserID int64  // GitHub's numeric user id when the noreply address carries one; 0 otherwise (survives a rename; the login does not)
	CntrbID        string // the stored identity when the writer knows it (uuid text; the backfill's writer); "" = unknown
}

// UTCDay is the UTC midnight that starts t's UTC day: the bound every
// default window must use so the daily commit table can serve it.
func UTCDay(t time.Time) time.Time { return utcDay(t) }

// repoCommitDailyLockSQL serialises the two writers per repository
// (review round 1 F4): the backfill's minutes-long scan and the facade's
// replace must not interleave — a 23505 is not a retried error, and the
// reverse order would leave the union of two walks.
const repoCommitDailyLockSQL = `SELECT pg_advisory_xact_lock(hashtextextended('repo_commit_daily:' || ($1::bigint)::text, 0))`

// utcDay is the UTC midnight that starts t's UTC day (zero stays zero).
func utcDay(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	u := t.UTC()
	return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC)
}

// alignedToUTCDay reports whether t is a UTC midnight (a zero time counts:
// it stands for "unbounded").
func alignedToUTCDay(t time.Time) bool {
	return t.IsZero() || t.UTC().Equal(utcDay(t))
}

// ReplaceRepoCommitDaily makes rows the repository's daily picture in one
// transaction under the per-repository lock: every (day, author) in rows is
// upserted — a known identity the walk could not supply is KEPT — and, when
// trim is set, every (day, author) not in rows is deleted, so a walk that
// saw nothing (an empty default branch) clears the table. The facade trims
// only after a walk whose every commit was proven written (review round 2
// F2): a walk that swallowed writes still records what it saw, so the rows
// stay fresh, but never removes rows it may have missed, and (round 3 F3)
// never lowers a bucket's count either — a commit whose rows failed this
// run still sits in the commits table from an earlier walk, so the fuller
// count stands until a clean walk. A row whose effective values are
// unchanged is not touched at all (round 4 F1, round 5 F1): it is left OUT
// of the INSERT by the NOT EXISTS — ON CONFLICT DO UPDATE locks every
// conflicting row before it evaluates its WHERE, and a tuple lock dirties
// the page and logs a record, so a DO UPDATE WHERE alone (kept below as a
// backstop) still wrote every page of a kernel fork's ~10^6 buckets on an
// identical walk — on a host whose storage stalls on writes (summary/37).
// The pre-filter is race-free: both writers hold the per-repository lock.
// Nothing is written for a walk that did not complete.
// Emails are scrubbed the way the commits rows' were (review round 1 F3),
// so the two tables carry the same string.
func (s *PostgresStore) ReplaceRepoCommitDaily(ctx context.Context, repoID int64, rows []CommitDailyRow, trim bool) error {
	days := make([]string, len(rows))
	emails := make([]string, len(rows))
	logins := make([]string, len(rows))
	uids := make([]int64, len(rows))
	counts := make([]int32, len(rows))
	ids := make([]*string, len(rows))
	for i, r := range rows {
		days[i] = utcDay(r.Day).Format("2006-01-02")
		emails[i] = SafeUTF8(r.AuthorEmail)
		logins[i] = SafeUTF8(r.AuthorLogin)
		uids[i] = r.AuthorGhUserID
		counts[i] = int32(r.Commits)
		if r.CntrbID != "" {
			id := r.CntrbID
			ids[i] = &id
		}
	}
	return s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, repoCommitDailyLockSQL, repoID); err != nil {
			return fmt.Errorf("lock repo_commit_daily: %w", err)
		}
		if len(rows) > 0 {
			if _, err := tx.Exec(ctx, `
				INSERT INTO aveloxis_data.repo_commit_daily (repo_id, day, author_email, commits, author_login, author_gh_user_id, cntrb_id)
				SELECT $1, u.d::date, u.e, u.c, u.l, u.g, u.i::uuid
				FROM unnest($2::text[], $3::text[], $4::int[], $5::text[], $6::bigint[], $7::text[]) AS u(d, e, c, l, g, i)
				WHERE NOT EXISTS (
				    SELECT 1 FROM aveloxis_data.repo_commit_daily x
				    WHERE x.repo_id = $1 AND x.day = u.d::date AND x.author_email = u.e
				      AND x.commits IS NOT DISTINCT FROM CASE WHEN $8::boolean THEN u.c ELSE GREATEST(x.commits, u.c) END
				      AND (u.l = '' OR x.author_login IS NOT DISTINCT FROM u.l)
				      AND (u.g = 0 OR x.author_gh_user_id IS NOT DISTINCT FROM u.g)
				      AND (u.i IS NULL OR x.cntrb_id IS NOT DISTINCT FROM u.i::uuid))
				ON CONFLICT (repo_id, day, author_email) DO UPDATE
				SET commits = CASE WHEN $8::boolean THEN EXCLUDED.commits ELSE GREATEST(repo_commit_daily.commits, EXCLUDED.commits) END,
				    author_login = CASE WHEN EXCLUDED.author_login <> '' THEN EXCLUDED.author_login ELSE repo_commit_daily.author_login END,
				    author_gh_user_id = CASE WHEN EXCLUDED.author_gh_user_id <> 0 THEN EXCLUDED.author_gh_user_id ELSE repo_commit_daily.author_gh_user_id END,
				    cntrb_id = COALESCE(EXCLUDED.cntrb_id, repo_commit_daily.cntrb_id),
				    computed_at = NOW()
				WHERE repo_commit_daily.commits IS DISTINCT FROM CASE WHEN $8::boolean THEN EXCLUDED.commits ELSE GREATEST(repo_commit_daily.commits, EXCLUDED.commits) END
				   OR (EXCLUDED.author_login <> '' AND repo_commit_daily.author_login IS DISTINCT FROM EXCLUDED.author_login)
				   OR (EXCLUDED.author_gh_user_id <> 0 AND repo_commit_daily.author_gh_user_id IS DISTINCT FROM EXCLUDED.author_gh_user_id)
				   OR (EXCLUDED.cntrb_id IS NOT NULL AND repo_commit_daily.cntrb_id IS DISTINCT FROM EXCLUDED.cntrb_id)`,
				repoID, days, emails, counts, logins, uids, ids, trim); err != nil {
				return fmt.Errorf("write repo_commit_daily: %w", err)
			}
		}
		if trim {
			if _, err := tx.Exec(ctx, `
				DELETE FROM aveloxis_data.repo_commit_daily d
				WHERE d.repo_id = $1
				  AND NOT EXISTS (SELECT 1 FROM unnest($2::text[], $3::text[]) AS u(dd, e)
				                  WHERE u.dd::date = d.day AND u.e = d.author_email)`,
				repoID, days, emails); err != nil {
				return fmt.Errorf("trim repo_commit_daily: %w", err)
			}
		}
		return tx.Commit(ctx)
	})
}

// RepoCommitDailyFilled reports whether the repository has daily rows. An
// error is an error (SR-5): the readers return it rather than silently
// taking the slow path.
func (s *PostgresStore) RepoCommitDailyFilled(ctx context.Context, repoID int64) (bool, error) {
	var filled bool
	err := s.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1)`, repoID).Scan(&filled)
	if err != nil {
		return false, fmt.Errorf("repo_commit_daily probe: %w", err)
	}
	return filled, nil
}

// FillRepoCommitDailyFromCommits computes the repository's daily rows from
// the commits table — one scan of its rows, the cost today's first page view
// pays — and replaces them in one transaction under the per-repository
// lock, carrying each bucket's stored identity (cmt_ght_author_id) when its
// resolved commits agree on one; a bucket whose commits carry two ids is
// ambiguous and carries none (SR-6; review round 2 F5). The heal command's
// unit of work: an interrupt loses at most the repository in flight.
// Returns the rows written.
func (s *PostgresStore) FillRepoCommitDailyFromCommits(ctx context.Context, repoID int64) (int64, error) {
	var written int64
	err := s.withRetry(ctx, func(ctx context.Context) error {
		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, repoCommitDailyLockSQL, repoID); err != nil {
			return fmt.Errorf("lock repo_commit_daily: %w", err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM aveloxis_data.repo_commit_daily WHERE repo_id = $1`, repoID); err != nil {
			return fmt.Errorf("clear repo_commit_daily: %w", err)
		}
		tag, err := tx.Exec(ctx, `
			INSERT INTO aveloxis_data.repo_commit_daily (repo_id, day, author_email, commits, cntrb_id)
			SELECT repo_id, (cmt_author_timestamp AT TIME ZONE 'UTC')::date, cmt_author_email,
			       COUNT(DISTINCT cmt_commit_hash),
			       CASE WHEN COUNT(DISTINCT cmt_ght_author_id) = 1 THEN MIN(cmt_ght_author_id::text)::uuid END
			FROM aveloxis_data.commits
			WHERE repo_id = $1 AND cmt_author_timestamp IS NOT NULL
			GROUP BY 1, 2, 3
			ON CONFLICT (repo_id, day, author_email) DO UPDATE
			SET commits = EXCLUDED.commits, cntrb_id = EXCLUDED.cntrb_id, computed_at = NOW()`, repoID)
		if err != nil {
			return fmt.Errorf("fill repo_commit_daily from commits: %w", err)
		}
		written = tag.RowsAffected()
		return tx.Commit(ctx)
	})
	return written, err
}

// ListReposNeedingCommitDaily lists repositories that have dated commit rows
// but no daily rows, largest first (the queue's last commit count), up to
// limit. The largest are the ones whose page times out.
func (s *PostgresStore) ListReposNeedingCommitDaily(ctx context.Context, limit int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id
		FROM aveloxis_data.repos r
		LEFT JOIN aveloxis_ops.collection_queue q ON q.repo_id = r.repo_id
		WHERE EXISTS (SELECT 1 FROM aveloxis_data.commits c WHERE c.repo_id = r.repo_id AND c.cmt_author_timestamp IS NOT NULL)
		  AND NOT EXISTS (SELECT 1 FROM aveloxis_data.repo_commit_daily d WHERE d.repo_id = r.repo_id)
		ORDER BY COALESCE(q.last_commits, 0) DESC, r.repo_id
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list repositories needing repo_commit_daily: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// dailyCommitsArmSQL is the commits arm of TopContributors read from the
// daily table: distinct commits per (day, email) summed over the window,
// each row attributed in this order — its stored identity (the backfill's);
// GitHub's numeric user id a noreply address carried (contributors.
// gh_user_id: the id survives a rename, the login does not — round 3 F2);
// the login it named, through the backfill's own rule (LOWER(gh_login),
// one live contributor); then the house email rule exactly as
// ResolveContributorIDByEmail applies it: the unambiguous direct match over
// the contributor's own emails among live contributors, then the alias
// joined to a live contributor (alias_email is UNIQUE). Every lookup is a
// scalar subquery that yields at most one row: a LEFT JOIN on the
// non-unique email columns fanned a row out across every claimant and
// summed it per claimant, and crediting any of several is fabricated
// identity (SR-6 — an email two contributors claim attributes to neither).
// The id and login lookups carry the LITERAL guards their partial indexes
// are built on (a non-zero gh_user_id; a non-empty gh_login): the planner cannot prove
// them from the equality, and without them each lookup is a sequential
// scan of contributors per daily row (round 3 F1; the backfill's v0.27.25
// lesson). $1 repo, $2/$3 the window as timestamptz UTC midnights.
const dailyCommitsArmSQL = `
    SELECT i.cntrb_id,
           SUM(i.commits)::bigint AS commits,
           0::bigint AS issues, 0::bigint AS prs, 0::bigint AS reviews, 0::bigint AS comments
    FROM (
        SELECT COALESCE(
                   d.cntrb_id,
                   (SELECT MIN(c.cntrb_id::text)::uuid FROM aveloxis_data.contributors c
                     WHERE d.author_gh_user_id <> 0 AND c.gh_user_id = d.author_gh_user_id AND c.gh_user_id <> 0
                       AND COALESCE(c.cntrb_deleted, 0) = 0
                     HAVING COUNT(DISTINCT c.cntrb_id) = 1),
                   (SELECT MIN(c.cntrb_id::text)::uuid FROM aveloxis_data.contributors c
                     WHERE d.author_login <> '' AND LOWER(c.gh_login) = LOWER(d.author_login) AND c.gh_login <> ''
                       AND COALESCE(c.cntrb_deleted, 0) = 0
                     HAVING COUNT(DISTINCT c.cntrb_id) = 1),
                   (SELECT t.cntrb_id FROM (
                        SELECT (array_agg(DISTINCT cntrb_id::text))[1]::uuid AS cntrb_id,
                               count(DISTINCT cntrb_id) AS n
                        FROM aveloxis_data.contributors
                        WHERE (cntrb_email = d.author_email OR cntrb_canonical = d.author_email)
                          AND COALESCE(cntrb_deleted, 0) = 0
                    ) t WHERE t.n = 1),
                   (SELECT c.cntrb_id
                    FROM aveloxis_data.contributors_aliases a
                    JOIN aveloxis_data.contributors c ON c.cntrb_id = a.cntrb_id
                    WHERE a.alias_email = d.author_email
                      AND COALESCE(c.cntrb_deleted, 0) = 0
                    LIMIT 1)
               ) AS cntrb_id,
               d.commits
        FROM aveloxis_data.repo_commit_daily d
        WHERE d.repo_id = $1
          AND d.day >= (($2::timestamptz) AT TIME ZONE 'UTC')::date
          AND d.day <  (($3::timestamptz) AT TIME ZONE 'UTC')::date
    ) i
    WHERE i.cntrb_id IS NOT NULL
    GROUP BY 1`

// dailyWeeklyCommitsSQL is the weekly commit series read from the daily
// table: a commit is in exactly one UTC day and so in exactly one UTC week,
// which is the same Monday date_trunc gives the timestamp form.
const dailyWeeklyCommitsSQL = `
		SELECT date_trunc('week', d.day::timestamp) AS week_start,
			SUM(d.commits) AS cnt
		FROM aveloxis_data.repo_commit_daily d
		WHERE d.repo_id = $1
		  AND d.day >= (($2::timestamptz) AT TIME ZONE 'UTC')::date
		  AND d.day <  (($3::timestamptz) AT TIME ZONE 'UTC')::date
		GROUP BY week_start
		ORDER BY week_start`

// commitDailyServes reports whether the daily table answers this window for
// this repository: filled, and both bounds UTC-day aligned. A probe error
// is returned (SR-5).
func (s *PostgresStore) commitDailyServes(ctx context.Context, repoID int64, lower, upper time.Time) (bool, error) {
	if !alignedToUTCDay(lower) || !alignedToUTCDay(upper) {
		return false, nil
	}
	return s.RepoCommitDailyFilled(ctx, repoID)
}
