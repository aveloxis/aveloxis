// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/aveloxis/aveloxis/internal/model"
)

// errTrafficNoSnapshotDate refuses a traffic read whose snapshot date
// was never set: the date keys which path and referrer rows the read
// replaces, and a zero time would file the snapshot under 0001-01-01
// while replacing nothing.
var errTrafficNoSnapshotDate = errors.New("traffic read has no snapshot date")

// trafficDate is the UTC calendar day of t as the "YYYY-MM-DD" text the
// SQL casts with ::date. Passing text keeps the day independent of the
// session's TimeZone, which the integration tier sets to
// America/Chicago: a UTC midnight sent as a timestamp would land on the
// previous day there.
func trafficDate(t time.Time) string {
	return t.UTC().Format(time.DateOnly)
}

// UpsertRepoTraffic writes one traffic read (the daily clones go through
// UpsertRepoClone) in one transaction, so the window rows that mark a
// successful read never exist without the rest of it (v0.29.118,
// aveloxis/aveloxis#234). The latest read wins: days, windows and weeks
// upsert on their UNIQUE key, and the snapshot of tr.SnapshotDate is
// replaced as a whole, so an entry that left the top 10 is not kept.
// Zero counts are written like any other. The write stamps
// repos.data_changed_at itself: the staged processor runs outside jobs.
func (s *PostgresStore) UpsertRepoTraffic(ctx context.Context, repoID int64, tr *model.RepoTraffic) error {
	if tr.SnapshotDate.IsZero() {
		return errTrafficNoSnapshotDate
	}
	snapshot := trafficDate(tr.SnapshotDate)
	return s.withRetry(ctx, func(ctx context.Context) error {
		batch := &pgx.Batch{}
		// The repository page's cached traffic answer moves with the
		// rows. The stamp goes first on purpose: it takes the repos row
		// lock before any snapshot row is touched, so a second writer of
		// the same repository waits here until the first commits. Last,
		// both could delete first and both insert, and the stored top 10
		// would be the union of two reads.
		batch.Queue(stampRepoCacheStateSQL+` WHERE repo_id = $1`, repoID)
		for _, v := range tr.Views {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_views
					(repo_id, view_timestamp, total_views, unique_views, data_source)
				VALUES ($1, $2, $3, $4, $5)
				ON CONFLICT (repo_id, view_timestamp) DO UPDATE SET
					total_views = EXCLUDED.total_views,
					unique_views = EXCLUDED.unique_views,
					tool_version = EXCLUDED.tool_version,
					data_collection_date = NOW()`,
				repoID, v.Timestamp, v.TotalViews, v.UniqueViews, v.Origin.DataSource)
		}
		for _, w := range tr.Windows {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_traffic_windows
					(repo_id, traffic_kind, window_start, window_end, uniques, data_source)
				VALUES ($1, $2, $3::date, $4::date, $5, $6)
				ON CONFLICT (repo_id, traffic_kind, window_start, window_end) DO UPDATE SET
					uniques = EXCLUDED.uniques,
					tool_version = EXCLUDED.tool_version,
					data_collection_date = NOW()`,
				repoID, string(w.Kind), trafficDate(w.Start), trafficDate(w.End), w.Uniques, tr.Origin.DataSource)
		}
		for _, w := range tr.Weeks {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_traffic_weeks
					(repo_id, traffic_kind, week_start, uniques, data_source)
				VALUES ($1, $2, $3::date, $4, $5)
				ON CONFLICT (repo_id, traffic_kind, week_start) DO UPDATE SET
					uniques = EXCLUDED.uniques,
					tool_version = EXCLUDED.tool_version,
					data_collection_date = NOW()`,
				repoID, string(w.Kind), trafficDate(w.WeekStart), w.Uniques, tr.Origin.DataSource)
		}

		// The snapshot: the date's earlier rows go first, in the same
		// transaction, so the inserts below can only conflict with an
		// entry this read lists twice (GitHub lists each once).
		batch.Queue(`
			DELETE FROM aveloxis_data.repo_traffic_paths
			WHERE repo_id = $1 AND snapshot_date = $2::date`, repoID, snapshot)
		batch.Queue(`
			DELETE FROM aveloxis_data.repo_traffic_referrers
			WHERE repo_id = $1 AND snapshot_date = $2::date`, repoID, snapshot)
		for _, p := range tr.Paths {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_traffic_paths
					(repo_id, snapshot_date, path, title, total_views, unique_views, data_source)
				VALUES ($1, $2::date, $3, $4, $5, $6, $7)
				ON CONFLICT (repo_id, snapshot_date, path) DO NOTHING`,
				repoID, snapshot, p.Path, p.Title, p.TotalViews, p.UniqueViews, tr.Origin.DataSource)
		}
		for _, r := range tr.Referrers {
			batch.Queue(`
				INSERT INTO aveloxis_data.repo_traffic_referrers
					(repo_id, snapshot_date, referrer, total_views, unique_views, data_source)
				VALUES ($1, $2::date, $3, $4, $5, $6)
				ON CONFLICT (repo_id, snapshot_date, referrer) DO NOTHING`,
				repoID, snapshot, r.Referrer, r.TotalViews, r.UniqueViews, tr.Origin.DataSource)
		}

		tx, err := s.pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if err := tx.SendBatch(ctx, batch).Close(); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

// RepoTrafficHistory is everything stored about one repository's
// traffic, as GET /api/v1/repos/{repoID}/traffic serves it: every
// stored day, window and complete week, and the path and referrer
// snapshot of the latest snapshot date. The lists are never nil, so a
// repository without traffic answers empty arrays; SnapshotDate is nil
// when no path or referrer was ever stored.
type RepoTrafficHistory struct {
	RepoID       int64             `json:"repo_id"`
	Clones       []TrafficDay      `json:"clones"`
	Views        []TrafficDay      `json:"views"`
	Windows      []TrafficWindow   `json:"windows"`
	Weeks        []TrafficWeek     `json:"weeks"`
	SnapshotDate *string           `json:"snapshot_date"`
	Paths        []TrafficPath     `json:"paths"`
	Referrers    []TrafficReferrer `json:"referrers"`
}

// TrafficDay is one stored UTC day of clones or views. The counts are
// pointers because the columns are nullable, like repo_clones'; a NULL
// is never read as 0.
type TrafficDay struct {
	Date    string `json:"date"`
	Count   *int   `json:"count"`
	Uniques *int   `json:"uniques"`
}

// TrafficWindow is one stored 14-day window: its first and last UTC
// days and the distinct visitors or cloners GitHub reported for it.
type TrafficWindow struct {
	Kind    model.TrafficKind `json:"kind"`
	Start   string            `json:"start"`
	End     string            `json:"end"`
	Uniques *int              `json:"uniques"`
}

// TrafficWeek is one stored complete Monday-to-Sunday week.
type TrafficWeek struct {
	Kind      model.TrafficKind `json:"kind"`
	WeekStart string            `json:"week_start"`
	Uniques   *int              `json:"uniques"`
}

// TrafficPath is one entry of a stored top-paths snapshot.
type TrafficPath struct {
	Path    string `json:"path"`
	Title   string `json:"title"`
	Count   *int   `json:"count"`
	Uniques *int   `json:"uniques"`
}

// TrafficReferrer is one entry of a stored top-referrers snapshot.
type TrafficReferrer struct {
	Referrer string `json:"referrer"`
	Count    *int   `json:"count"`
	Uniques  *int   `json:"uniques"`
}

// GetRepoTraffic returns the repository's whole stored traffic history
// in one read (about 730 daily rows a year; not paged). Dates are
// "YYYY-MM-DD" UTC days formatted in SQL, so the session's TimeZone
// cannot move them. Paths and referrers both come from the latest
// snapshot date: a read whose referrer list was empty answers none,
// never an older read's, and a read with both lists empty stores no
// snapshot, so the latest one keeps its earlier date. One read-only
// REPEATABLE READ transaction keeps a window row from outrunning its days.
func (s *PostgresStore) GetRepoTraffic(ctx context.Context, repoID int64) (*RepoTrafficHistory, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("begin traffic read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() // read-only: nothing to undo

	h := &RepoTrafficHistory{RepoID: repoID}
	if h.Clones, err = readTrafficRows[TrafficDay](ctx, tx, "clones", `
		SELECT to_char(clone_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD'), total_clones, unique_clones
		FROM aveloxis_data.repo_clones
		WHERE repo_id = $1
		ORDER BY clone_timestamp`, repoID); err != nil {
		return nil, err
	}
	if h.Views, err = readTrafficRows[TrafficDay](ctx, tx, "views", `
		SELECT to_char(view_timestamp AT TIME ZONE 'UTC', 'YYYY-MM-DD'), total_views, unique_views
		FROM aveloxis_data.repo_views
		WHERE repo_id = $1
		ORDER BY view_timestamp`, repoID); err != nil {
		return nil, err
	}
	if h.Windows, err = readTrafficRows[TrafficWindow](ctx, tx, "windows", `
		SELECT traffic_kind, to_char(window_start, 'YYYY-MM-DD'), to_char(window_end, 'YYYY-MM-DD'), uniques
		FROM aveloxis_data.repo_traffic_windows
		WHERE repo_id = $1
		ORDER BY window_start, window_end, traffic_kind`, repoID); err != nil {
		return nil, err
	}
	if h.Weeks, err = readTrafficRows[TrafficWeek](ctx, tx, "weeks", `
		SELECT traffic_kind, to_char(week_start, 'YYYY-MM-DD'), uniques
		FROM aveloxis_data.repo_traffic_weeks
		WHERE repo_id = $1
		ORDER BY week_start, traffic_kind`, repoID); err != nil {
		return nil, err
	}
	if err := tx.QueryRow(ctx, `
		SELECT to_char(MAX(snapshot_date), 'YYYY-MM-DD') FROM (
			SELECT snapshot_date FROM aveloxis_data.repo_traffic_paths WHERE repo_id = $1
			UNION ALL
			SELECT snapshot_date FROM aveloxis_data.repo_traffic_referrers WHERE repo_id = $1
		) d`, repoID).Scan(&h.SnapshotDate); err != nil {
		return nil, fmt.Errorf("read traffic snapshot date: %w", err)
	}
	h.Paths, h.Referrers = []TrafficPath{}, []TrafficReferrer{}
	if h.SnapshotDate != nil {
		if h.Paths, err = readTrafficRows[TrafficPath](ctx, tx, "paths", `
			SELECT path, COALESCE(title, ''), total_views, unique_views
			FROM aveloxis_data.repo_traffic_paths
			WHERE repo_id = $1 AND snapshot_date = $2::date
			ORDER BY total_views DESC NULLS LAST, unique_views DESC NULLS LAST, path`, repoID, *h.SnapshotDate); err != nil {
			return nil, err
		}
		if h.Referrers, err = readTrafficRows[TrafficReferrer](ctx, tx, "referrers", `
			SELECT referrer, total_views, unique_views
			FROM aveloxis_data.repo_traffic_referrers
			WHERE repo_id = $1 AND snapshot_date = $2::date
			ORDER BY total_views DESC NULLS LAST, unique_views DESC NULLS LAST, referrer`, repoID, *h.SnapshotDate); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("end traffic read: %w", err)
	}
	return h, nil
}

// readTrafficRows runs one query of GetRepoTraffic and maps its columns,
// in order, onto T's fields. The result is never nil (pgx.CollectRows
// starts from an empty slice), so an empty list encodes as [].
func readTrafficRows[T any](ctx context.Context, tx pgx.Tx, what, query string, args ...any) ([]T, error) {
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("read traffic %s: %w", what, err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[T])
	if err != nil {
		return nil, fmt.Errorf("read traffic %s: %w", what, err)
	}
	return out, nil
}
