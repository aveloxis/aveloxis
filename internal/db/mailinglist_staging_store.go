// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aveloxis/aveloxis/internal/model"
)

// mailinglist_staging_store.go is the store layer for aveloxis_ops.mailing_list_staging
// (summary/12 §11). The MailingListWorker STAGES classified messages here; a
// per-list single-threaded batch Processor DRAINS them. Keeping the hot-table
// writes behind this staging→batch boundary is what stops the mailing-list
// pipeline from reproducing Augur's contention on contributors/issues/PRs.

// StagedMailingListRow is one unprocessed staging row handed to the Processor.
type StagedMailingListRow struct {
	MlsID       int64
	RepoGroupID *int64
	RepoID      *int64
	Message     model.MailingListStagedMessage
}

// StageMailingListMessage appends one classified message to staging. Idempotent
// on (rgls_id, message_id_header) — re-staging an already-staged message (e.g.
// a re-collected month) is a no-op, so the fetcher can be replayed safely.
func (s *PostgresStore) StageMailingListMessage(ctx context.Context, rglsID int64, repoGroupID, repoID *int64, msg model.MailingListStagedMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal staged message: %w", err)
	}
	_, err = s.pool.Exec(ctx, `
		INSERT INTO aveloxis_ops.mailing_list_staging
			(rgls_id, repo_group_id, repo_id, message_id_header, envelope)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (rgls_id, message_id_header) DO NOTHING`,
		rglsID, repoGroupID, repoID, msg.MessageID, payload)
	return err
}

// ListsWithStaging returns the rgls_ids that have unprocessed staging rows, so
// the Processor can drain one list at a time (oldest staged first).
func (s *PostgresStore) ListsWithStaging(ctx context.Context, limit int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT rgls_id FROM aveloxis_ops.mailing_list_staging
		WHERE NOT processed
		GROUP BY rgls_id
		ORDER BY min(created_at)
		LIMIT $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("lists with staging: %w", err)
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

// GetMailingListStagingBatch returns up to limit unprocessed rows for one list,
// oldest first, decoding each envelope. The Processor drains a list in these
// batches.
func (s *PostgresStore) GetMailingListStagingBatch(ctx context.Context, rglsID int64, limit int) ([]StagedMailingListRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT mls_id, repo_group_id, repo_id, envelope
		FROM aveloxis_ops.mailing_list_staging
		WHERE rgls_id = $1 AND NOT processed
		ORDER BY mls_id
		LIMIT $2`, rglsID, limit)
	if err != nil {
		return nil, fmt.Errorf("get mailing-list staging batch: %w", err)
	}
	defer rows.Close()
	var out []StagedMailingListRow
	for rows.Next() {
		var r StagedMailingListRow
		var payload []byte
		if err := rows.Scan(&r.MlsID, &r.RepoGroupID, &r.RepoID, &payload); err != nil {
			return nil, err
		}
		if err := json.Unmarshal(payload, &r.Message); err != nil {
			return nil, fmt.Errorf("unmarshal staged envelope (mls_id=%d): %w", r.MlsID, err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// MarkMailingListStagingProcessed flags a drained batch as processed.
func (s *PostgresStore) MarkMailingListStagingProcessed(ctx context.Context, mlsIDs []int64) error {
	if len(mlsIDs) == 0 {
		return nil
	}
	_, err := s.pool.Exec(ctx, `
		UPDATE aveloxis_ops.mailing_list_staging SET processed = TRUE
		WHERE mls_id = ANY($1)`, mlsIDs)
	return err
}

// StuckMailingList is a list that has staged-but-undrained messages whose
// repo_group has no repo yet. The MailingListProcessor leaves such lists
// staged (messages.repo_id is NOT NULL, so it can't write a body without a
// repo) until load-foundation-orgs / DOAP-enrichment populates the group
// (summary/12 §11). Surfacing them lets an operator see which PMCs still need
// org-population instead of discovering it by grepping logs.
type StuckMailingList struct {
	RglsID      int64
	ListAddress string
	RepoGroupID int64
	StagedRows  int64
}

// StuckMailingLists returns every list with unprocessed staging rows whose
// repo_group has no repo, ordered by staged-row count (the biggest backlog
// first). An empty result means every list with staged data can resolve a
// repo (or has already drained).
func (s *PostgresStore) StuckMailingLists(ctx context.Context) ([]StuckMailingList, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT s.rgls_id, COALESCE(rgls.rgls_email, ''), COALESCE(rgls.repo_group_id, 0), count(*)
		FROM aveloxis_ops.mailing_list_staging s
		JOIN aveloxis_data.repo_groups_list_serve rgls ON rgls.rgls_id = s.rgls_id
		WHERE NOT s.processed
		  AND NOT EXISTS (
		      SELECT 1 FROM aveloxis_data.repos r WHERE r.repo_group_id = rgls.repo_group_id
		  )
		GROUP BY s.rgls_id, rgls.rgls_email, rgls.repo_group_id
		ORDER BY count(*) DESC`)
	if err != nil {
		return nil, fmt.Errorf("stuck mailing lists: %w", err)
	}
	defer rows.Close()
	var out []StuckMailingList
	for rows.Next() {
		var m StuckMailingList
		if err := rows.Scan(&m.RglsID, &m.ListAddress, &m.RepoGroupID, &m.StagedRows); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// PurgeMailingListStagingProcessed drops processed staging rows older than the
// retention window (transient data; mirrors PurgeStagedProcessed).
func (s *PostgresStore) PurgeMailingListStagingProcessed(ctx context.Context, retentionSeconds float64) (int64, error) {
	tag, err := s.pool.Exec(ctx, `
		DELETE FROM aveloxis_ops.mailing_list_staging
		WHERE processed AND created_at < NOW() - make_interval(secs => $1)`, retentionSeconds)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}
