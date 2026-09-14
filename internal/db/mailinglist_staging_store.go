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

// ListsWithStaging returns the rgls_ids that have unprocessed staging rows
// AND are registered under the given system, so the Processor can drain one
// list at a time (oldest staged first).
//
// The system filter is LOAD-BEARING (Part G layer-3 find, 2026-09-01): the
// wiring spawns one drain pool PER system, each built around that system's
// processor — and lore_public_inbox's processor carries projectionClean=false.
// Unfiltered, every pool drained every list, and 90-92% of apache mail was
// drained by the lore processor: no issue projection, no tracker actions, no
// thread inheritance, wrong ml_system stamp. A drain pool may only ever see
// lists registered under ITS system.
// ListsWithStaging pages staged lists by rgls_id KEYSET — the same
// round-13 starvation fix as JiraProjectsWithStaging: no-repo lists
// ("no repo for group, leaving staged") keep their old min(created_at)
// and would permanently occupy an oldest-first head once a window
// fills with them. afterID 0 starts from the top.
func (s *PostgresStore) ListsWithStaging(ctx context.Context, system string, afterID int64, limit int) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT st.rgls_id FROM aveloxis_ops.mailing_list_staging st
		JOIN aveloxis_data.repo_groups_list_serve r ON r.rgls_id = st.rgls_id
		WHERE NOT st.processed AND r.mlls_system = $1 AND st.rgls_id > $3
		GROUP BY st.rgls_id
		ORDER BY st.rgls_id
		LIMIT $2`, system, limit, afterID)
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
// GetMailingListStagingBatch pages a list's staged rows by mls_id
// keyset (the round-13 rotation one level down; fresh-context round
// 2026-09-02 #4): deferred/dropped-candidate rows stay unprocessed,
// and re-serving the same window head-blocked the list's tail.
// afterID 0 starts from the top; deferred rows retry next drain.
func (s *PostgresStore) GetMailingListStagingBatch(ctx context.Context, rglsID, afterID int64, limit int) ([]StagedMailingListRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT mls_id, repo_group_id, repo_id, envelope
		FROM aveloxis_ops.mailing_list_staging
		WHERE rgls_id = $1 AND NOT processed AND mls_id > $3
		ORDER BY mls_id
		LIMIT $2`, rglsID, limit, afterID)
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
