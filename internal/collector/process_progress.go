// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import "log/slog"

// process_progress.go — INFO-level progress visibility for the processing
// phase (v0.27.53).
//
// Motivating incident (2026-07-29): pytorch/pytorch entered "processing
// staged data" on 2026-07-25T11:18:45Z with 1.32M staged messages and then
// emitted ZERO log lines for 4+ days while grinding healthily — the operator
// had to fall back to live DB queries (staging processed-counts + the
// locked_at heartbeat) to distinguish "working" from "dead". The
// collection-side phases got progress lines in v0.22.4 ("review comments
// progress" every 1000); processing was the remaining silent multi-day
// phase. This tracker closes that gap without touching the processing logic
// itself: ProcessRepo counts rows AFTER each successful processBatch and the
// tracker decides when a line is due.

// processProgressEvery is the row interval between "processing progress"
// lines. Derivation, not a guess: batches are processBatchSize (500) rows,
// so this is every 10th batch; at the pytorch incident's observed worst-case
// rate (~500 rows/1–2 min) that is one line every ~10–20 minutes; and
// typical incremental cycles stage well under 5,000 rows per entity type, so
// the fleet's steady-state cycles emit no new lines at all (pinned by
// TestProcessProgressBelowIntervalStaysSilent).
const processProgressEvery = 5000

// processProgress emits periodic progress lines for one (repo, entity type)
// processing phase. Purely observational: it never returns errors and never
// touches the store, so wiring it into ProcessRepo cannot alter processing
// behavior.
type processProgress struct {
	logger     *slog.Logger
	repoID     int64
	entityType string
	every      int
	processed  int
	lastLogged int
}

func newProcessProgress(logger *slog.Logger, repoID int64, entityType string, every int) *processProgress {
	return &processProgress{logger: logger, repoID: repoID, entityType: entityType, every: every}
}

// add records n successfully processed rows and logs a progress line
// whenever at least `every` rows have accumulated since the last line.
func (p *processProgress) add(n int) {
	p.processed += n
	if p.processed-p.lastLogged >= p.every {
		p.lastLogged = p.processed
		p.logger.Info("processing progress",
			"repo_id", p.repoID, "type", p.entityType, "rows", p.processed)
	}
}

// finish logs a completion summary for phases big enough to have been worth
// progress lines (>= every). Small phases — the overwhelming majority of
// incremental cycles — stay completely silent so this feature adds no fleet
// log volume.
func (p *processProgress) finish() {
	if p.processed >= p.every {
		p.logger.Info("entity processed",
			"repo_id", p.repoID, "type", p.entityType, "rows", p.processed)
	}
}
