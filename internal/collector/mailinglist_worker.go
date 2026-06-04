// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// mlStore is the slice of the store the MailingListWorker needs. As of the
// v0.25.x staging refactor (summary/12 §11) the worker only FETCHES, CLASSIFIES,
// and STAGES — it no longer touches the hot tables (contributors / email_message
// / messages). The resolve+write half moved to MailingListProcessor (which
// drains the staging table per-list, single-threaded). This is what keeps the
// mailing-list pipeline off the per-message direct-upsert path that reproduces
// Augur's lock contention.
type mlStore interface {
	ClaimNextList(ctx context.Context, system string, cadence time.Duration, pid int, bootID string) (*db.ListJob, error)
	CheckpointListMonth(ctx context.Context, rglsID int64, yyyymm string) error
	CompleteListScan(ctx context.Context, rglsID int64, complete bool) error
	RecordListFailure(ctx context.Context, rglsID int64) error
	StageMailingListMessage(ctx context.Context, rglsID int64, repoGroupID, repoID *int64, msg model.MailingListStagedMessage) error
}

// MailingListWorker scans one mailing-list system's registered lists,
// classifies each message (§3), and routes it: every email → an
// email_message entity; non-mirror bodies → messages + email_message_ref;
// mirror classes (github_mirror/commit_notify) → metadata-only provenance
// (§5, no body re-copy — we already collect that data). The Pacer/Breaker
// (§8) wrap each fetch.
type MailingListWorker struct {
	store    mlStore
	sys      *mailinglist.System
	backend  mailinglist.ArchiveSource
	pacer    *mailinglist.Pacer
	breaker  *mailinglist.Breaker
	cadence  time.Duration
	backfill int // months of history to scan when a list has no checkpoint
	pid      int
	bootID   string
	logger   *slog.Logger
	now      func() time.Time
}

// NewMailingListWorker builds a worker for one system + backend. Mirror
// handling moved to the MailingListProcessor with the v0.25.x staging split:
// the worker only fetches, classifies, and stages.
func NewMailingListWorker(store mlStore, sys *mailinglist.System, backend mailinglist.ArchiveSource,
	pacer *mailinglist.Pacer, breaker *mailinglist.Breaker, cadence time.Duration, backfillMonths int, pid int, bootID string, logger *slog.Logger) *MailingListWorker {
	if logger == nil {
		logger = slog.Default()
	}
	// Do NOT clamp backfillMonths to a positive default here. A value of 0 (or
	// negative) is the explicit "full history from the list's first month"
	// signal that monthsToScan's default branch depends on. The bounded default
	// of 6 is applied at the config layer (MailingListBackfillMonthsOrDefault,
	// nil → 6); coercing it again here made full-history mode unreachable even
	// when backfill_months=0 was set — the startup log showed 0 while the worker
	// silently used 6 (v0.25.13).
	return &MailingListWorker{
		store: store, sys: sys, backend: backend, pacer: pacer, breaker: breaker,
		cadence: cadence, backfill: backfillMonths,
		pid: pid, bootID: bootID, logger: logger, now: time.Now,
	}
}

// RunOnce claims and scans one eligible list. Returns true if it claimed
// work. When the source breaker is open it pauses (claims nothing) — the
// §8 dispatcher-pause: don't stamp partial state during an outage.
func (w *MailingListWorker) RunOnce(ctx context.Context) (bool, error) {
	if !w.breaker.Healthy() {
		w.logger.Warn("mailing-list: source breaker open, pausing dispatch", "system", w.sys.Name)
		return false, nil
	}
	job, err := w.store.ClaimNextList(ctx, w.sys.Name, w.cadence, w.pid, w.bootID)
	if err != nil {
		return false, fmt.Errorf("claim list: %w", err)
	}
	if job == nil {
		return false, nil
	}
	if err := w.ProcessList(ctx, job); err != nil {
		w.logger.Warn("mailing-list: list scan failed (will resume from checkpoint)",
			"system", w.sys.Name, "list", job.ListAddress, "error", err)
	}
	return true, nil
}

// ProcessList scans every month from the list's checkpoint forward to now.
// On a fetch failure it records the failure (releasing the lock; the
// checkpoint is preserved so the next claim resumes) and returns. On a clean
// pass it marks the scan complete.
func (w *MailingListWorker) ProcessList(ctx context.Context, job *db.ListJob) error {
	// No repo resolution here anymore — the worker stages classified messages
	// and the MailingListProcessor resolves repo / sender / mirror at drain
	// time. (summary/12 §11.)
	for _, month := range w.monthsToScan(ctx, job) {
		if d := w.pacer.Delay(); d > 0 {
			if !sleepCtx(ctx, d) {
				return ctx.Err()
			}
		}
		msgs, retryAfter, ferr := w.backend.FetchMonth(ctx, job.ListAddress, month)
		if ferr != nil {
			w.pacer.OnStrain()
			switch {
			case errors.Is(ferr, mailinglist.ErrRateLimited):
				w.logger.Warn("mailing-list: rate_limit_detected", "list", job.ListAddress, "retry_after", retryAfter)
				if retryAfter > 0 {
					_ = sleepCtx(ctx, retryAfter)
				}
			case errors.Is(ferr, mailinglist.ErrTransient):
				if tripped := w.breaker.RecordTransientFailure(); tripped {
					w.logger.Warn("mailing-list: circuit_open — pausing source", "system", w.sys.Name)
				}
			}
			_ = w.store.RecordListFailure(ctx, job.RglsID)
			return fmt.Errorf("fetch %s %s: %w", job.ListAddress, month, ferr)
		}
		w.pacer.OnSuccess()
		w.breaker.RecordSuccess()

		rgID := job.RepoGroupID
		staged := 0
		for i := range msgs {
			env := w.buildStagedMessage(msgs[i])
			if err := w.store.StageMailingListMessage(ctx, job.RglsID, &rgID, nil, env); err != nil {
				w.logger.Warn("mailing-list: stage message failed", "list", job.ListAddress,
					"message_id", msgs[i].MessageID, "error", err)
				continue
			}
			staged++
		}
		if err := w.store.CheckpointListMonth(ctx, job.RglsID, month); err != nil {
			return err
		}
		w.logger.Info("mailing-list: month staged", "list", job.ListAddress, "month", month,
			"messages", len(msgs), "staged", staged)
	}
	return w.store.CompleteListScan(ctx, job.RglsID, true)
}

// buildStagedMessage classifies a fetched message (cheap, no DB) into the
// staging envelope the MailingListProcessor drains. Every DB-dependent
// resolution (sender→cntrb, signaled_repo_id, mirror-link) and every hot-table
// write happens at drain time, not here.
func (w *MailingListWorker) buildStagedMessage(am mailinglist.ArchiveMessage) model.MailingListStagedMessage {
	cls := w.sys.Classify(mailinglist.Message{
		ListID: am.ListID, ListAddress: am.ListAddress, Subject: am.Subject, Sender: am.Sender, Body: am.Body,
	})
	env := model.MailingListStagedMessage{
		MessageID:            am.MessageID,
		ListAddress:          am.ListAddress,
		ListID:               am.ListID,
		Subject:              am.Subject,
		SenderEmail:          am.SenderEmail,
		SentAt:               am.SentAt,
		InReplyTo:            am.InReplyTo,
		References:           am.References,
		ThreadRoot:           threadRoot(am),
		Body:                 am.Body,
		HasPatch:             am.HasPatch,
		MsgClass:             cls.Class,
		ClassificationSource: cls.Source,
		IsMirror:             cls.Class == mailinglist.ClassGitHubMirror || cls.Class == mailinglist.ClassCommitNotify,
		SignaledRepoURL:      w.sys.RepoURLFromCaptures(cls),
		ExternalKey:          cls.Captures["external_key"],
	}
	if cls.Class == mailinglist.ClassGitHubMirror {
		if n, err := strconv.Atoi(cls.Captures["number"]); err == nil && cls.Captures["repo"] != "" {
			owner := cls.Captures["owner"]
			if owner == "" {
				owner = "apache"
			}
			env.MirrorOwner = owner
			env.MirrorRepo = cls.Captures["repo"]
			env.MirrorKind = cls.Captures["kind"]
			env.MirrorNumber = n
		}
	}
	return env
}

// monthsToScan returns the yyyy-mm windows to fetch, through the current
// month inclusive. The start is, in order of precedence:
//   - the month after the checkpoint (resume), if checkpointed;
//   - `backfill` months back, when backfill > 0 (bounded window, the default);
//   - the list's actual FIRST month (full history), when backfill <= 0 — the
//     #5 full-history mode. Falls back to a 30-year floor if FirstMonth is
//     unavailable.
func (w *MailingListWorker) monthsToScan(ctx context.Context, job *db.ListJob) []string {
	now := w.now().UTC()
	cur := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var start time.Time
	switch {
	case job.LastMonth != "":
		if t, err := time.Parse("2006-01", job.LastMonth); err == nil {
			start = t.AddDate(0, 1, 0) // month after the checkpoint
		} else {
			start = cur.AddDate(0, -w.backfill+1, 0)
		}
	case w.backfill > 0:
		start = cur.AddDate(0, -w.backfill+1, 0) // bounded backfill window
	default:
		// Full-history mode: start at the list's first month.
		start = cur.AddDate(0, -360, 0) // 30-year floor fallback
		if fm, err := w.backend.FirstMonth(ctx, job.ListAddress); err == nil && fm != "" {
			if t, perr := time.Parse("2006-01", fm); perr == nil {
				start = t
			}
		}
	}
	if start.After(cur) {
		return nil // already current
	}

	var out []string
	for m := start; !m.After(cur); m = m.AddDate(0, 1, 0) {
		out = append(out, m.Format("2006-01"))
	}
	return out
}

// threadRoot picks the thread root Message-ID: the first entry of References
// (the original), else In-Reply-To, else empty (the message is itself a root).
func threadRoot(am mailinglist.ArchiveMessage) string {
	if refs := strings.Fields(am.References); len(refs) > 0 {
		return strings.Trim(refs[0], "<> ")
	}
	return am.InReplyTo
}

// sleepCtx sleeps for d, returning false if ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
