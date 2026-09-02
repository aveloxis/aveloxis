// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_worker.go — the Jira collector's fetch half (C3). The
// mailing-list architecture applied to a tracker: claim a registered
// project (aveloxis_ops.jira_project_serve), page its incremental JQL
// window against /rest/api/2/search (identity fields + inline
// comments — the pilot measured zero comment truncation across 718
// comments and ~1.1s per 100-issue page), stage raw envelopes into
// aveloxis_ops.jira_staging, checkpoint per STAGED page (SR-3), and
// complete. The resolve/write half is the JiraProcessor.
//
// Politeness is self-imposed (issues.apache.org emits no rate-limit
// headers): sequential pages, a fixed sleep between them, and the
// claim-side quadratic failure backoff. A 400 (dead project key — 5 of
// the 191 pilot keys) DISABLES the registration; retrying cannot
// revive a key that does not exist.

package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/jira"
)

// jiraSearchFields is the production field selection: identity +
// state + the inline comment block. Every identity-bearing field here
// has a consumer that BANKS it (reporter AND assignee both route
// through the processor's resolveIdentity → jira_identities) — a field
// in this list with no banking consumer is a defect (Copilot round 2
// on PR #193, suppressed #1).
var jiraSearchFields = []string{
	"summary", "reporter", "assignee", "status", "resolution",
	"resolutiondate", "created", "updated", "comment",
}

// jiraTimeLayout aliases the jira package's ONE timestamp-layout
// spelling (SR-17) for this package's parse sites.
const jiraTimeLayout = jira.TimeLayout

// jiraStore is the worker's narrow store surface.
type jiraStore interface {
	ClaimNextJiraProject(ctx context.Context, cadence time.Duration, keyFilter string) (*db.JiraProjectJob, error)
	StageJiraIssue(ctx context.Context, jpsID int64, projectKey, issueKey string, issueUpdated time.Time, repoID *int64, envelope []byte) error
	CheckpointJiraProject(ctx context.Context, jpsID int64, lastUpdated time.Time) error
	CompleteJiraScan(ctx context.Context, jpsID int64, lockedAt time.Time) error
	RecordJiraFailure(ctx context.Context, jpsID int64, lockedAt time.Time) error
	DisableJiraProject(ctx context.Context, jpsID int64) error
	ReleaseJiraClaim(ctx context.Context, jpsID int64, lockedAt time.Time) error
}

// jiraClaimReleaseTimeout bounds the background-context claim release
// on the shutdown path (inside the scheduler's goTracked drain — the
// scancode/CompleteJob best-effort-write budget).
const jiraClaimReleaseTimeout = 5 * time.Second

// JiraWorker syncs one claimed project at a time.
type JiraWorker struct {
	store       jiraStore
	cadence     time.Duration
	politeEmail string
	pageSize    int
	pageSleep   time.Duration
	logger      *slog.Logger
}

// NewJiraWorker builds a worker. pageSize/pageSleep are parameters so
// tests drive small fast pages; production wiring passes
// jiraDefaultPageSize / jiraDefaultPageSleep.
func NewJiraWorker(store jiraStore, cadence time.Duration, politeEmail string, pageSize int, pageSleep time.Duration, logger *slog.Logger) *JiraWorker {
	if pageSize <= 0 {
		pageSize = jiraDefaultPageSize
		if pageSleep == 0 {
			// Production default only when the page size defaults too —
			// tests pass explicit sizes with zero sleep.
			pageSleep = jiraDefaultPageSleep
		}
	}
	return &JiraWorker{store: store, cadence: cadence, politeEmail: politeEmail,
		pageSize: pageSize, pageSleep: pageSleep, logger: logger}
}

const (
	// jiraDefaultPageSize: 100 issues WITH inline comments measured
	// ~600KB / 1.1s on issues.apache.org — bounded response size beats
	// the server's 1000 cap.
	jiraDefaultPageSize = 100
	// jiraDefaultPageSleep is the between-pages politeness gap (~2
	// requests/second ceiling combined with page latency).
	jiraDefaultPageSleep = 500 * time.Millisecond
	// jiraIdleSleep is the claim-miss idle.
	jiraIdleSleep = 30 * time.Second
)

// Run loops claim→sync until ctx cancels.
func (w *JiraWorker) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		if !w.RunOnce(ctx) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(jiraIdleSleep):
			}
		}
	}
}

// RunOnce claims and syncs one project. Returns false when nothing was
// claimed (caller idles).
func (w *JiraWorker) RunOnce(ctx context.Context) bool {
	job, err := w.store.ClaimNextJiraProject(ctx, w.cadence, "")
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			w.logger.Warn("jira: claim failed", "error", err)
		}
		return false
	}
	if job == nil {
		return false
	}
	w.syncProject(ctx, job)
	return true
}

// releaseClaimBestEffort clears a shutdown-cut scan's claim on a
// bounded BACKGROUND context (Copilot round 4 on PR #193: every
// Canceled exit left jps_locked_at held, so a graceful restart
// stranded the project for the claim query's 2-hour stale window).
// No failure recorded, no completion stamped — the per-page
// checkpoint is the resume state; the release only makes the row
// immediately reclaimable. Runs inside the scheduler's goTracked
// shutdown drain, so the pool is still open.
func (w *JiraWorker) releaseClaimBestEffort(job *db.JiraProjectJob) {
	rctx, cancel := context.WithTimeout(context.Background(), jiraClaimReleaseTimeout)
	defer cancel()
	if err := w.store.ReleaseJiraClaim(rctx, job.JpsID, job.LockedAt); err != nil && !errors.Is(err, context.Canceled) {
		w.logger.Warn("jira: shutdown claim release failed — the project waits out the stale window",
			"project", job.ProjectKey, "error", err)
	}
}

// syncProject pages one project's incremental window through the ONE
// shared drift-safe walk (jira.WalkProjectByUpdated — SR-17; the walk
// mechanics, ceiling, tie-minute fallback and termination bound live
// there). Each page checkpoints AFTER its issues staged (SR-3), so an
// interruption resumes from proven work; re-listed boundary minutes
// are no-ops under the staging natural key.
func (w *JiraWorker) syncProject(ctx context.Context, job *db.JiraProjectJob) {
	client := jira.New(job.BaseURL, w.politeEmail)
	var since time.Time
	if job.LastUpdated != nil {
		since = *job.LastUpdated
	}
	stageCalls := 0
	distinct := map[string]struct{}{}
	err := client.WalkProjectByUpdated(ctx, job.ProjectKey, jiraSearchFields, w.pageSize, w.pageSleep, since,
		func(issues []jira.Issue, updated []time.Time) error {
			var pageMax time.Time
			for i, is := range issues {
				// Copilot round 2 on PR #193 (#2): Jira caps the inline
				// comment block independently of the search page size.
				// An envelope staged with a truncated block loses the
				// tail FOREVER (the envelope is immutable and the
				// incremental JQL never re-serves an unchanged issue),
				// so completeness is enforced HERE — the worker is the
				// only layer with a client. The re-fetch walks the
				// comment endpoint from startAt=0 and REPLACES the
				// block (never an offset splice of the tail; the
				// comment upsert dedups by id, so overlap is a no-op).
				if is.Fields.Comment != nil && is.Fields.Comment.Total > len(is.Fields.Comment.Comments) {
					all, cerr := fetchAllJiraComments(ctx, client, is.Key, w.pageSleep)
					if cerr != nil {
						// SR-3 shape: an issue that cannot stage
						// COMPLETE must fail the scan — skipping it
						// would let later issues push the checkpoint
						// past it.
						return fmt.Errorf("comment tail fetch for %s (inline %d of %d): %w",
							is.Key, len(is.Fields.Comment.Comments), is.Fields.Comment.Total, cerr)
					}
					is.Fields.Comment.Comments = all
					is.Fields.Comment.Total = len(all)
				}
				envelope, merr := json.Marshal(is)
				if merr != nil {
					return fmt.Errorf("envelope marshal for %s: %w", is.Key, merr)
				}
				if serr := w.store.StageJiraIssue(ctx, job.JpsID, job.ProjectKey, is.Key, updated[i], job.RepoID, envelope); serr != nil {
					return fmt.Errorf("stage %s: %w", is.Key, serr)
				}
				stageCalls++
				distinct[is.Key+"@"+is.Fields.Updated] = struct{}{}
				if updated[i].After(pageMax) {
					pageMax = updated[i]
				}
			}
			// SR-3: the checkpoint stamps only over rows proven staged —
			// and a checkpoint FAILURE fails the scan (Copilot round 10
			// on PR #193): warn-and-continue let CompleteJiraScan stamp
			// success over a stale jps_last_updated, so a transient
			// write failure on a late page silently converted the next
			// cadence into a re-walk of everything past the last stamp
			// (worst case the whole history on a first sync). Failing
			// here records backoff instead; the checkpointed prefix is
			// the resume state. A Canceled write flows to the worker's
			// shutdown arm (%w keeps errors.Is intact).
			if !pageMax.IsZero() {
				if cerr := w.store.CheckpointJiraProject(ctx, job.JpsID, pageMax); cerr != nil {
					return fmt.Errorf("checkpoint at %s: %w", pageMax.Format(time.RFC3339), cerr)
				}
			}
			return nil
		})
	if err != nil {
		if errors.Is(err, context.Canceled) {
			// Shutdown mid-sync: pages already staged are checkpointed;
			// no failure recorded (v0.27.28 — cancellation is terminal,
			// not a strike). The claim is released on a bounded
			// background context so the restart can re-claim
			// immediately instead of waiting out the 2h stale window.
			w.releaseClaimBestEffort(job)
			return
		}
		if platform.ClassifyError(err) == platform.ClassSkip {
			// Dead project key — disable, never retry (the 5 dead
			// James sub-keys of the pilot's 191).
			w.logger.Warn("jira: dead project key — disabling", "project", job.ProjectKey, "error", err)
			if derr := w.store.DisableJiraProject(ctx, job.JpsID); derr != nil {
				if errors.Is(derr, context.Canceled) {
					w.releaseClaimBestEffort(job)
					return
				}
				w.logger.Warn("jira: disable failed", "project", job.ProjectKey, "error", derr)
			}
			return
		}
		w.logger.Warn("jira: sync failed", "project", job.ProjectKey, "error", err)
		if ferr := w.store.RecordJiraFailure(ctx, job.JpsID, job.LockedAt); ferr != nil {
			if errors.Is(ferr, context.Canceled) {
				// The failure record (which also clears the lock) was
				// itself cut down by shutdown — release so the claim
				// doesn't strand; the failure re-derives on the next run.
				w.releaseClaimBestEffort(job)
				return
			}
			if errors.Is(ferr, db.ErrJiraClaimLost) {
				// Round 6: the scan outlived the stale window and was
				// re-claimed — the outcome belongs to the new holder;
				// record nothing, release nothing.
				w.logger.Info("jira: claim ownership lost mid-scan — outcomes belong to the new holder",
					"project", job.ProjectKey)
				return
			}
			w.logger.Warn("jira: record failure failed", "project", job.ProjectKey, "error", ferr)
		}
		return
	}
	if err := w.store.CompleteJiraScan(ctx, job.JpsID, job.LockedAt); err != nil {
		if errors.Is(err, db.ErrJiraClaimLost) {
			// Round 6: a >2h scan whose claim was stolen must not stamp
			// the run, clear the new holder's lock, or log success —
			// the staged work stands (idempotent) and the new holder
			// owns the outcome.
			w.logger.Info("jira: claim ownership lost before completion — outcomes belong to the new holder",
				"project", job.ProjectKey, "staged", len(distinct))
			return
		}
		if errors.Is(err, context.Canceled) {
			// A canceled completion write leaves the lock held AND must
			// not log "synced" — release and stop; the checkpointed work
			// stands and the next claim re-runs a no-op window.
			w.releaseClaimBestEffort(job)
			return
		}
		// Copilot round 5 on PR #193: the NON-cancel arm used to warn
		// and fall through to the synced log with the lock still held —
		// a transient completion-write failure suppressed collection
		// for the 2h stale window while reporting false success. The
		// failure record clears the lock AND paces the retry
		// (quadratic backoff); if it too fails, the best-effort release
		// at least frees the claim. The checkpointed work stands either
		// way — the retried scan re-runs a cheap window.
		w.logger.Warn("jira: complete failed — recording failure, nothing stamped as synced",
			"project", job.ProjectKey, "error", err)
		if ferr := w.store.RecordJiraFailure(ctx, job.JpsID, job.LockedAt); ferr != nil {
			if !errors.Is(ferr, db.ErrJiraClaimLost) {
				// Claim lost = the new holder owns the lock; anything
				// else = free the claim best-effort.
				w.releaseClaimBestEffort(job)
			}
		}
		return
	}
	// "staged" = distinct issues this scan; "stage_calls" includes the
	// deliberate boundary-minute re-lists (natural-key no-ops).
	w.logger.Info("jira: project synced", "project", job.ProjectKey,
		"staged", len(distinct), "stage_calls", stageCalls)
}
func fetchAllJiraComments(ctx context.Context, client *jira.Client, issueKey string, sleep time.Duration) ([]jira.Comment, error) {
	const commentPageSize = 100
	var all []jira.Comment
	for start := 0; ; {
		page, err := client.IssueCommentsPage(ctx, issueKey, start, commentPageSize)
		if err != nil {
			return nil, err
		}
		all = append(all, page.Comments...)
		if len(page.Comments) == 0 {
			return all, nil
		}
		eff := page.MaxResults
		if eff <= 0 || eff > commentPageSize {
			eff = commentPageSize
		}
		if len(page.Comments) < eff {
			return all, nil
		}
		if page.Total > 0 && len(all) >= page.Total {
			return all, nil
		}
		start += len(page.Comments)
		if sleep > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(sleep):
			}
		}
	}
}
