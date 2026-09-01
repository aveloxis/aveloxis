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
// state + the inline comment block.
var jiraSearchFields = []string{
	"summary", "reporter", "assignee", "status", "resolution",
	"resolutiondate", "created", "updated", "comment",
}

// jiraTimeLayout parses Jira Server timestamps
// ("2024-01-05T10:00:00.000+0000").
const jiraTimeLayout = "2006-01-02T15:04:05.000-0700"

// jiraStore is the worker's narrow store surface.
type jiraStore interface {
	ClaimNextJiraProject(ctx context.Context, cadence time.Duration, keyFilter string) (*db.JiraProjectJob, error)
	StageJiraIssue(ctx context.Context, jpsID int64, projectKey, issueKey string, issueUpdated time.Time, repoID *int64, envelope []byte) error
	CheckpointJiraProject(ctx context.Context, jpsID int64, lastUpdated time.Time) error
	CompleteJiraScan(ctx context.Context, jpsID int64) error
	RecordJiraFailure(ctx context.Context, jpsID int64) error
	DisableJiraProject(ctx context.Context, jpsID int64) error
}

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

	// jiraMaxStalePages: consecutive pages contributing zero NEW
	// (key, updated) pairs before the scan fails. A boundary-minute
	// re-list legally repeats up to one full page; two would need a
	// pathological cohort; three consecutive means the server is
	// re-serving content regardless of the cursor/offset.
	jiraMaxStalePages = 3
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

// syncProject pages one project's incremental window with the C2
// drift-safe walk (window advance by jql cursor; see the comment block
// inside). Each page checkpoints AFTER its issues staged (SR-3), so an
// interruption resumes from proven work; re-listed boundary minutes are
// no-ops under the staging natural key.
func (w *JiraWorker) syncProject(ctx context.Context, job *db.JiraProjectJob) {
	client := jira.New(job.BaseURL, w.politeEmail)
	// Copilot round on PR #193, C2: NEVER walk bare offsets over
	// ORDER BY updated ASC — the window's membership MUTATES during the
	// scan (an issue touched mid-walk moves later, shifting every offset
	// left), so offset paging can skip an unseen issue while the
	// per-page checkpoint advances past it: a PERMANENT skip (the next
	// cycle's `updated >=` never re-lists it). The drift-safe walk:
	//   - advance the WINDOW (a jql `updated >=` cursor at Jira's minute
	//     precision) with startAt=0 after every page whose max updated
	//     moved the cursor forward — the boundary minute re-lists, which
	//     the staging natural key makes a no-op;
	//   - hold a fixed CEILING (`updated <=` the scan-claim minute) so a
	//     busy project cannot chase its own tail — later updates belong
	//     to the next cycle via the checkpoint;
	//   - fall back to in-window offsets ONLY for a same-minute cohort
	//     wider than a page (cursor cannot advance). Drift inside that
	//     one minute carries a documented residual — see the fallback
	//     branch below.
	const jqlMinute = "2006-01-02 15:04"
	ceiling := time.Now().UTC().Truncate(time.Minute)
	var cursor time.Time
	if job.LastUpdated != nil {
		cursor = job.LastUpdated.UTC().Truncate(time.Minute)
	}
	buildJQL := func() string {
		if cursor.IsZero() {
			return fmt.Sprintf("project = %s AND updated <= '%s' ORDER BY updated ASC",
				job.ProjectKey, ceiling.Format(jqlMinute))
		}
		return fmt.Sprintf("project = %s AND updated >= '%s' AND updated <= '%s' ORDER BY updated ASC",
			job.ProjectKey, cursor.Format(jqlMinute), ceiling.Format(jqlMinute))
	}
	startAt := 0 // offset WITHIN the current window (tie-minute fallback only)
	staged := 0
	// Defensive termination bound (L10 review, F2 — the first version
	// keyed on an unchanged (cursor, startAt) pair, which every
	// non-empty page strictly advances: an unfireable gate, while the
	// rewrite had dropped the old `startAt < total` bound entirely, so a
	// server re-serving one page regardless of startAt held the claim
	// forever — 8,779 requests in the reviewer's probe). The signal the
	// loop cannot self-satisfy is NEWNESS: a page whose every
	// (key, updated) pair was already seen this scan contributes
	// nothing; boundary-minute re-lists legally repeat one page, so
	// only CONSECUTIVE all-stale pages fail the scan.
	seenThisScan := map[string]struct{}{}
	stalePages := 0
	for {
		jql := buildJQL()
		if ctx.Err() != nil {
			// Shutdown mid-sync: pages already staged are checkpointed;
			// no failure recorded (v0.27.28 — cancellation is terminal,
			// not a strike).
			return
		}
		page, err := client.SearchPage(ctx, jql, jiraSearchFields, startAt, w.pageSize)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return // shutdown, not a failure (v0.27.28)
			}
			if platform.ClassifyError(err) == platform.ClassSkip {
				// Dead project key — disable, never retry (the 5 dead
				// James sub-keys of the pilot's 191).
				if !errors.Is(err, context.Canceled) {
					w.logger.Warn("jira: dead project key — disabling", "project", job.ProjectKey, "error", err)
				}
				if derr := w.store.DisableJiraProject(ctx, job.JpsID); derr != nil && !errors.Is(derr, context.Canceled) {
					w.logger.Warn("jira: disable failed", "project", job.ProjectKey, "error", derr)
				}
				return
			}
			if !errors.Is(err, context.Canceled) {
				w.logger.Warn("jira: sync page failed", "project", job.ProjectKey, "start_at", startAt, "error", err)
			}
			if ferr := w.store.RecordJiraFailure(ctx, job.JpsID); ferr != nil && !errors.Is(ferr, context.Canceled) {
				w.logger.Warn("jira: record failure failed", "project", job.ProjectKey, "error", ferr)
			}
			return
		}
		if len(page.Issues) == 0 {
			break // window exhausted — the walk is done
		}
		var pageMax time.Time
		pageNew := 0
		for _, is := range page.Issues {
			// Review 2026-08-30 #7 (SR-3): a skipped issue must FAIL the
			// scan, never `continue` — later issues in the ASC page would
			// push pageMax past it and the checkpoint would stamp over
			// work never staged. Jira's timestamp format is fixed, so a
			// parse failure is a defect signal; backoff-retry is the
			// honest arm.
			updated, perr := time.Parse(jiraTimeLayout, is.Fields.Updated)
			if perr != nil {
				w.logger.Warn("jira: unparseable updated timestamp — failing scan", "issue", is.Key, "raw", is.Fields.Updated)
				if ferr := w.store.RecordJiraFailure(ctx, job.JpsID); ferr != nil && !errors.Is(ferr, context.Canceled) {
					w.logger.Warn("jira: record failure failed", "project", job.ProjectKey, "error", ferr)
				}
				return
			}
			envelope, merr := json.Marshal(is)
			if merr != nil {
				w.logger.Warn("jira: envelope marshal failed — failing scan", "issue", is.Key, "error", merr)
				if ferr := w.store.RecordJiraFailure(ctx, job.JpsID); ferr != nil && !errors.Is(ferr, context.Canceled) {
					w.logger.Warn("jira: record failure failed", "project", job.ProjectKey, "error", ferr)
				}
				return
			}
			if serr := w.store.StageJiraIssue(ctx, job.JpsID, job.ProjectKey, is.Key, updated, job.RepoID, envelope); serr != nil {
				if errors.Is(serr, context.Canceled) {
					return
				}
				w.logger.Warn("jira: stage failed", "issue", is.Key, "error", serr)
				if ferr := w.store.RecordJiraFailure(ctx, job.JpsID); ferr != nil && !errors.Is(ferr, context.Canceled) {
					w.logger.Warn("jira: record failure failed", "project", job.ProjectKey, "error", ferr)
				}
				return
			}
			staged++
			nk := is.Key + "@" + is.Fields.Updated
			if _, dup := seenThisScan[nk]; !dup {
				seenThisScan[nk] = struct{}{}
				pageNew++
			}
			if updated.After(pageMax) {
				pageMax = updated
			}
		}
		if pageNew == 0 {
			stalePages++
			if stalePages >= jiraMaxStalePages {
				w.logger.Warn("jira: consecutive pages contributed nothing new — failing scan (misbehaving server?)",
					"project", job.ProjectKey, "stale_pages", stalePages, "cursor", cursor, "start_at", startAt)
				if ferr := w.store.RecordJiraFailure(ctx, job.JpsID); ferr != nil && !errors.Is(ferr, context.Canceled) {
					w.logger.Warn("jira: record failure failed", "project", job.ProjectKey, "error", ferr)
				}
				return
			}
		} else {
			stalePages = 0
		}
		// SR-3: the checkpoint stamps only over rows proven staged.
		if !pageMax.IsZero() {
			if cerr := w.store.CheckpointJiraProject(ctx, job.JpsID, pageMax); cerr != nil && !errors.Is(cerr, context.Canceled) {
				w.logger.Warn("jira: checkpoint failed", "project", job.ProjectKey, "error", cerr)
			}
		}
		// Advance the window; offsets only within an unmovable minute.
		pageMaxMinute := pageMax.UTC().Truncate(time.Minute)
		if pageMaxMinute.After(cursor) {
			cursor = pageMaxMinute
			// TRUE startAt=0 (L10 review on this fix, F1): the next
			// window RE-LISTS the whole boundary minute and the staging
			// natural key no-ops the duplicates. The first version
			// "optimized" this to skip the boundary issues already
			// staged — an offset ASSUMPTION that mid-walk drift defeats
			// (an issue touched between pages shifts the window left and
			// the skip count lands past an unseen sibling: a permanent
			// skip, empirically reproduced by the reviewer).
			startAt = 0
		} else {
			// Same-minute cohort wider than a page: the cursor cannot
			// advance at minute precision, so offsets within the fixed
			// window are the only walk. RESIDUAL (documented, accepted):
			// drift INSIDE this one minute between its pages can skip a
			// sibling, and once a later minute advances the checkpoint
			// the skip is permanent — the exposure is a touch landing in
			// the page gap of an over-a-page same-minute cohort, orders
			// of magnitude rarer than the page-boundary hole above.
			startAt += len(page.Issues)
		}
		if w.pageSleep > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(w.pageSleep):
			}
		}
	}
	if err := w.store.CompleteJiraScan(ctx, job.JpsID); err != nil && !errors.Is(err, context.Canceled) {
		w.logger.Warn("jira: complete failed", "project", job.ProjectKey, "error", err)
	}
	// "staged" = distinct issues this scan; "stage_calls" includes the
	// deliberate boundary-minute re-lists (natural-key no-ops).
	w.logger.Info("jira: project synced", "project", job.ProjectKey,
		"staged", len(seenThisScan), "stage_calls", staged)
}
