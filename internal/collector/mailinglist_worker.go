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

// mlStore is the slice of the store the MailingListWorker needs (interface
// so the routing logic is unit-testable with a fake).
type mlStore interface {
	ClaimNextList(ctx context.Context, system string, cadence time.Duration, pid int, bootID string) (*db.ListJob, error)
	CheckpointListMonth(ctx context.Context, rglsID int64, yyyymm string) error
	CompleteListScan(ctx context.Context, rglsID int64, complete bool) error
	RecordListFailure(ctx context.Context, rglsID int64) error
	GetPrimaryRepoForGroup(ctx context.Context, repoGroupID int64) (int64, bool, error)
	UpsertEmailMessage(ctx context.Context, em *model.EmailMessage) (int64, error)
	UpsertMailingListMessageBody(ctx context.Context, repoID int64, messageID, listAddress, senderEmail, body string, sentAt time.Time, cntrbID *string) (int64, error)
	InsertEmailMessageRef(ctx context.Context, emailMessageID, msgID int64, repoGroupID *int64) error
	ResolveContributorIDByEmail(ctx context.Context, email string) (string, bool, error)
	ResolveMirrorLink(ctx context.Context, owner, repo, kind string, number int) (issueID, prID *int64, err error)
	FindRepoByURL(ctx context.Context, gitURL string) (int64, error)
}

// MailingListWorker scans one mailing-list system's registered lists,
// classifies each message (§3), and routes it: every email → an
// email_message entity; non-mirror bodies → messages + email_message_ref;
// mirror classes (github_mirror/commit_notify) → metadata-only provenance
// (§5, no body re-copy — we already collect that data). The Pacer/Breaker
// (§8) wrap each fetch.
type MailingListWorker struct {
	store          mlStore
	sys            *mailinglist.System
	backend        mailinglist.ArchiveSource
	pacer          *mailinglist.Pacer
	breaker        *mailinglist.Breaker
	cadence        time.Duration
	backfill       int    // months of history to scan when a list has no checkpoint
	mirrorHandling string // skip | metadata_only | full (§5b)
	pid            int
	bootID         string
	logger         *slog.Logger
	now            func() time.Time
}

// NewMailingListWorker builds a worker for one system + backend.
func NewMailingListWorker(store mlStore, sys *mailinglist.System, backend mailinglist.ArchiveSource,
	pacer *mailinglist.Pacer, breaker *mailinglist.Breaker, cadence time.Duration, backfillMonths int, mirrorHandling string, pid int, bootID string, logger *slog.Logger) *MailingListWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if backfillMonths <= 0 {
		backfillMonths = 6
	}
	if mirrorHandling != "skip" && mirrorHandling != "full" {
		mirrorHandling = "metadata_only"
	}
	return &MailingListWorker{
		store: store, sys: sys, backend: backend, pacer: pacer, breaker: breaker,
		cadence: cadence, backfill: backfillMonths, mirrorHandling: mirrorHandling,
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
	repoID, ok, err := w.store.GetPrimaryRepoForGroup(ctx, job.RepoGroupID)
	if err != nil {
		return err
	}
	if !ok {
		// The per-PMC group has no repos yet — load-foundation-orgs /
		// DOAP-enrichment must populate it first. messages.repo_id is NOT
		// NULL, so we cannot write bodies without a repo.
		_ = w.store.RecordListFailure(ctx, job.RglsID)
		return fmt.Errorf("list %q: no repo in repo_group %d — run load-foundation-orgs/DOAP-enrich first",
			job.ListAddress, job.RepoGroupID)
	}

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

		staged := 0
		for i := range msgs {
			if err := w.routeMessage(ctx, job, repoID, msgs[i]); err != nil {
				w.logger.Warn("mailing-list: route message failed", "list", job.ListAddress,
					"message_id", msgs[i].MessageID, "error", err)
				continue
			}
			staged++
		}
		if err := w.store.CheckpointListMonth(ctx, job.RglsID, month); err != nil {
			return err
		}
		w.logger.Info("mailing-list: month complete", "list", job.ListAddress, "month", month,
			"messages", len(msgs), "staged", staged)
	}
	return w.store.CompleteListScan(ctx, job.RglsID, true)
}

// routeMessage classifies one message and writes the rows: an email_message
// entity always; for non-mirror classes the body to messages +
// email_message_ref. Mirror classes record provenance + link only (§5).
func (w *MailingListWorker) routeMessage(ctx context.Context, job *db.ListJob, repoID int64, am mailinglist.ArchiveMessage) error {
	cls := w.sys.Classify(mailinglist.Message{
		ListID: am.ListID, ListAddress: am.ListAddress, Subject: am.Subject, Sender: am.Sender, Body: am.Body,
	})
	isMirror := cls.Class == mailinglist.ClassGitHubMirror || cls.Class == mailinglist.ClassCommitNotify
	signaledURL := w.sys.RepoURLFromCaptures(cls)

	// §5b mirror handling: "skip" drops mirrors entirely (no provenance row).
	if isMirror && w.mirrorHandling == "skip" {
		return nil
	}

	var cntrbPtr *string
	if id, ok, _ := w.store.ResolveContributorIDByEmail(ctx, am.SenderEmail); ok {
		cntrbPtr = &id
	}

	// §5b: a github_mirror with a resolvable body URL links to the existing
	// issue/PR we already collected (instead of duplicating it).
	var linkedIssueID, linkedPRID *int64
	if cls.Class == mailinglist.ClassGitHubMirror {
		if n, err := strconv.Atoi(cls.Captures["number"]); err == nil && cls.Captures["repo"] != "" {
			owner := cls.Captures["owner"]
			if owner == "" {
				owner = "apache"
			}
			linkedIssueID, linkedPRID, _ = w.store.ResolveMirrorLink(ctx, owner, cls.Captures["repo"], cls.Captures["kind"], n)
		}
	}

	// §5c mail-side resolution: if the signaled repo is already in the
	// catalog, resolve signaled_repo_id now (the repo-side org-scan backfill
	// handles mail that predates the repo).
	var signaledRepoID *int64
	if signaledURL != "" {
		if rid, err := w.store.FindRepoByURL(ctx, signaledURL); err == nil && rid > 0 {
			signaledRepoID = &rid
		}
	}

	rgID := job.RepoGroupID
	rgls := job.RglsID
	mirrorsURL := ""
	if isMirror {
		mirrorsURL = signaledURL
	}
	em := &model.EmailMessage{
		RepoID:               &repoID,
		RepoGroupID:          &rgID,
		RglsID:               &rgls,
		PlatformID:           model.Platform(db.MailingListPlatformID),
		MLSystem:             w.sys.Name,
		MessageIDHeader:      am.MessageID,
		ListAddress:          am.ListAddress,
		ListIDHeader:         am.ListID,
		Subject:              am.Subject,
		SenderEmail:          am.SenderEmail,
		SentAt:               am.SentAt,
		InReplyTo:            am.InReplyTo,
		ReferencesChain:      am.References,
		ThreadRootID:         threadRoot(am),
		HasPatch:             am.HasPatch,
		MsgClass:             cls.Class,
		ClassificationSource: cls.Source,
		IsMirror:             isMirror,
		MirrorsURL:           mirrorsURL,
		SignaledRepoURL:      signaledURL,
		SignaledRepoID:       signaledRepoID,
		LinkedIssueID:        linkedIssueID,
		LinkedPullRequestID:  linkedPRID,
		LinkedExternalKey:    cls.Captures["external_key"],
		DataSource:           am.ListAddress,
	}
	emID, err := w.store.UpsertEmailMessage(ctx, em)
	if err != nil {
		return err
	}

	// Mirror classes: by default (metadata_only) record provenance + link
	// only, no body re-copy (§5 — we already collect that data via GitHub).
	// "full" keeps the body too (belt-and-suspenders completeness).
	if isMirror && w.mirrorHandling != "full" {
		return nil
	}

	msgID, err := w.store.UpsertMailingListMessageBody(ctx, repoID, am.MessageID, am.ListAddress, am.SenderEmail, am.Body, am.SentAt, cntrbPtr)
	if err != nil {
		return err
	}
	return w.store.InsertEmailMessageRef(ctx, emID, msgID, &rgID)
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
