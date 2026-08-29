// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// mailingListDrainBatch bounds one staging→write batch. Per-list draining is
// single-threaded (summary/12 §11), so this only governs transaction size, not
// concurrency.
const mailingListDrainBatch = 500

// mlProcessorStore is the slice of the store the MailingListProcessor needs:
// staging drain + the DB-dependent resolution/write half that used to live on
// the worker (sender→cntrb, mirror-link, signaled-repo, email_message /
// messages / email_message_ref writes). Keeping these writes behind the
// staging→batch boundary — drained one list at a time — is what stops the
// mailing-list pipeline from reproducing Augur's contention on the hot tables.
type mlProcessorStore interface {
	ListsWithStaging(ctx context.Context, limit int) ([]int64, error)
	GetMailingListStagingBatch(ctx context.Context, rglsID int64, limit int) ([]db.StagedMailingListRow, error)
	MarkMailingListStagingProcessed(ctx context.Context, mlsIDs []int64) error

	GetPrimaryRepoForGroup(ctx context.Context, repoGroupID int64) (int64, bool, error)
	ResolveContributorIDByEmail(ctx context.Context, email string) (string, bool, error)
	ResolveMirrorLink(ctx context.Context, owner, repo, kind string, number int) (*int64, *int64, error)
	FindRepoByURL(ctx context.Context, gitURL string) (int64, error)
	UpsertEmailMessage(ctx context.Context, em *model.EmailMessage) (int64, error)
	UpsertMailingListMessageBody(ctx context.Context, repoID int64, messageID, listAddress, senderEmail, body string, sentAt time.Time, cntrbID *string) (int64, error)
	InsertEmailMessageRef(ctx context.Context, emailMessageID, msgID int64, repoGroupID *int64) error

	// Phase A projection (§3): LINK an existing issue by external_key, else
	// CREATE a synthetic one; bridge the email as a comment.
	LinkOrCreateIssueFromEmail(ctx context.Context, repoID int64, externalKey, title, body, dataSource string, reporterID *string, createdAt time.Time) (int64, bool, error)
	BridgeEmailToIssue(ctx context.Context, issueID, repoID, msgID int64) error
	// #1 thread-inheritance: the issue a thread is already projected onto, so a
	// non-keyed reply/discussion in that thread attaches to the same issue.
	FindIssueForThread(ctx context.Context, threadRoot string, repoID int64) (int64, bool, error)
}

// MailingListProcessor drains aveloxis_ops.mailing_list_staging one list at a
// time, resolving sender / mirror-link / signaled-repo and writing the
// email_message + messages + email_message_ref rows. It is the resolve+write
// half of the v0.25.x staging split (summary/12 §11); the MailingListWorker is
// the fetch+classify+stage half.
type MailingListProcessor struct {
	store           mlProcessorStore
	system          string // ML system definition name (apache_ponymail | lore_public_inbox)
	mirrorHandling  string // skip | metadata_only | full (§5b)
	projectionClean bool   // §2: project issue_event→issues etc. (clean_fit systems only)
	logger          *slog.Logger

	// inflight guards single-threaded-per-list draining when more than one
	// drain goroutine shares this Processor: a list being drained by one
	// goroutine is skipped by the others until it's released. Default config
	// runs a single goroutine, so this is a no-op there.
	mu       sync.Mutex
	inflight map[int64]bool

	// noRepoWarned rate-limits the "no repo for group, leaving staged" WARN
	// to once per list per noRepoWarnInterval. A list whose repo_group has
	// no resolvable repo is stable operator-attention state, not a new
	// event — unconditional logging re-warned four wedged lists every few
	// seconds forever (65,592 lines in the Aug 7–16 2026 production log).
	// Guarded by mu.
	noRepoWarned map[int64]time.Time
}

// noRepoWarnInterval bounds how often one list's "no repo for group" WARN
// repeats. Hourly keeps the wedged state visible in the log without
// drowning it.
const noRepoWarnInterval = time.Hour

// shouldWarnNoRepo reports whether the "no repo for group" WARN should fire
// for this list, and stamps the last-warned time when it does.
func (p *MailingListProcessor) shouldWarnNoRepo(rglsID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if last, ok := p.noRepoWarned[rglsID]; ok && time.Since(last) < noRepoWarnInterval {
		return false
	}
	p.noRepoWarned[rglsID] = time.Now()
	return true
}

// NewMailingListProcessor builds a processor for one ML system. projectionClean
// gates Layer-2 entity projection (§2): true for clean_fit systems (Apache),
// false for forge-less systems (kernel) where mail stays Layer-1 only.
func NewMailingListProcessor(store mlProcessorStore, system, mirrorHandling string, projectionClean bool, logger *slog.Logger) *MailingListProcessor {
	if logger == nil {
		logger = slog.Default()
	}
	if mirrorHandling != "skip" && mirrorHandling != "full" {
		mirrorHandling = "metadata_only"
	}
	return &MailingListProcessor{
		store: store, system: system, mirrorHandling: mirrorHandling,
		projectionClean: projectionClean, logger: logger, inflight: map[int64]bool{},
		noRepoWarned: map[int64]time.Time{},
	}
}

// tryClaim marks a list as being drained; returns false if another goroutine
// already holds it (so the caller skips it this pass).
func (p *MailingListProcessor) tryClaim(rglsID int64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.inflight[rglsID] {
		return false
	}
	p.inflight[rglsID] = true
	return true
}

func (p *MailingListProcessor) release(rglsID int64) {
	p.mu.Lock()
	delete(p.inflight, rglsID)
	p.mu.Unlock()
}

// DrainOnce drains every list that currently has staged rows, one list at a
// time (single-threaded per list). Returns the total number of staged rows
// processed across all lists.
func (p *MailingListProcessor) DrainOnce(ctx context.Context, listLimit int) (int, error) {
	lists, err := p.store.ListsWithStaging(ctx, listLimit)
	if err != nil {
		return 0, err
	}
	total := 0
	for _, rgls := range lists {
		if ctx.Err() != nil {
			return total, ctx.Err()
		}
		if !p.tryClaim(rgls) {
			continue // another drain goroutine has this list — single-threaded per list
		}
		n, derr := p.DrainList(ctx, rgls)
		p.release(rgls)
		total += n
		if errors.Is(derr, context.Canceled) {
			return total, derr // shutdown mid-list: the wiring loop exits (pass 36)
		}
		if derr != nil {
			p.logger.Warn("mailing-list processor: list drain failed (will resume)",
				"rgls_id", rgls, "processed", n, "error", derr)
		}
	}
	return total, nil
}

// DrainList drains one list's staged rows in batches. It resolves the list's
// repo once (from the staged rows' repo_group_id) and keeps a per-list
// write-through contributor cache so a sender that recurs across the list's
// messages is resolved once. If the list's repo_group has no repo yet
// (messages.repo_id is NOT NULL), the rows are LEFT staged for a later drain
// once load-foundation-orgs / DOAP-enrichment populates the group.
func (p *MailingListProcessor) DrainList(ctx context.Context, rglsID int64) (int, error) {
	cntrbCache := map[string]*string{}
	threadIssue := map[string]int64{} // thread root → projected issue_id (#1 inheritance)
	var repoID int64
	repoResolved := false
	processed := 0

	for {
		batch, err := p.store.GetMailingListStagingBatch(ctx, rglsID, mailingListDrainBatch)
		if err != nil {
			return processed, err
		}
		if len(batch) == 0 {
			return processed, nil
		}

		if !repoResolved {
			rid, ok, rerr := p.resolveRepo(ctx, batch[0])
			if rerr != nil {
				return processed, rerr
			}
			if !ok {
				// Rate-limited: a group with no resolvable repo is stable
				// state, and this line re-fired every few seconds per
				// wedged list pre-v0.27.91. Rows stay staged untouched.
				if p.shouldWarnNoRepo(rglsID) {
					p.logger.Warn("mailing-list processor: no repo for group, leaving staged",
						"rgls_id", rglsID, "repo_group_id", derefInt64(batch[0].RepoGroupID),
						"next_warn_in", noRepoWarnInterval)
				}
				return processed, nil
			}
			repoID, repoResolved = rid, true
		}

		done := make([]int64, 0, len(batch))
		dropped := 0
		for _, row := range batch {
			if ctx.Err() != nil {
				return processed, ctx.Err() // shutdown mid-batch: nothing marked, the rows drain next start (pass 37)
			}
			if err := p.processRow(ctx, repoID, rglsID, row, cntrbCache, threadIssue); err != nil {
				if errors.Is(err, context.Canceled) {
					return processed, err // not a drop: the row stays staged
				}
				// Mark processed anyway so the drain makes progress (collect what
				// we can; one bad message must not wedge the whole list — the
				// failing-cohort-dominates-pool lesson). The message IS lost,
				// which is why this logs at ERROR, not WARN (v0.25.36): data
				// loss must be loud. If dropped counts trend non-zero in
				// production, the fix is an attempts column on
				// mailing_list_staging + bounded retry, not silent re-marking.
				dropped++
				p.logger.Error("mailing-list processor: write failed, DROPPING message",
					"rgls_id", rglsID, "message_id", row.Message.MessageID, "error", err)
			}
			done = append(done, row.MlsID)
		}
		if dropped > 0 {
			p.logger.Error("mailing-list processor: batch completed with dropped messages",
				"rgls_id", rglsID, "dropped", dropped, "batch", len(batch))
		}
		if err := p.store.MarkMailingListStagingProcessed(ctx, done); err != nil {
			return processed, err
		}
		processed += len(done)
	}
}

// resolveRepo finds the repo to attribute a list's bodies to: the staged
// repo_id if present, else the repo_group's primary repo.
func (p *MailingListProcessor) resolveRepo(ctx context.Context, row db.StagedMailingListRow) (int64, bool, error) {
	if row.RepoID != nil && *row.RepoID > 0 {
		return *row.RepoID, true, nil
	}
	rgid := derefInt64(row.RepoGroupID)
	if rgid <= 0 {
		return 0, false, nil
	}
	return p.store.GetPrimaryRepoForGroup(ctx, rgid)
}

// processRow resolves + writes one staged message. Mirrors the pre-split
// worker.routeMessage, reading the already-computed classification from the
// staging envelope instead of re-classifying.
func (p *MailingListProcessor) processRow(ctx context.Context, repoID, rglsID int64, row db.StagedMailingListRow, cntrbCache map[string]*string, threadIssue map[string]int64) error {
	m := row.Message

	// §5b mirror handling: "skip" drops mirrors entirely (no provenance row).
	if m.IsMirror && p.mirrorHandling == "skip" {
		return nil
	}

	// Per-list write-through contributor cache.
	cntrbPtr, cached := cntrbCache[m.SenderEmail]
	if !cached {
		if id, ok, _ := p.store.ResolveContributorIDByEmail(ctx, m.SenderEmail); ok {
			cp := id
			cntrbPtr = &cp
		}
		cntrbCache[m.SenderEmail] = cntrbPtr
	}

	// §5b: a github_mirror with a resolvable body URL links to the existing
	// issue/PR we already collected (instead of duplicating it).
	var linkedIssueID, linkedPRID *int64
	if m.MsgClass == mailinglist.ClassGitHubMirror && m.MirrorRepo != "" && m.MirrorNumber > 0 {
		owner := m.MirrorOwner
		if owner == "" {
			owner = "apache"
		}
		linkedIssueID, linkedPRID, _ = p.store.ResolveMirrorLink(ctx, owner, m.MirrorRepo, m.MirrorKind, m.MirrorNumber)
	}

	// Phase A projection (§3): an issue_event with a parsed external_key →
	// LINK an existing issue carrying that key, else CREATE a synthetic one.
	// Only for clean_fit systems (Apache); forge-less systems stay Layer-1.
	var projectedIssueID int64
	if p.projectionClean && m.MsgClass == mailinglist.ClassIssueEvent && m.ExternalKey != "" {
		// Reporter = the resolved sender, but NEVER the jira@/bot sender
		// (attribution integrity; real-actor-from-body parsing is a follow-up).
		var reporterID *string
		if cntrbPtr != nil && !IsBotEmail(m.SenderEmail) {
			reporterID = cntrbPtr
		}
		title := mailingListIssueTitle(m.Subject, m.ExternalKey)
		// data_source = 'JIRA' for the Apache issue_event case (the only
		// projected tracker today; Bugzilla support is a follow-up).
		if id, _, perr := p.store.LinkOrCreateIssueFromEmail(ctx, repoID, m.ExternalKey, title, m.Body, "JIRA", reporterID, m.SentAt); perr != nil {
			if errors.Is(perr, context.Canceled) {
				return perr // shutdown: DrainList reports it once (pass 38)
			}
			p.logger.Warn("mailing-list processor: issue projection failed",
				"rgls_id", rglsID, "external_key", m.ExternalKey, "error", perr)
		} else if id > 0 {
			projectedIssueID = id
			linkedIssueID = &id
		}
	}

	// Record the keyed message's issue under its thread so non-keyed siblings
	// in the same drain inherit it without a DB round-trip.
	if projectedIssueID > 0 {
		threadIssue[threadKey(m)] = projectedIssueID
	}

	// #1 thread-inheritance: a non-keyed, non-mirror message in a thread that
	// already has a projected issue attaches to that issue — so the FULL thread
	// (human discussion, Re: replies, class=discuss) lands on the issue, not
	// just the Jira-notification stream. Cache first, then a DB lookup that
	// covers cross-batch / cross-cycle siblings.
	if projectedIssueID == 0 && p.projectionClean && !m.IsMirror && linkedPRID == nil && m.ThreadRoot != "" {
		if id, ok := threadIssue[m.ThreadRoot]; ok && id > 0 {
			projectedIssueID = id
			linkedIssueID = &id
		} else if id, ok, _ := p.store.FindIssueForThread(ctx, m.ThreadRoot, repoID); ok {
			projectedIssueID = id
			linkedIssueID = &id
			threadIssue[m.ThreadRoot] = id
		}
	}

	// §5c mail-side resolution: if the signaled repo is already in the
	// catalog, resolve signaled_repo_id now (the repo-side org-scan backfill
	// handles mail that predates the repo).
	var signaledRepoID *int64
	if m.SignaledRepoURL != "" {
		if rid, err := p.store.FindRepoByURL(ctx, m.SignaledRepoURL); err == nil && rid > 0 {
			signaledRepoID = &rid
		}
	}

	rgls := rglsID
	mirrorsURL := ""
	if m.IsMirror {
		mirrorsURL = m.SignaledRepoURL
	}
	em := &model.EmailMessage{
		RepoID:               &repoID,
		RepoGroupID:          row.RepoGroupID,
		RglsID:               &rgls,
		PlatformID:           model.Platform(db.MailingListPlatformID),
		MLSystem:             p.system,
		MessageIDHeader:      m.MessageID,
		ListAddress:          m.ListAddress,
		ListIDHeader:         m.ListID,
		Subject:              m.Subject,
		SenderEmail:          m.SenderEmail,
		SentAt:               m.SentAt,
		InReplyTo:            m.InReplyTo,
		ReferencesChain:      m.References,
		ThreadRootID:         m.ThreadRoot,
		HasPatch:             m.HasPatch,
		MsgClass:             m.MsgClass,
		ClassificationSource: m.ClassificationSource,
		IsMirror:             m.IsMirror,
		MirrorsURL:           mirrorsURL,
		SignaledRepoURL:      m.SignaledRepoURL,
		SignaledRepoID:       signaledRepoID,
		LinkedIssueID:        linkedIssueID,
		LinkedPullRequestID:  linkedPRID,
		LinkedExternalKey:    m.ExternalKey,
		ProjectedKind:        projectedKind(linkedIssueID, linkedPRID, nil),
		DataSource:           m.ListAddress,
	}
	emID, err := p.store.UpsertEmailMessage(ctx, em)
	if err != nil {
		return err
	}

	// Mirror classes: by default (metadata_only) record provenance + link only,
	// no body re-copy (§5 — we already collect that data via GitHub). "full"
	// keeps the body too (belt-and-suspenders completeness).
	if m.IsMirror && p.mirrorHandling != "full" {
		return nil
	}

	msgID, err := p.store.UpsertMailingListMessageBody(ctx, repoID, m.MessageID, m.ListAddress, m.SenderEmail, m.Body, m.SentAt, cntrbPtr)
	if err != nil {
		return err
	}
	if err := p.store.InsertEmailMessageRef(ctx, emID, msgID, row.RepoGroupID); err != nil {
		return err
	}
	// Bridge the email body as a comment on the projected issue so the thread
	// shows up in per-repo issue analytics.
	if projectedIssueID > 0 {
		return p.store.BridgeEmailToIssue(ctx, projectedIssueID, repoID, msgID)
	}
	return nil
}

// projectedKind maps the resolved link fields to the §10a queryable kind.
func projectedKind(issueID, prID, reviewID *int64) string {
	switch {
	case prID != nil:
		return "pr"
	case reviewID != nil:
		return "review"
	case issueID != nil:
		return "issue"
	default:
		return "mailing_list_only"
	}
}

// mailingListIssueTitle strips the tracker decoration from a subject so the
// projected issue carries a clean title. Handles the Jira shape
// "[jira] [Created] (KAFKA-123) summary" → "summary" (take the text after the
// "(KEY)"), falling back to a leading "[KEY] " strip, else the trimmed subject.
func mailingListIssueTitle(subject, externalKey string) string {
	if externalKey != "" {
		if i := strings.Index(subject, "("+externalKey+")"); i >= 0 {
			return strings.TrimSpace(subject[i+len(externalKey)+2:])
		}
		if i := strings.Index(subject, "["+externalKey+"]"); i >= 0 {
			return strings.TrimSpace(subject[i+len(externalKey)+2:])
		}
	}
	return strings.TrimSpace(subject)
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

// threadKey is the cache key for a message's thread: its thread root if it has
// one, else its own Message-ID (it IS a root, so siblings' thread_root points
// at it). Keeps the in-drain inheritance cache aligned with FindIssueForThread.
func threadKey(m model.MailingListStagedMessage) string {
	if m.ThreadRoot != "" {
		return m.ThreadRoot
	}
	return m.MessageID
}
