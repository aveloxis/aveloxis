// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/model"
)

// fakeProcStore records the resolve+write decisions the MailingListProcessor
// makes when draining staged messages.
type fakeProcStore struct {
	rows []db.StagedMailingListRow // staged input, returned in one batch then empty

	emails        []*model.EmailMessage
	bodies        int
	refs          int
	processed     []int64
	resolvable    map[string]string // sender email → cntrb_id
	resolveCalls  map[string]int    // sender email → ResolveContributorIDByEmail call count
	primaryRepoID int64
	primaryRepoOK bool
	mirrorIssueID *int64
	mirrorPRID    *int64

	// v0.28.20 node-ID mirror-link path.
	chunk        int // >0 → hand out rows in batches of this size
	nodeIssueID  *int64
	nodePRID     *int64
	nodeErr      error
	nodeErrUntil int // fail only for the first N resolve calls (0 = use nodeErr for all)
	nodeIDsAsked []string
	nextBodyID   int64

	// projection (Phase A)
	issueByKey     map[string]int64 // external_key → issue_id (link-or-create)
	createdIssues  []string         // external_keys for which CREATE/LINK was called
	bridgedIssues  []int64          // issue_ids bridged
	nextIssueID    int64
	threadIssues   map[string]int64 // thread root → issue_id (FindIssueForThread fake)
	cleanBodies    []string
	cleanRules     []string
	appliedActions []string
	listedSystems  []string // systems DrainOnce asked ListsWithStaging for
}

func (f *fakeProcStore) ListsWithStaging(_ context.Context, system string, _ int) ([]int64, error) {
	f.listedSystems = append(f.listedSystems, system)
	return []int64{7}, nil
}

// GetMailingListStagingBatch honors `chunk` when set, so a test can drive a
// MULTI-batch drain. With chunk == 0 it returns everything at once (the
// original single-batch behavior every pre-existing test relies on).
//
// The chunking matters: per-BATCH state (drainCounters) is indistinguishable
// from per-DRAIN state when the fake can only ever produce one batch — a test
// asserting "logged once per batch" would pass either way.
func (f *fakeProcStore) GetMailingListStagingBatch(_ context.Context, _ int64, limit int) ([]db.StagedMailingListRow, error) {
	n := len(f.rows)
	if n == 0 {
		return nil, nil
	}
	if f.chunk > 0 && f.chunk < n {
		n = f.chunk
	}
	if limit > 0 && limit < n {
		n = limit
	}
	out := f.rows[:n]
	f.rows = f.rows[n:]
	return out, nil
}
func (f *fakeProcStore) MarkMailingListStagingProcessed(_ context.Context, ids []int64) error {
	f.processed = append(f.processed, ids...)
	return nil
}
func (f *fakeProcStore) GetPrimaryRepoForGroup(context.Context, int64) (int64, bool, error) {
	return f.primaryRepoID, f.primaryRepoOK, nil
}
func (f *fakeProcStore) ResolveContributorIDByEmail(_ context.Context, email string) (string, bool, error) {
	if f.resolveCalls == nil {
		f.resolveCalls = map[string]int{}
	}
	f.resolveCalls[email]++
	if id, ok := f.resolvable[email]; ok {
		return id, true, nil
	}
	return "", false, nil
}
func (f *fakeProcStore) ResolveMirrorLink(context.Context, string, string, string, int) (*int64, *int64, error) {
	return f.mirrorIssueID, f.mirrorPRID, nil
}

// ResolveMirrorLinkByNodeID records the node IDs the processor asked about so
// tests can pin that the node-ID path is tried, and returns the configured
// node-keyed result (v0.28.20).
func (f *fakeProcStore) ResolveMirrorLinkByNodeID(_ context.Context, nodeID string) (*int64, *int64, error) {
	f.nodeIDsAsked = append(f.nodeIDsAsked, nodeID)
	if f.nodeErrUntil > 0 {
		if len(f.nodeIDsAsked) <= f.nodeErrUntil {
			return nil, nil, errors.New("connection reset")
		}
		return f.nodeIssueID, f.nodePRID, nil
	}
	if f.nodeErr != nil {
		return nil, nil, f.nodeErr
	}
	return f.nodeIssueID, f.nodePRID, nil
}

func (f *fakeProcStore) FindRepoByURL(context.Context, string) (int64, error) { return 0, nil }
func (f *fakeProcStore) UpsertEmailMessage(_ context.Context, em *model.EmailMessage) (int64, error) {
	f.emails = append(f.emails, em)
	return int64(len(f.emails)), nil
}
func (f *fakeProcStore) UpsertMailingListMessageBody(_ context.Context, _ int64, _, _, _, _ string, _ time.Time, _ *string, cleanBody, cleanRule string) (int64, error) {
	f.bodies++
	f.nextBodyID++
	f.cleanBodies = append(f.cleanBodies, cleanBody)
	f.cleanRules = append(f.cleanRules, cleanRule)
	return f.nextBodyID, nil
}
func (f *fakeProcStore) ApplyTrackerAction(_ context.Context, issueID int64, action string, _ time.Time) error {
	f.appliedActions = append(f.appliedActions, fmt.Sprintf("%d:%s", issueID, action))
	return nil
}
func (f *fakeProcStore) InsertEmailMessageRef(context.Context, int64, int64, *int64) error {
	f.refs++
	return nil
}
func (f *fakeProcStore) LinkOrCreateIssueFromEmail(_ context.Context, _ int64, externalKey, _, _, _ string, _ *string, _ time.Time) (int64, bool, error) {
	if f.issueByKey == nil {
		f.issueByKey = map[string]int64{}
	}
	if id, ok := f.issueByKey[externalKey]; ok {
		f.createdIssues = append(f.createdIssues, externalKey)
		return id, false, nil // LINK
	}
	f.nextIssueID++
	f.issueByKey[externalKey] = f.nextIssueID
	f.createdIssues = append(f.createdIssues, externalKey)
	return f.nextIssueID, true, nil // CREATE
}
func (f *fakeProcStore) BridgeEmailToIssue(_ context.Context, issueID, _, _ int64) error {
	f.bridgedIssues = append(f.bridgedIssues, issueID)
	return nil
}
func (f *fakeProcStore) FindIssueForThread(_ context.Context, threadRoot string, _ int64) (int64, bool, error) {
	if id, ok := f.threadIssues[threadRoot]; ok {
		return id, true, nil
	}
	return 0, false, nil
}

func rg(v int64) *int64 { return &v }

// TestProcessorRoutesMirrorVsBody pins the resolve+write half: mirror →
// metadata-only (no body), discuss/jira → body + ref, mirror links to the
// existing PR, jira carries the external key, and a repeated sender is
// resolved once (write-through cache).
func TestProcessorRoutesMirrorVsBody(t *testing.T) {
	mirrorPR := int64(555)
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		resolvable: map[string]string{"alice@example.org": "cntrb-alice"},
		mirrorPRID: &mirrorPR,
		rows: []db.StagedMailingListRow{
			{MlsID: 1, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-mirror@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "x@example.org",
				MsgClass: mailinglist.ClassGitHubMirror, IsMirror: true,
				SignaledRepoURL: "https://github.com/apache/arrow-rs",
				MirrorOwner:     "apache", MirrorRepo: "arrow-rs", MirrorKind: "pull", MirrorNumber: 1, Body: "URL: ...",
			}},
			{MlsID: 2, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-discuss@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "alice@example.org",
				MsgClass: mailinglist.ClassDiscuss, Body: "let's discuss",
			}},
			{MlsID: 3, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-jira@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "jira@apache.org",
				MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "ARROW-99", Body: "ticket body",
			}},
			// second discuss from alice → must reuse the cached resolution
			{MlsID: 4, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-discuss2@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "alice@example.org",
				MsgClass: mailinglist.ClassDiscuss, Body: "more",
			}},
		},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))

	n, err := p.DrainList(context.Background(), 7)
	if err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if n != 4 {
		t.Errorf("processed = %d, want 4", n)
	}
	if len(store.emails) != 4 {
		t.Fatalf("expected 4 email_message rows, got %d", len(store.emails))
	}
	// discuss + jira + discuss2 get bodies; the github_mirror is metadata-only.
	if store.bodies != 3 {
		t.Errorf("expected 3 bodies (mirror is metadata-only), got %d", store.bodies)
	}
	if store.refs != 3 {
		t.Errorf("expected 3 email_message_ref rows, got %d", store.refs)
	}
	if len(store.processed) != 4 {
		t.Errorf("expected 4 staging rows marked processed, got %d", len(store.processed))
	}
	// Sender alice resolved once, reused for the second message.
	if store.resolveCalls["alice@example.org"] != 1 {
		t.Errorf("alice resolved %d times; write-through cache must resolve once", store.resolveCalls["alice@example.org"])
	}

	byID := map[string]*model.EmailMessage{}
	for _, em := range store.emails {
		byID[em.MessageIDHeader] = em
	}
	if m := byID["m-mirror@x"]; m == nil || !m.IsMirror || m.LinkedPullRequestID == nil || *m.LinkedPullRequestID != 555 {
		t.Errorf("mirror must link to existing PR 555: %+v", m)
	}
	if m := byID["m-jira@x"]; m == nil || m.LinkedExternalKey != "ARROW-99" {
		t.Errorf("jira external key not carried: %+v", m)
	}
	if m := byID["m-discuss@x"]; m == nil || m.DataSource != "dev@arrow.apache.org" || m.MLSystem != "apache_ponymail" {
		t.Errorf("discuss provenance wrong: %+v", m)
	}

	// Phase A projection: the jira issue_event projected onto an issue.
	if m := byID["m-jira@x"]; m == nil || m.LinkedIssueID == nil || m.ProjectedKind != "issue" {
		t.Errorf("jira message must project to an issue (linked_issue_id set, projected_kind=issue): %+v", m)
	}
	if len(store.createdIssues) != 1 || store.createdIssues[0] != "ARROW-99" {
		t.Errorf("expected one issue link-or-create for ARROW-99, got %v", store.createdIssues)
	}
	if len(store.bridgedIssues) != 1 {
		t.Errorf("expected the jira email bridged to its issue once, got %v", store.bridgedIssues)
	}
	// projected_kind reflects the routing for every class.
	if m := byID["m-mirror@x"]; m == nil || m.ProjectedKind != "pr" {
		t.Errorf("mirror→PR must have projected_kind=pr: %+v", m)
	}
	if m := byID["m-discuss@x"]; m == nil || m.ProjectedKind != "mailing_list_only" {
		t.Errorf("discuss must have projected_kind=mailing_list_only: %+v", m)
	}
}

// TestProcessorSkipsProjectionWhenNotCleanFit pins the §2 gate: a forge-less
// system (projectionClean=false) never projects issue_event onto an issue —
// the message stays Layer-1 (projected_kind=mailing_list_only, no issue create).
func TestProcessorSkipsProjectionWhenNotCleanFit(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{
			{MlsID: 1, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m-jira@x", ListAddress: "dev@x.apache.org", SenderEmail: "jira@apache.org",
				MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "FOO-1", Body: "b",
			}},
		},
	}
	p := NewMailingListProcessor(store, "lore_public_inbox", "metadata_only", false /*projectionClean*/, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if len(store.createdIssues) != 0 {
		t.Errorf("non-clean_fit system must NOT project issues; got %v", store.createdIssues)
	}
	if len(store.emails) != 1 || store.emails[0].ProjectedKind != "mailing_list_only" {
		t.Errorf("non-clean_fit issue_event must stay mailing_list_only: %+v", store.emails)
	}
}

// TestProcessorThreadInheritance pins #1: a non-keyed discussion reply that
// shares a thread with a keyed Jira message inherits that message's issue —
// linked_issue_id set, bridged as a comment, projected_kind=issue — so the
// full thread (not just the Jira-notification stream) lands on the issue. A
// message in no/unknown thread stays mailing_list_only.
func TestProcessorThreadInheritance(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoID: 42, primaryRepoOK: true,
		rows: []db.StagedMailingListRow{
			// keyed Jira root (ThreadRoot empty → its own MessageID is the root)
			{MlsID: 1, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "root@x", ListAddress: "dev@arrow.apache.org", SenderEmail: "jira@apache.org",
				MsgClass: mailinglist.ClassIssueEvent, ExternalKey: "ARROW-1", Body: "created",
			}},
			// human discussion reply in the same thread, NO external key
			{MlsID: 2, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "reply@x", ThreadRoot: "root@x", ListAddress: "dev@arrow.apache.org",
				SenderEmail: "alice@example.org", MsgClass: mailinglist.ClassDiscuss, Body: "I have thoughts",
			}},
			// unrelated discussion, no thread → stays mailing_list_only
			{MlsID: 3, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "lone@x", ListAddress: "dev@arrow.apache.org",
				SenderEmail: "bob@example.org", MsgClass: mailinglist.ClassDiscuss, Body: "unrelated",
			}},
		},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainList(context.Background(), 7); err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	byID := map[string]*model.EmailMessage{}
	for _, em := range store.emails {
		byID[em.MessageIDHeader] = em
	}
	// The keyed root projected an issue.
	root := byID["root@x"]
	if root == nil || root.LinkedIssueID == nil {
		t.Fatalf("keyed root must project an issue: %+v", root)
	}
	// The reply inherited the SAME issue via the in-drain thread cache.
	reply := byID["reply@x"]
	if reply == nil || reply.LinkedIssueID == nil || *reply.LinkedIssueID != *root.LinkedIssueID || reply.ProjectedKind != "issue" {
		t.Errorf("discussion reply must inherit the thread's issue (linked_issue_id=%v, projected_kind=issue): %+v", root.LinkedIssueID, reply)
	}
	// Both root and reply bridged onto the issue (2 bridges for 1 issue).
	if len(store.bridgedIssues) != 2 {
		t.Errorf("expected root + reply bridged (2), got %v", store.bridgedIssues)
	}
	// The lone discussion stayed mailing_list_only.
	if lone := byID["lone@x"]; lone == nil || lone.LinkedIssueID != nil || lone.ProjectedKind != "mailing_list_only" {
		t.Errorf("threadless discussion must stay mailing_list_only: %+v", lone)
	}
}

// TestProcessorLeavesStagedWhenNoRepo pins the deferral path: a list whose
// repo_group has no repo yet leaves its rows staged (messages.repo_id is NOT
// NULL) for a later drain once the org-scan populates the group.
func TestProcessorLeavesStagedWhenNoRepo(t *testing.T) {
	store := &fakeProcStore{
		primaryRepoOK: false, // no repo
		rows: []db.StagedMailingListRow{
			{MlsID: 1, RepoGroupID: rg(3), Message: model.MailingListStagedMessage{
				MessageID: "m1", ListAddress: "dev@x.apache.org", MsgClass: mailinglist.ClassDiscuss, Body: "x",
			}},
		},
	}
	p := NewMailingListProcessor(store, "apache_ponymail", "metadata_only", true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := p.DrainList(context.Background(), 7)
	if err != nil {
		t.Fatalf("DrainList: %v", err)
	}
	if n != 0 {
		t.Errorf("processed = %d, want 0 (no repo → leave staged)", n)
	}
	if len(store.processed) != 0 {
		t.Errorf("no rows should be marked processed when the repo is unresolved; got %d", len(store.processed))
	}
	if len(store.emails) != 0 {
		t.Errorf("no email_message rows should be written without a repo; got %d", len(store.emails))
	}
}

// TestDrainOncePassesProcessorSystem pins the cross-system drain fix at the
// collector layer: DrainOnce must scope its list claim to the processor's OWN
// system. A hardcoded system name here would re-open the Part G find (the
// lore pool draining apache lists with projectionClean=false).
func TestDrainOncePassesProcessorSystem(t *testing.T) {
	// A NONSENSE system name, deliberately: constructing with a real name
	// would let a hardcoded "lore_public_inbox" in DrainOnce pass (review
	// find #3) — no production system is ever spelled like this probe.
	f := &fakeProcStore{}
	proc := NewMailingListProcessor(f, "xsys_probe_system", "metadata_only", false, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := proc.DrainOnce(context.Background(), 10); err != nil {
		t.Fatalf("DrainOnce: %v", err)
	}
	if len(f.listedSystems) == 0 || f.listedSystems[0] != "xsys_probe_system" {
		t.Fatalf("DrainOnce must claim lists for ITS system; asked for %v", f.listedSystems)
	}
}
