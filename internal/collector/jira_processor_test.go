// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// jira_processor_test.go — C3 drain half: staged envelopes become
// issues (provider-precedence writer) + native comment messages, with
// identity resolution per person: unambiguous match links, pure
// Jira-only identities MINT a contributor (networks include them),
// ambiguous stays raw. Rows whose project has no repo mapping stay
// staged (the mailing-list stuck-list pattern).
package collector

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

type fakeJiraProcStore struct {
	batches    [][]db.JiraStagingRow
	batchCall  int
	identities map[string][3]any // name -> {cntrb, method, ambiguous}
	minted     []string
	issues     []db.JiraAPIIssue
	comments   []db.JiraAPIComment
	processed  []int64
}

func (f *fakeJiraProcStore) JiraProjectsWithStaging(context.Context, int) ([]int64, error) {
	return []int64{7}, nil
}
func (f *fakeJiraProcStore) GetJiraStagingBatch(context.Context, int64, int) ([]db.JiraStagingRow, error) {
	if f.batchCall >= len(f.batches) {
		return nil, nil
	}
	b := f.batches[f.batchCall]
	f.batchCall++
	return b, nil
}
func (f *fakeJiraProcStore) ResolveJiraIdentity(_ context.Context, name, _, _ string) (string, string, bool, error) {
	if v, ok := f.identities[name]; ok {
		return v[0].(string), v[1].(string), v[2].(bool), nil
	}
	return "", "", false, nil
}
func (f *fakeJiraProcStore) MintJiraContributor(_ context.Context, name, _ string) (string, error) {
	f.minted = append(f.minted, name)
	return "minted-" + name, nil
}
func (f *fakeJiraProcStore) UpsertJiraIssueFromAPI(_ context.Context, in db.JiraAPIIssue) (int64, error) {
	f.issues = append(f.issues, in)
	return 900 + int64(len(f.issues)), nil
}
func (f *fakeJiraProcStore) UpsertJiraComment(_ context.Context, in db.JiraAPIComment) (int64, error) {
	f.comments = append(f.comments, in)
	return 5000 + int64(len(f.comments)), nil
}
func (f *fakeJiraProcStore) MarkJiraStagingProcessed(_ context.Context, ids []int64) error {
	f.processed = append(f.processed, ids...)
	return nil
}

const jiraEnvelope = `{"id":"13657732","key":"AVJP-1","fields":{
  "summary":"the thing",
  "reporter":{"name":"alice-gh","key":"JIRAUSER1","displayName":"Alice Smith"},
  "status":{"name":"Resolved"},
  "resolutiondate":"2026-02-01T10:00:00.000+0000",
  "created":"2026-01-01T09:00:00.000+0000",
  "updated":"2026-02-01T10:00:00.000+0000",
  "comment":{"total":2,"comments":[
    {"id":"111","author":{"name":"alice-gh","displayName":"Alice Smith"},"body":"c1","created":"2026-01-05T08:00:00.000+0000"},
    {"id":"112","author":{"name":"jira-only-person","displayName":"Only Jira"},"body":"c2","created":"2026-01-06T08:00:00.000+0000"}
  ]}}}`

func TestJiraProcessorDrainsEnvelope(t *testing.T) {
	repoID := int64(42)
	store := &fakeJiraProcStore{
		batches: [][]db.JiraStagingRow{{
			{JsID: 1, IssueKey: "AVJP-1", RepoID: &repoID, Envelope: []byte(jiraEnvelope)},
			{JsID: 2, IssueKey: "AVJP-2", RepoID: nil, Envelope: []byte(`{"key":"AVJP-2","fields":{"summary":"x","updated":"2026-01-01T00:00:00.000+0000"}}`)},
		}},
		identities: map[string][3]any{
			"alice-gh": {"cntrb-alice", "login", false},
		},
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	n, err := p.DrainOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("processed = %d, want 1 (the nil-repo row stays staged)", n)
	}
	if len(store.issues) != 1 {
		t.Fatalf("issues = %d", len(store.issues))
	}
	is := store.issues[0]
	if is.ExternalKey != "AVJP-1" || is.JiraIssueID != 13657732 || is.Status != "Resolved" ||
		is.ReporterCntrb != "cntrb-alice" || is.RepoID != repoID {
		t.Fatalf("issue input: %+v", is)
	}
	if is.ResolutionDate.IsZero() || is.Updated.IsZero() {
		t.Fatalf("timestamps must parse: %+v", is)
	}
	if len(store.comments) != 2 {
		t.Fatalf("comments = %d", len(store.comments))
	}
	if store.comments[0].AuthorCntrbID != "cntrb-alice" {
		t.Fatalf("matched author: %+v", store.comments[0])
	}
	if store.comments[1].AuthorCntrbID != "minted-jira-only-person" {
		t.Fatalf("jira-only author must be minted: %+v", store.comments[1])
	}
	if len(store.minted) != 1 || store.minted[0] != "jira-only-person" {
		t.Fatalf("minted = %v", store.minted)
	}
	if len(store.processed) != 1 || store.processed[0] != 1 {
		t.Fatalf("processed = %v — only the drained row, never the nil-repo row", store.processed)
	}
}

// TestJiraProcessorNeverMintsAmbiguous — SR-6: an AMBIGUOUS identity
// (candidates exist, not uniquely) must neither link nor mint.
func TestJiraProcessorNeverMintsAmbiguous(t *testing.T) {
	repoID := int64(42)
	env := `{"id":"5","key":"AVJP-3","fields":{"summary":"x",
		"reporter":{"name":"ambig-person","displayName":"Same Name"},
		"status":{"name":"Open"},"updated":"2026-01-01T00:00:00.000+0000","created":"2026-01-01T00:00:00.000+0000"}}`
	store := &fakeJiraProcStore{
		batches: [][]db.JiraStagingRow{{
			{JsID: 3, IssueKey: "AVJP-3", RepoID: &repoID, Envelope: []byte(env)},
		}},
		identities: map[string][3]any{
			"ambig-person": {"", "", true},
		},
	}
	p := NewJiraProcessor(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := p.DrainOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(store.minted) != 0 {
		t.Fatalf("ambiguous identity was minted: %v — that fabricates a third person beside two candidates", store.minted)
	}
	if store.issues[0].ReporterCntrb != "" {
		t.Fatalf("ambiguous reporter must stay unattributed: %+v", store.issues[0])
	}
	_ = time.Now
}
