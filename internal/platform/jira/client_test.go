// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// client_test.go — C2: the minimal Jira Server REST client. Dumb typed
// HTTP only — politeness (pacing, breaker) belongs to the CALLER (the
// worker / CLI), matching the mailing-list architecture. Pilot facts
// this client is built on (issues.apache.org, 2026-08-31): Server
// 8.20.10, anonymous reads, search honors maxResults=1000 and returns
// FULL comment bodies inline, no rate-limit headers (self-imposed
// politeness only), identity is the Server-era stable `name`.
package jira

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aveloxis/aveloxis/internal/platform"
)

const cannedSearch = `{
  "startAt": 0, "maxResults": 2, "total": 19711,
  "issues": [
    {"id": "13657732", "key": "KAFKA-15000", "fields": {
      "reporter": {"name": "arushir", "key": "JIRAUSER300459", "displayName": "Arushi Rai"},
      "assignee": null,
      "status": {"name": "Resolved"},
      "resolution": {"name": "Fixed"},
      "resolutiondate": "2024-01-05T10:00:00.000+0000",
      "created": "2023-05-01T09:00:00.000+0000",
      "updated": "2024-01-05T10:00:00.000+0000",
      "summary": "the thing",
      "comment": {"total": 1, "maxResults": 1, "comments": [
        {"id": "998877", "author": {"name": "mimaison", "displayName": "Mickael Maison"},
         "body": "looks right", "created": "2023-06-01T08:00:00.000+0000"}]}
    }},
    {"id": "13657733", "key": "KAFKA-15001", "fields": {
      "reporter": null, "status": {"name": "Open"}, "summary": "x",
      "created": "2023-05-02T09:00:00.000+0000", "updated": "2023-05-02T09:00:00.000+0000"
    }}
  ]}`

func TestSearchPageDecodesAndSendsContract(t *testing.T) {
	var gotUA, gotJQL, gotFields, gotStart, gotMax string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUA = r.Header.Get("User-Agent")
		q := r.URL.Query()
		gotJQL, gotFields, gotStart, gotMax = q.Get("jql"), q.Get("fields"), q.Get("startAt"), q.Get("maxResults")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(cannedSearch))
	}))
	defer srv.Close()

	c := New(srv.URL, "ops@example.org")
	res, err := c.SearchPage(context.Background(), "project = KAFKA ORDER BY updated ASC",
		[]string{"reporter", "comment"}, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 19711 || len(res.Issues) != 2 {
		t.Fatalf("total=%d issues=%d", res.Total, len(res.Issues))
	}
	is := res.Issues[0]
	if is.Key != "KAFKA-15000" || is.ID != "13657732" {
		t.Fatalf("issue identity: %+v", is)
	}
	if is.Fields.Reporter == nil || is.Fields.Reporter.Name != "arushir" || is.Fields.Reporter.Key != "JIRAUSER300459" {
		t.Fatalf("reporter: %+v", is.Fields.Reporter)
	}
	if is.Fields.Status == nil || is.Fields.Status.Name != "Resolved" || is.Fields.ResolutionDate == "" {
		t.Fatalf("status fields: %+v", is.Fields)
	}
	if is.Fields.Comment == nil || len(is.Fields.Comment.Comments) != 1 ||
		is.Fields.Comment.Comments[0].Author.Name != "mimaison" {
		t.Fatalf("comments: %+v", is.Fields.Comment)
	}
	// Null reporter (ancient/deleted accounts — 1% of the pilot sample)
	// decodes as nil, never a zero-valued phantom.
	if res.Issues[1].Fields.Reporter != nil {
		t.Fatal("null reporter must decode nil")
	}
	if gotUA == "" || gotJQL == "" || gotFields != "reporter,comment" || gotStart != "0" || gotMax != "100" {
		t.Fatalf("request contract: ua=%q jql=%q fields=%q start=%q max=%q", gotUA, gotJQL, gotFields, gotStart, gotMax)
	}
	for _, want := range []string{"aveloxis", "ops@example.org"} {
		if !contains(gotUA, want) {
			t.Errorf("User-Agent %q must carry %q (the polite-pool contract)", gotUA, want)
		}
	}
}

func TestSearchPageClassifiesErrors(t *testing.T) {
	status := 429
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		if status == 400 {
			// Round 23: a DEAD project key returns this body; the client
			// only classifies THIS shape as a disable-worthy skip.
			_, _ = w.Write([]byte(`{"errorMessages":["The value 'X' does not exist for the field 'project'."]}`))
		}
	}))
	defer srv.Close()
	c := New(srv.URL, "")

	_, err := c.SearchPage(context.Background(), "project = X", nil, 0, 10)
	if platform.ClassifyError(err) != platform.ClassRateLimit {
		t.Fatalf("429 must classify ClassRateLimit, got %v (%v)", platform.ClassifyError(err), err)
	}
	status = 503
	_, err = c.SearchPage(context.Background(), "project = X", nil, 0, 10)
	if platform.ClassifyError(err) != platform.ClassTransient {
		t.Fatalf("503 must classify ClassTransient, got %v (%v)", platform.ClassifyError(err), err)
	}
	// 400 = invalid JQL / dead project key (the 5 James sub-keys in the
	// pilot) — a definitive skip, not a retry.
	status = 400
	_, err = c.SearchPage(context.Background(), "project = MAILETDOCS", nil, 0, 10)
	if !errors.Is(err, ErrInvalidQuery) || platform.ClassifyError(err) != platform.ClassSkip {
		t.Fatalf("400 must wrap ErrInvalidQuery and classify ClassSkip, got %v (%v)", platform.ClassifyError(err), err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
