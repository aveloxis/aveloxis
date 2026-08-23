// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package collector

// Behavioral regression test for the 2026-07-09 data-test incident:
// the one-shot collect path must actually STORE events. The legacy
// direct-write path passed events straight to UpsertIssueEvent with the
// local-serial IssueID field at its zero value (the platform client
// only populates PlatformIssueID — the number), so Postgres rejected
// every event with FK 23503 and the one-shot path had never stored a
// single event row. Only the staged Processor resolves number → serial.
//
// This test drives Collector.collectAndProcess (the v0.26.2 staged
// delegation — API phases only, no facade/git so no network beyond the
// httptest server) against a mock GitHub and a real Postgres, and
// asserts the event lands with a VALID FK. Under the pre-v0.26.2 code
// this exact flow produced zero event rows.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

func TestCollectStoresEventsEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	// t.Cleanup, not defer: function defers run BEFORE t.Cleanup
	// callbacks, and the seed-row cleanup below needs the pool alive.
	t.Cleanup(store.Close)
	store.SetMatviewSkip(true)
	testMigrate(ctx, t, store)

	const owner, repo = "aveloxis-it", "collect-events"

	// Mock GitHub: one issue + one issue event referencing it. Every
	// unmatched GET returns an empty JSON array so the remaining staged
	// phases no-op cleanly.
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/"+owner+"/"+repo+"/issues", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{{
			"id": 900001, "number": 7, "title": "seeded issue",
			"state":      "closed",
			"user":       map[string]any{"login": "alice", "id": 42},
			"created_at": "2026-01-02T03:04:05Z",
			"updated_at": "2026-01-02T03:04:05Z",
		}})
	})
	mux.HandleFunc("/repos/"+owner+"/"+repo+"/issues/events", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{{
			"id": 700001, "event": "closed",
			"actor":      map[string]any{"login": "alice", "id": 42},
			"created_at": "2026-01-03T00:00:00Z",
			"issue": map[string]any{
				"id": 900001, "number": 7,
			},
		}})
	})
	mux.HandleFunc("/repos/"+owner+"/"+repo, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": 555, "full_name": owner + "/" + repo})
	})
	mux.HandleFunc("/graphql", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"data":{"repository":null}}`)
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "[]")
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	keys := platform.NewKeyPool([]string{"test-token"}, logger)
	client := github.New(srv.URL, keys, logger)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + owner + "/" + repo,
		Owner:    owner,
		Name:     repo,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, tbl := range []string{"issue_events", "issues", "staging"} {
			schema := "aveloxis_data"
			if tbl == "staging" {
				schema = "aveloxis_ops"
			}
			_, _ = store.Pool().Exec(context.Background(),
				fmt.Sprintf("DELETE FROM %s.%s WHERE repo_id = $1", schema, tbl), repoID)
		}
	})

	// REST modes keep the mock surface small; the number→serial
	// resolution under test is mode-independent (it lives in the
	// Processor).
	coll := NewWithOptions(client, store, logger, nil, t.TempDir()).
		WithCollectionModes("rest", "rest", "single", defaultShardSize, "rest")

	result, err := coll.collectAndProcess(ctx, repoID, owner, repo, time.Time{})
	if err != nil {
		t.Fatalf("collectAndProcess: %v", err)
	}
	if result == nil {
		t.Fatal("nil result")
	}

	// The assertion that fails on the pre-v0.26.2 path: the event must
	// exist AND its issue_id FK must resolve to the seeded issue.
	var got int
	err = store.Pool().QueryRow(ctx, `
		SELECT COUNT(*) FROM aveloxis_data.issue_events e
		JOIN aveloxis_data.issues i ON i.issue_id = e.issue_id
		WHERE e.repo_id = $1 AND e.platform_event_id = 700001
		  AND i.platform_issue_id = 900001`, repoID).Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	if got != 1 {
		t.Errorf("expected 1 issue_event with a resolved FK to the seeded issue, got %d — "+
			"the one-shot collect path is dropping events again (2026-07-09 incident class)", got)
	}
}
