// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// The org scan's found branch (0.29.140, round 1 F1/F5): a gone-stamped
// repository the forge lists under the forge ID the row stores is resurrected
// — cleared, its episode closed, re-queued — but only for a PUBLIC listing.
// A member token lists an organization's private repositories too, and
// prelim's probe is unauthenticated (it answers 404 for them), so a private
// listing is no sign of life: resurrecting it would flap the repository
// between gone and queued at every org refresh.
func TestIngestDiscoveredResurrectsAPublicListingUnderTheStoredID(t *testing.T) {
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
	t.Cleanup(store.Close)
	const slug = "_avorgres"
	cleanup := func() {
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, slug)
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repo_gone_history WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, slug)
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repo_forge_id_changes WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = $1)`, slug)
		store.Pool().Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = $1`, slug)
	}
	cleanup()
	t.Cleanup(cleanup)
	url := "https://github.com/" + slug + "/r"
	repoID, err := store.UpsertRepo(ctx, &model.Repo{Platform: model.PlatformGitHub, GitURL: url, Owner: slug, Name: "r", PlatformID: "95336057"})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkRepoGone(ctx, repoID, 404, db.GoneSourcePrelim); err != nil {
		t.Fatal(err)
	}
	if err := store.DequeueRepo(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	s := &Scheduler{store: store, logger: logger}
	state := func() (gone bool, queued bool) {
		t.Helper()
		var at *string
		if err := store.Pool().QueryRow(ctx, `SELECT repo_gone_at::text FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&at); err != nil {
			t.Fatal(err)
		}
		var n int
		if err := store.Pool().QueryRow(ctx, `SELECT COUNT(*) FROM aveloxis_ops.collection_queue WHERE repo_id = $1`, repoID).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return at != nil, n == 1
	}
	item := platform.OwnerRepo{URL: url, Owner: slug, Name: "r", ForgeID: "95336057"}

	private := item
	private.Private = true
	if _, err := s.ingestDiscovered(ctx, private, model.PlatformGitHub, ingestTargets{}); err != nil {
		t.Fatal(err)
	}
	if gone, queued := state(); !gone || queued {
		t.Fatalf("a PRIVATE listing is no sign of life: gone=%v queued=%v", gone, queued)
	}

	other := item
	other.ForgeID = "1438985131"
	other.Private = true
	if _, err := s.ingestDiscovered(ctx, other, model.PlatformGitHub, ingestTargets{}); err != nil {
		t.Fatal(err)
	}
	if gone, queued := state(); !gone || queued {
		t.Fatalf("a listing under ANOTHER forge id is a pending change, not a resurrection: gone=%v queued=%v", gone, queued)
	}
	// The pending change carries the listing's visibility (Copilot PR #252,
	// second review): adopting it will move the id and keep the stamp.
	var observedPrivate bool
	if err := store.Pool().QueryRow(ctx, `SELECT observed_private FROM aveloxis_data.repo_forge_id_changes WHERE repo_id = $1 AND new_forge_id = '1438985131'`, repoID).Scan(&observedPrivate); err != nil || !observedPrivate {
		t.Fatalf("the recorded change notes the private listing: %v %v", observedPrivate, err)
	}

	out, err := s.ingestDiscovered(ctx, item, model.PlatformGitHub, ingestTargets{})
	if err != nil {
		t.Fatal(err)
	}
	if out.repoID != repoID || out.created {
		t.Fatalf("the found branch: %+v", out)
	}
	if gone, queued := state(); gone || !queued {
		t.Fatalf("a public listing under the stored id resurrects: gone=%v queued=%v", gone, queued)
	}
}
