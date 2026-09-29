// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

// v0.29.69 (worklist 69) — the startup metadata backfill re-fetched the
// same ~5,600 repositories at every serve start (3–4 h each time on
// production), because nothing recorded that a repo had been asked: an
// honestly empty forge answer and a 404 both left the row a candidate.
// This drives the backfill twice against the real store with a fake forge:
// the second start must not re-ask a repo the first start got an ANSWER for
// (an empty one, a 404), and must re-ask one that got a non-answer (a 502
// says nothing about the repo — SR-5/SR-16, platform.IsDefinitiveAnswer).

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// metadataFakeForge answers FetchRepoInfo from a table and counts the
// calls. Every other platform.Client method but the constructor's hook is
// the nil embedded interface: reaching one panics, which is the point — the
// backfill asks nothing else.
type metadataFakeForge struct {
	platform.Client
	mu      sync.Mutex
	answers map[string]error // repo name → error (nil = an empty answer)
	calls   map[string]int
}

func (f *metadataFakeForge) FetchRepoInfo(_ context.Context, _, repo string) (*model.RepoInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[repo]++
	err, ok := f.answers[repo]
	if !ok {
		return nil, fmt.Errorf("fake forge: unexpected repo %q", repo)
	}
	if err != nil {
		return nil, err
	}
	return &model.RepoInfo{}, nil // the forge's answer really is empty
}

// OnPermanentRedirect is called by the scheduler constructor to install its
// rename hook; the fake has no HTTP client to hang it on.
func (f *metadataFakeForge) OnPermanentRedirect(func(from, to string)) {}

func TestMetadataBackfillDoesNotReaskWithinCooldown(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)

	parkOtherMetadataCandidates(t, store)

	forge := &metadataFakeForge{
		answers: map[string]error{
			"empty":     nil,
			"gone":      fmt.Errorf("GET /projects: %w", platform.ErrNotFound),
			"transient": errors.New("502 bad gateway"),
			"writefail": nil, // answered, but the store refuses the write
		},
		calls: map[string]int{},
	}
	var ids []int64
	for name := range forge.answers {
		id, err := store.UpsertRepo(ctx, &model.Repo{
			Platform: model.PlatformGitLab,
			GitURL:   "https://gitlab.com/_avmetacool/" + name,
			Owner:    "_avmetacool",
			Name:     name,
		})
		if err != nil {
			t.Fatalf("seed %s: %v", name, err)
		}
		ids = append(ids, id)
	}
	t.Cleanup(func() {
		if _, err := store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = ANY($1)`, ids); err != nil {
			t.Logf("cleanup: deleting fixture repos: %v", err)
		}
	})

	// The write of "writefail"'s answer fails in the store: an answer that
	// was never written must not be stamped (SR-3). The trigger fires only
	// on UpdateRepoMetadata (it sets repo_description), never on the stamp.
	if _, err := store.Pool().Exec(ctx, `
		CREATE OR REPLACE FUNCTION aveloxis_data._avmetacool_refuse() RETURNS trigger AS $$
		BEGIN RAISE EXCEPTION 'avmetacool: write refused'; END $$ LANGUAGE plpgsql;
		CREATE TRIGGER _avmetacool_refuse BEFORE UPDATE OF repo_description ON aveloxis_data.repos
		FOR EACH ROW WHEN (NEW.repo_owner = '_avmetacool' AND NEW.repo_name = 'writefail')
		EXECUTE FUNCTION aveloxis_data._avmetacool_refuse();`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := store.Pool().Exec(context.Background(), `
			DROP TRIGGER IF EXISTS _avmetacool_refuse ON aveloxis_data.repos;
			DROP FUNCTION IF EXISTS aveloxis_data._avmetacool_refuse();`); err != nil {
			t.Logf("cleanup: dropping the write-refusing trigger: %v", err)
		}
	})

	cfg := config.DefaultConfig()
	// A GitLab pool with a usable key: without one the backfill skips GitLab
	// candidates (PR #218 review E15).
	glKeys := platform.NewKeyPool([]string{"glpat-avmetacool"}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := NewWithKeys(store, nil, forge, nil, glKeys, slog.New(slog.NewTextHandler(io.Discard, nil)), Config{Collection: &cfg.Collection})

	s.runRepoMetadataBackfill(ctx) // the first serve start asks every candidate once
	for name := range forge.answers {
		if forge.calls[name] != 1 {
			t.Fatalf("first start: %s asked %d times, want 1", name, forge.calls[name])
		}
	}

	s.runRepoMetadataBackfill(ctx) // the next serve start, within the recollect interval
	for _, name := range []string{"empty", "gone"} {
		if forge.calls[name] != 1 {
			t.Errorf("second start re-asked %s (answer %v): %d calls — a repo answered within one recollect interval must not be re-fetched at every serve start",
				name, forge.answers[name], forge.calls[name])
		}
	}
	if forge.calls["transient"] != 2 {
		t.Errorf("second start asked the 502 repo %d times in total, want 2 — a non-answer must not be stamped, or a pool-level outage parks every repo it touched for a full recollect interval",
			forge.calls["transient"])
	}
	if forge.calls["writefail"] != 2 {
		t.Errorf("second start asked the repo whose write failed %d times in total, want 2 — an answer that was never written must not be stamped (SR-3)",
			forge.calls["writefail"])
	}
}

// parkOtherMetadataCandidates stamps every unstamped repository so only a
// test's own fixtures are backfill candidates. Rows other tests in this
// package left behind would be candidates too, each costing the backfill's
// one-second pacing; they are parked for the duration (this package's
// database is its own) and put back.
func parkOtherMetadataCandidates(t *testing.T, store *db.PostgresStore) {
	t.Helper()
	ctx := context.Background()
	var parked []int64
	rows, err := store.Pool().Query(ctx, `
		UPDATE aveloxis_data.repos SET metadata_backfill_attempted_at = NOW()
		WHERE metadata_backfill_attempted_at IS NULL RETURNING repo_id`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		parked = append(parked, id)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if _, err := store.Pool().Exec(context.Background(),
			`UPDATE aveloxis_data.repos SET metadata_backfill_attempted_at = NULL WHERE repo_id = ANY($1)`, parked); err != nil {
			t.Logf("cleanup: un-parking repos: %v", err)
		}
	})
}

// TestMetadataBackfillSkipsGitLabWithoutUsableKey (PR #218 review E15): a
// GitHub candidate met with no usable GitHub key is skipped and counted
// apart, but a GitLab candidate on a deployment whose GitLab pool has no
// usable key was fetched anyway, failed on the pool's ErrNoKeys and was
// counted as failed at every restart. It must be skipped the same way: no
// fetch, no stamp (nothing was answered), counted as skipped, not failed.
func TestMetadataBackfillSkipsGitLabWithoutUsableKey(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	store, err := db.NewPostgresStore(ctx, dsn, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	parkOtherMetadataCandidates(t, store)

	id, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitLab,
		GitURL:   "https://gitlab.com/_avmetanokey/project",
		Owner:    "_avmetanokey",
		Name:     "project",
	})
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Cleanup(func() {
		if _, err := store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id); err != nil {
			t.Logf("cleanup: deleting fixture repo: %v", err)
		}
	})

	forge := &metadataFakeForge{answers: map[string]error{"project": nil}, calls: map[string]int{}}
	var logs strings.Builder // the backfill runs on this goroutine
	logger := slog.New(slog.NewTextHandler(&logs, nil))
	emptyGitLab := platform.NewKeyPool(nil, logger) // loadKeys' shape for "no GitLab keys"
	cfg := config.DefaultConfig()
	s := NewWithKeys(store, nil, forge, nil, emptyGitLab, logger, Config{Collection: &cfg.Collection})

	s.runRepoMetadataBackfill(ctx)
	if forge.calls["project"] != 0 {
		t.Errorf("a GitLab candidate was fetched %d times with no usable GitLab key; want 0 (skipped like the GitHub arm)", forge.calls["project"])
	}
	var stamped bool
	if err := store.Pool().QueryRow(ctx,
		`SELECT metadata_backfill_attempted_at IS NOT NULL FROM aveloxis_data.repos WHERE repo_id = $1`, id).Scan(&stamped); err != nil {
		t.Fatal(err)
	}
	if stamped {
		t.Error("a skipped GitLab candidate was stamped; nothing was answered, so it must stay a candidate for a restart with a key")
	}
	out := logs.String()
	if !strings.Contains(out, "repo metadata backfill complete") ||
		!strings.Contains(out, "failed=0") || !strings.Contains(out, "skipped_no_gitlab_key=1") {
		t.Errorf("the completion line must count the candidate as skipped_no_gitlab_key=1 and failed=0; log:\n%s", out)
	}
}
