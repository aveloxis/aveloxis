// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Worklist item 72: a `stop serve` logged as an ERROR. The scheduler
// cancels the serve ctx, waits out the shutdown grace, and then closes
// the pgx pool under whatever batch is still in flight. That batch then
// fails with pgx's "conn closed" / puddle's "closed pool" — never
// context.Canceled — so Processor.ProcessRepo's `errors.Is(err,
// context.Canceled)` classification missed it: 3x ERROR "failed to
// process entity type" plus 3x WARN "failed to upsert contributor batch"
// in the production log at a stop.
//
// THE DRIVER: the in-flight statement had already left the ctx check
// behind when the pool closed. doneUnobservedCtx models exactly that
// moment: the job's ctx reports done (Err) but its Done channel is never
// observed by the call, so pgx fails on the closed pool / the COMMIT
// rather than on the ctx. A real cancelled ctx would short-circuit in
// puddle's Acquire with context.Canceled and never reach the arm.
//
// Gated on AVELOXIS_TEST_DB (scratch DB only).

package collector

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"

	"github.com/jackc/pgx/v5/pgxpool"
)

// doneUnobservedCtx is a ctx the shutdown has cancelled (Err) whose
// cancellation the in-flight call never saw (Done is Background's nil
// channel).
type doneUnobservedCtx struct{ context.Context }

func (doneUnobservedCtx) Err() error { return context.Canceled }

func shutdownTestStore(t *testing.T) (*db.PostgresStore, string) {
	t.Helper()
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	store, err := db.NewPostgresStore(context.Background(), dsn, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	return store, dsn
}

func bufLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// The ProcessRepo arm: the pool closed under a done job ctx is an
// interruption — INFO, and the returned error carries context.Canceled
// so every caller's own `errors.Is(err, context.Canceled)` classifies it
// too (the scheduler's leftover drain WARNed on it).
func TestProcessRepoPoolClosedUnderShutdownIsNotAnError(t *testing.T) {
	store, _ := shutdownTestStore(t)
	store.Close() // the scheduler's shutdown pool close

	logger, logs := bufLogger()
	err := NewProcessor(store, logger).ProcessRepo(doneUnobservedCtx{context.Background()}, 1, int16(model.PlatformGitHub))
	if err == nil {
		t.Fatal("ProcessRepo returned nil on a closed pool — the fixture did not reach the error arm")
	}
	if !strings.Contains(err.Error(), "closed pool") {
		t.Fatalf("fixture: want the closed-pool failure, got %v", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("returned error %q does not carry context.Canceled — callers cannot classify the shutdown", err)
	}
	out := logs.String()
	if strings.Contains(out, "level=ERROR") || strings.Contains(out, "level=WARN") {
		t.Errorf("shutdown logged as a failure:\n%s", out)
	}
	if !strings.Contains(out, "entity processing aborted by shutdown") {
		t.Errorf("want the INFO interruption line, got:\n%s", out)
	}
}

// SR-5 counterpart: the same closed-pool failure under a LIVE ctx is a
// genuine defect (nobody asked to stop) and stays an ERROR, without a
// cancellation in its chain.
func TestProcessRepoPoolClosedLiveCtxStaysError(t *testing.T) {
	store, _ := shutdownTestStore(t)
	store.Close()

	logger, logs := bufLogger()
	err := NewProcessor(store, logger).ProcessRepo(context.Background(), 1, int16(model.PlatformGitHub))
	if err == nil {
		t.Fatal("ProcessRepo returned nil on a closed pool")
	}
	if errors.Is(err, context.Canceled) {
		t.Errorf("a live-ctx failure was dressed as a cancellation: %v", err)
	}
	out := logs.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "failed to process entity type") {
		t.Errorf("genuine failure not logged at ERROR:\n%s", out)
	}
	if strings.Contains(out, "aborted by shutdown") {
		t.Errorf("genuine failure logged as a shutdown:\n%s", out)
	}
}

// The contributor-batch arm: a batch failing while the job ctx is done
// is the same interruption — no WARN, no error count, no ERROR — and the
// staged rows stay unprocessed for the next drain. Under a live ctx the
// same failure keeps its WARN + ERROR (the F6 fixture's genuine shape).
func TestContributorBatchFailureUnderShutdownIsNotWarned(t *testing.T) {
	store, dsn := shutdownTestStore(t)
	t.Cleanup(store.Close)
	ctx := context.Background()
	if err := db.RunMigrations(ctx, store, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("RunMigrations: %v", err)
	}
	raw, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(raw.Close) // SR-9

	const slug = "_avc72shut"
	cleanup := func() {
		for _, sql := range []string{
			`DELETE FROM aveloxis_ops.staging WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%')`,
			`DELETE FROM aveloxis_data.repos WHERE repo_git ILIKE '%` + slug + `%'`,
			`DELETE FROM aveloxis_data.contributor_login_history WHERE login LIKE '` + slug + `%'`,
			`DELETE FROM aveloxis_data.contributor_identities WHERE login LIKE '` + slug + `%'`,
			`DELETE FROM aveloxis_data.contributors WHERE cntrb_login LIKE '` + slug + `%'`,
		} {
			cleanupExec(ctx, raw, sql)
		}
	}
	cleanup()
	t.Cleanup(cleanup)

	repoID, err := store.UpsertRepo(ctx, &model.Repo{
		Platform: model.PlatformGitHub,
		GitURL:   "https://github.com/" + slug + "/Repo",
		Owner:    slug, Name: "Repo",
	})
	if err != nil {
		t.Fatalf("UpsertRepo: %v", err)
	}
	// The poison identity fails the batch at COMMIT (deferred FK 23503).
	sw := db.NewStagingWriter(store, repoID, int16(model.PlatformGitHub), slog.New(slog.DiscardHandler))
	poison := model.Contributor{
		Login: slug + "_poison",
		Identities: []model.ContributorIdentity{{
			Platform: model.Platform(unknownPlatformID), UserID: 910072, Login: slug + "_poison",
		}},
	}
	if err := sw.Stage(ctx, EntityContributor, poison); err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if err := sw.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}

	// Shutdown arm.
	logger, logs := bufLogger()
	proc := NewProcessor(store, logger)
	err = proc.ProcessRepo(doneUnobservedCtx{ctx}, repoID, int16(model.PlatformGitHub))
	if err == nil {
		t.Fatal("fixture: the poison batch did not fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("returned error %q does not carry context.Canceled", err)
	}
	out := logs.String()
	if strings.Contains(out, "failed to upsert contributor batch") ||
		strings.Contains(out, "level=ERROR") || strings.Contains(out, "level=WARN") {
		t.Errorf("shutdown mid-contributor-batch logged as a failure:\n%s", out)
	}
	if proc.errors != 0 {
		t.Errorf("shutdown counted %d processing errors; want 0", proc.errors)
	}
	if got := countStaged(ctx, t, raw, repoID, false); got != 1 {
		t.Fatalf("staged contributor rows left unprocessed = %d; want 1 (replayed on the next drain)", got)
	}

	// Genuine arm: same failure, live ctx.
	logger, logs = bufLogger()
	err = NewProcessor(store, logger).ProcessRepo(ctx, repoID, int16(model.PlatformGitHub))
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("live-ctx batch failure: want a non-cancellation error, got %v", err)
	}
	out = logs.String()
	if !strings.Contains(out, "failed to upsert contributor batch") ||
		!strings.Contains(out, "level=ERROR") || !strings.Contains(out, "failed to process entity type") {
		t.Errorf("genuine batch failure lost its WARN/ERROR:\n%s", out)
	}
}
