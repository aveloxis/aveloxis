// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package scheduler

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// Worklist item 82: the scheduler stores the forge's notice from any
// phase's error (REST's block object, the clone's remote text) and from
// the one REST request it makes after prelim sidelines a 451; a clone that
// succeeds clears it.

func forgeNoticeFixture(t *testing.T) (*Scheduler, *db.PostgresStore, *bytes.Buffer, func(platformID int) *model.Repo) {
	t.Helper()
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
	var logs bytes.Buffer
	s := &Scheduler{store: store, logger: slog.New(slog.NewTextHandler(&logs, nil))}
	n := 0
	seed := func(platformID int) *model.Repo {
		t.Helper()
		n++
		host := map[int]string{1: "github.com", 2: "gitlab.com", 3: "git.example.org"}[platformID]
		name := fmt.Sprintf("r%d", n)
		var id int64
		if err := store.Pool().QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_name, repo_owner, platform_id)
			VALUES ($1, $2, '_avnotice', $3)
			ON CONFLICT (repo_git) DO UPDATE SET repo_unavailable_reason = NULL, repo_unavailable_url = NULL
			RETURNING repo_id`, "https://"+host+"/_avnotice/"+name, name, platformID).Scan(&id); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.repos WHERE repo_id = $1`, id)
		})
		return &model.Repo{ID: id, Platform: model.Platform(platformID), Owner: "_avnotice", Name: name}
	}
	return s, store, &logs, seed
}

func storedNotice(t *testing.T, store *db.PostgresStore, id int64) (string, string) {
	t.Helper()
	var r, u string
	if err := store.Pool().QueryRow(context.Background(),
		`SELECT COALESCE(repo_unavailable_reason, ''), COALESCE(repo_unavailable_url, '') FROM aveloxis_data.repos WHERE repo_id = $1`, id).
		Scan(&r, &u); err != nil {
		t.Fatal(err)
	}
	return r, u
}

func TestRecordForgeNotice(t *testing.T) {
	s, store, logs, seed := forgeNoticeFixture(t)
	ctx := context.Background()
	notice := platform.ForgeNotice{Message: "Repository access blocked", Reason: "dmca", URL: "https://github.com/github/dmca/x.md"}
	wrapped := fmt.Errorf("phase: %w", &platform.NoticeError{Notice: notice, Err: platform.ErrGone})

	gh := seed(1)
	s.recordForgeNotice(ctx, gh, wrapped)
	if r, u := storedNotice(t, store, gh.ID); r != "Repository access blocked (dmca)" || u != notice.URL {
		t.Errorf("GitHub repo: stored %q %q", r, u)
	}
	if !strings.Contains(logs.String(), "forge notice recorded") {
		t.Errorf("no INFO line for the recorded notice:\n%s", logs.String())
	}

	gl := seed(2)
	s.recordForgeNotice(ctx, gl, &platform.NoticeError{Notice: platform.ForgeNotice{Message: "disabled"}, Err: errors.New("x")})
	if r, _ := storedNotice(t, store, gl.ID); r != "disabled" {
		t.Errorf("GitLab repo: stored %q, want the clone's remote text", r)
	}

	// A generic git host is not a forge whose words the page repeats:
	// any server can print any remote text.
	generic := seed(3)
	s.recordForgeNotice(ctx, generic, wrapped)
	if r, _ := storedNotice(t, store, generic.ID); r != "" {
		t.Errorf("generic git repo: stored %q, want nothing", r)
	}

	// No notice in the chain, or no error: nothing written, a stored
	// notice left alone (only a success clears it).
	s.recordForgeNotice(ctx, gh, errors.New("plain"))
	s.recordForgeNotice(ctx, gh, nil)
	if r, _ := storedNotice(t, store, gh.ID); r == "" {
		t.Error("an error without a notice erased the stored one")
	}
}

type fakeNoticeFetcher struct {
	n   platform.ForgeNotice
	ok  bool
	err error
}

func (f fakeNoticeFetcher) FetchRepoNotice(context.Context, string, string) (platform.ForgeNotice, bool, error) {
	return f.n, f.ok, f.err
}

func TestCaptureBlockNotice(t *testing.T) {
	s, store, logs, seed := forgeNoticeFixture(t)
	ctx := context.Background()

	blocked := seed(1)
	s.captureBlockNotice(ctx, blocked, fakeNoticeFetcher{n: platform.ForgeNotice{Message: "Repository access blocked", Reason: "dmca"}, ok: true})
	if r, _ := storedNotice(t, store, blocked.ID); r != "Repository access blocked (dmca)" {
		t.Errorf("stored %q", r)
	}

	none := seed(1)
	s.captureBlockNotice(ctx, none, fakeNoticeFetcher{})
	if r, _ := storedNotice(t, store, none.ID); r != "" {
		t.Errorf("a normal answer stored %q", r)
	}

	failed := seed(1)
	s.captureBlockNotice(ctx, failed, fakeNoticeFetcher{err: platform.ErrTransient})
	if r, _ := storedNotice(t, store, failed.ID); r != "" {
		t.Errorf("a failed request stored %q", r)
	}
	if !strings.Contains(logs.String(), "could not fetch the forge's block notice") {
		t.Errorf("a failed notice request must be logged:\n%s", logs.String())
	}
	// Shutdown is not a failure.
	logs.Reset()
	s.captureBlockNotice(ctx, failed, fakeNoticeFetcher{err: context.Canceled})
	if logs.Len() != 0 {
		t.Errorf("a cancelled request logged:\n%s", logs.String())
	}
}

func TestNoteCloneOutcome(t *testing.T) {
	s, store, _, seed := forgeNoticeFixture(t)
	ctx := context.Background()
	r := seed(1)
	s.noteCloneOutcome(ctx, r, false, fmt.Errorf("clone/fetch: %w", &platform.NoticeError{
		Notice: platform.ForgeNotice{Message: "Access to this repository has been disabled by GitHub staff."}, Err: errors.New("exit 128")}))
	if got, _ := storedNotice(t, store, r.ID); got != "Access to this repository has been disabled by GitHub staff." {
		t.Errorf("stored %q", got)
	}
	// Review round 1 F4: the clone succeeded but git log then failed — the
	// forge answered, so the notice is cleared all the same.
	s.noteCloneOutcome(ctx, r, true, errors.New("git log: exit status 128"))
	if got, _ := storedNotice(t, store, r.ID); got != "" {
		t.Errorf("a clone that succeeded (git log failed after it) left %q", got)
	}
}

// TestRecheckBlockNotice — review round 1 F1: a repository prelim already
// sidelined for a legal block never runs the API phase or the facade again,
// so the gone recheck (which sees its 451 every cadence) is the only place
// its notice can be captured. GitHub rows only; a missing row is logged.
func TestRecheckBlockNotice(t *testing.T) {
	s, store, logs, seed := forgeNoticeFixture(t)
	ctx := context.Background()
	f := fakeNoticeFetcher{n: platform.ForgeNotice{Message: "Repository access blocked", Reason: "dmca", URL: "https://github.com/github/dmca/x.md"}, ok: true}

	gh := seed(1)
	s.recheckBlockNotice(ctx, gh.ID, f)
	if r, u := storedNotice(t, store, gh.ID); r != "Repository access blocked (dmca)" || u != f.n.URL {
		t.Errorf("GitHub row: stored %q %q", r, u)
	}
	gl := seed(2)
	s.recheckBlockNotice(ctx, gl.ID, f)
	if r, _ := storedNotice(t, store, gl.ID); r != "" {
		t.Errorf("GitLab row: stored %q — the GitHub REST notice must not be asked for another forge's repository", r)
	}
	logs.Reset()
	s.recheckBlockNotice(ctx, -42, f)
	if !strings.Contains(logs.String(), "gone recheck: could not load the repository to fetch its block notice") {
		t.Errorf("a missing row must be logged:\n%s", logs.String())
	}
}

// TestFillCommitBounds drives the recheck helper against the store: an
// unfilled gone repository is filled from its commit rows.
func TestFillCommitBounds(t *testing.T) {
	s, store, logs, seed := forgeNoticeFixture(t)
	ctx := context.Background()
	r := seed(1)
	if _, err := store.Pool().Exec(ctx, `INSERT INTO aveloxis_data.commits (repo_id, cmt_commit_hash, cmt_filename, cmt_author_timestamp)
		VALUES ($1, 'cccccccccccccccccccccccccccccccccccccccc', 'f', '2016-06-06Z')`, r.ID); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = store.Pool().Exec(context.Background(), `DELETE FROM aveloxis_data.commits WHERE repo_id = $1`, r.ID)
	})
	s.fillCommitBounds(ctx, r.ID)
	var last *time.Time
	if err := store.Pool().QueryRow(ctx, `SELECT last_commit_at FROM aveloxis_data.repos WHERE repo_id = $1`, r.ID).Scan(&last); err != nil {
		t.Fatal(err)
	}
	if last == nil || last.UTC().Year() != 2016 {
		t.Errorf("last_commit_at = %v, want 2016", last)
	}
	if strings.Contains(logs.String(), "level=WARN") {
		t.Errorf("unexpected WARN:\n%s", logs.String())
	}
}
