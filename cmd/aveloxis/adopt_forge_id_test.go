// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

type fakeAdoptStore struct {
	repo    *model.Repo
	stored  string
	adopted []string
}

func (f *fakeAdoptStore) GetRepoByID(context.Context, int64) (*model.Repo, error) { return f.repo, nil }
func (f *fakeAdoptStore) GetRepoForgeID(context.Context, int64) (string, error)   { return f.stored, nil }
func (f *fakeAdoptStore) AdoptForgeID(_ context.Context, _ int64, oldID, newID string, created *time.Time, _, _ string) error {
	d := "nil"
	if created != nil {
		d = created.Format("2006-01-02")
	}
	f.adopted = append(f.adopted, oldID+"→"+newID+"@"+d)
	return nil
}

type fakeInfo struct {
	info *model.RepoInfo
	err  error
}

func (f fakeInfo) FetchRepoInfo(context.Context, string, string) (*model.RepoInfo, error) {
	return f.info, f.err
}

// TestAdoptForgeIDDecisions — the command writes only when the forge
// answered with a different ID; every other outcome is an error that
// changes nothing (a failed lookup is not "no change", SR-5).
func TestAdoptForgeIDDecisions(t *testing.T) {
	gh := &model.Repo{ID: 126257, Platform: model.PlatformGitHub, Owner: "intel", Name: "Enterprise-RAG", GitURL: "https://github.com/intel/Enterprise-RAG"}
	created := time.Date(2026, 9, 16, 21, 26, 15, 0, time.UTC)
	for _, tc := range []struct {
		name     string
		repo     *model.Repo
		stored   string
		fetch    fakeInfo
		wantErr  string
		wantDone string
	}{
		{"adopts the forge's new ID with its creation date", gh, "861974784",
			fakeInfo{info: &model.RepoInfo{PlatformRepoID: "1373652440", CreatedAt: created}}, "", "861974784→1373652440@2026-09-16"},
		{"same ID: nothing to adopt", gh, "861974784",
			fakeInfo{info: &model.RepoInfo{PlatformRepoID: "861974784"}}, "nothing to adopt", ""},
		{"forge lookup failed: an error, not a no-op", gh, "861974784",
			fakeInfo{err: errors.New("HTTP 502")}, "HTTP 502", ""},
		{"no stored ID: nothing to adopt over", gh, "",
			fakeInfo{info: &model.RepoInfo{PlatformRepoID: "1"}}, "no stored forge ID", ""},
		{"forge answered without an ID", gh, "861974784",
			fakeInfo{info: &model.RepoInfo{}}, "without a repository ID", ""},
		{"generic git has no forge API", &model.Repo{ID: 5, Platform: model.PlatformGenericGit, GitURL: "https://example.org/x.git"}, "1",
			fakeInfo{}, "no forge API", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := &fakeAdoptStore{repo: tc.repo, stored: tc.stored}
			clientFor := func(r *model.Repo) (repoInfoFetcher, error) {
				if r.Platform == model.PlatformGenericGit {
					return nil, errors.New("no forge API to ask for its identity (generic git)")
				}
				return tc.fetch, nil
			}
			var out bytes.Buffer
			err := adoptForgeIDFor(context.Background(), store, clientFor, tc.repo.ID, "operator", "", &out)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("want an error containing %q, got %v", tc.wantErr, err)
				}
				if len(store.adopted) != 0 {
					t.Fatalf("nothing may be written on this path, got %v", store.adopted)
				}
				return
			}
			if err != nil || len(store.adopted) != 1 || store.adopted[0] != tc.wantDone {
				t.Fatalf("err=%v adopted=%v, want %s", err, store.adopted, tc.wantDone)
			}
			if !strings.Contains(out.String(), "2026-09-16") {
				t.Errorf("the operator is told the creation date: %s", out.String())
			}
		})
	}
}

// TestAdoptClientForGuardsTheGitLabHost — review round 1: a GitLab
// repository is asked about only on the configured instance (project IDs
// are per instance; GitLab keys stay on that host — the org scan's
// guard). GitHub gets the GitHub client; generic git has no API.
func TestAdoptClientForGuardsTheGitLabHost(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	keys := platform.NewKeyPool([]string{"k"}, logger)
	gh := fakeInfo{}
	const base = "https://gitlab.com/api/v4"
	if c, err := adoptClientFor(&model.Repo{Platform: model.PlatformGitHub, GitURL: "https://github.com/o/r"}, gh, base, keys, logger); err != nil || c != gh {
		t.Errorf("github: %v %v", c, err)
	}
	if c, err := adoptClientFor(&model.Repo{Platform: model.PlatformGitLab, GitURL: "https://gitlab.com/o/r"}, gh, base, keys, logger); err != nil || c == nil {
		t.Errorf("gitlab on the configured instance: %v %v", c, err)
	}
	if _, err := adoptClientFor(&model.Repo{Platform: model.PlatformGitLab, GitURL: "https://gitlab.gnome.org/GNOME/gtk"}, gh, base, keys, logger); err == nil || !strings.Contains(err.Error(), "not the configured GitLab instance") {
		t.Errorf("gitlab on another instance must be refused: %v", err)
	}
	if _, err := adoptClientFor(&model.Repo{Platform: model.PlatformGenericGit, GitURL: "https://example.org/x.git"}, gh, base, keys, logger); err == nil {
		t.Error("generic git has no API to ask")
	}
}

// TestAdoptEachKeepsFailuresAcrossAnInterrupt — PR #212 review: a Ctrl-C
// after an earlier --repo-id had failed returned only "context canceled",
// dropping the real failure, and the id in flight when the interrupt
// landed was still logged at ERROR.
func TestAdoptEachKeepsFailuresAcrossAnInterrupt(t *testing.T) {
	// Two ways the interrupt meets the loop (review round 1 on v0.29.66:
	// only the first was driven, so the original top-of-loop bug could
	// come back unseen): the in-flight adopt returns the cancellation, or
	// it finishes nil and the NEXT iteration sees the interrupt.
	for _, inFlightErr := range []bool{true, false} {
		t.Run(fmt.Sprintf("in-flight returns error=%v", inFlightErr), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var logs bytes.Buffer
			logger := slog.New(slog.NewTextHandler(&logs, nil))
			boom := errors.New("forge said no")
			var tried []int64
			err := adoptEach(ctx, []int64{1, 2, 3}, logger, func(ctx context.Context, id int64) error {
				tried = append(tried, id)
				switch id {
				case 1:
					return boom
				case 2:
					cancel() // the interrupt lands while 2 is in flight
					if inFlightErr {
						return ctx.Err()
					}
				}
				return nil
			})
			if !errors.Is(err, boom) || !errors.Is(err, context.Canceled) {
				t.Errorf("the exit error must carry the earlier failure and the interrupt: %v", err)
			}
			if fmt.Sprint(tried) != "[1 2]" {
				t.Errorf("tried %v; nothing after the interrupt", tried)
			}
			if strings.Count(logs.String(), "level=ERROR") != 1 {
				t.Errorf("only the real failure is logged at ERROR:\n%s", logs.String())
			}
		})
	}
}
