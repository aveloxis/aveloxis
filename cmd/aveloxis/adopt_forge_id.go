// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"os/signal"
	"os/user"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// adopt-forge-id (v0.29.62, 2026-09-23 operator decision): a repository
// deleted and re-created upstream under the same URL gets a new forge ID.
// The org scan records the change as PENDING (observation-only); this
// command is the operator's approval to treat the new repository as a
// continuation. It asks the forge for the current ID and its creation
// date, moves the stored ID, and records the change, which the repository
// page then shows because the row now holds data from two upstreams.

type adoptForgeIDStore interface {
	GetRepoByID(ctx context.Context, repoID int64) (*model.Repo, error)
	GetRepoForgeID(ctx context.Context, repoID int64) (string, error)
	AdoptForgeID(ctx context.Context, repoID int64, oldID, newID string, forgeCreatedAt *time.Time, adoptedBy, note string) error
}

type repoInfoFetcher interface {
	FetchRepoInfo(ctx context.Context, owner, repo string) (*model.RepoInfo, error)
}

// errNothingToAdopt: the forge's ID already equals the stored one.
var errNothingToAdopt = errors.New("the stored forge ID already matches the forge — nothing to adopt")

// adoptForgeIDFor decides and applies one adoption. clientFor returns the
// forge client that may be asked about this repository, or an error saying
// why none may (no API for generic git; a GitLab host other than the
// configured instance — project IDs are per instance, and GitLab keys are
// only sent to that host). Every failure is returned; nothing is written
// unless the forge answered with an ID that differs from the stored one.
func adoptForgeIDFor(ctx context.Context, store adoptForgeIDStore, clientFor func(*model.Repo) (repoInfoFetcher, error),
	repoID int64, adoptedBy, note string, out io.Writer) error {
	repo, err := store.GetRepoByID(ctx, repoID)
	if err != nil {
		return fmt.Errorf("repo %d: %w", repoID, err)
	}
	client, err := clientFor(repo)
	if err != nil {
		return fmt.Errorf("repo %d (%s): %w", repoID, repo.GitURL, err)
	}
	stored, err := store.GetRepoForgeID(ctx, repoID)
	if err != nil {
		return fmt.Errorf("repo %d stored forge ID: %w", repoID, err)
	}
	if stored == "" {
		return fmt.Errorf("repo %d has no stored forge ID — the next collection fills it; there is nothing to adopt over", repoID)
	}
	info, err := client.FetchRepoInfo(ctx, repo.Owner, repo.Name)
	if err != nil {
		return fmt.Errorf("repo %d: asking %s for its current identity: %w", repoID, repo.GitURL, err)
	}
	if info == nil || info.PlatformRepoID == "" {
		return fmt.Errorf("repo %d: the forge answered without a repository ID", repoID)
	}
	if info.PlatformRepoID == stored {
		return fmt.Errorf("repo %d (%s): %w", repoID, repo.GitURL, errNothingToAdopt)
	}
	var created *time.Time
	if !info.CreatedAt.IsZero() {
		c := info.CreatedAt.UTC()
		created = &c
	}
	if err := store.AdoptForgeID(ctx, repoID, stored, info.PlatformRepoID, created, adoptedBy, note); err != nil {
		return err
	}
	when := "an unknown date"
	if created != nil {
		when = created.Format("2006-01-02")
	}
	fmt.Fprintf(out, "repo %d (%s): adopted forge ID %s (the forge created it on %s), replacing %s. The repository page now shows the change.\n",
		repoID, repo.GitURL, info.PlatformRepoID, when, stored)
	return nil
}

// adoptClientFor picks the forge client for one repository: GitHub's for a
// GitHub repository; for GitLab, a client on the configured instance only
// when the repository lives on that instance's host (the org scan's guard,
// platform.GitLabAPIBaseForHost) — asking another instance would read an
// unrelated project's ID (SR-6) and send keys off-host; generic git has no
// API to ask.
func adoptClientFor(r *model.Repo, gh repoInfoFetcher, gitlabBase string, glKeys *platform.KeyPool, logger *slog.Logger) (repoInfoFetcher, error) {
	switch r.Platform {
	case model.PlatformGitHub:
		return gh, nil
	case model.PlatformGitLab:
		u, err := url.Parse(r.GitURL)
		if err != nil || u.Host == "" {
			return nil, fmt.Errorf("cannot read the host of %q", r.GitURL)
		}
		apiBase, ok := platform.GitLabAPIBaseForHost(gitlabBase, u.Host)
		if !ok {
			return nil, fmt.Errorf("the repository is on %s, not the configured GitLab instance (gitlab.base_url %q) — its project ID can only be read there", u.Host, gitlabBase)
		}
		return gitlab.New(apiBase, glKeys, logger), nil
	}
	return nil, fmt.Errorf("no forge API to ask for its identity (generic git)")
}

func adoptForgeIDCmd(cfgPath *string) *cobra.Command {
	var (
		repoIDs []int64
		list    bool
		note    string
	)
	cmd := &cobra.Command{
		Use:   "adopt-forge-id",
		Short: "Treat a repository re-created upstream under the same URL as a continuation",
		Long: `A repository deleted and re-created on its forge under the same URL gets a
new forge ID. The org scan records such a change as PENDING and logs a
"forge-ID mismatch" ERROR; it never changes the repository row itself.

--list prints the recorded changes (pending first). --repo-id N asks the
forge for the repository's current ID and creation date, replaces the
stored ID, and records the change as adopted. The repository page then
shows a notice: the row holds data from both upstream repositories, which
may affect its statistics. The mismatch ERROR stops once adopted.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !list && len(repoIDs) == 0 {
				return fmt.Errorf("pass --list, or --repo-id N (repeatable) to adopt")
			}
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)
			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()
			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			if list {
				changes, err := store.ListForgeIDChanges(ctx, false)
				if err != nil {
					return fmt.Errorf("listing forge-ID changes: %w", err)
				}
				if len(changes) == 0 {
					fmt.Println("no forge-ID changes recorded")
				}
				for _, c := range changes {
					state := "PENDING"
					if c.Superseded {
						state = "superseded (not adoptable: the repository does not store " + c.OldForgeID + ")"
					}
					if c.AdoptedAt != nil {
						state = "adopted " + c.AdoptedAt.UTC().Format("2006-01-02")
					}
					fmt.Printf("repo %d  %s  %s → %s  first seen %s  %s\n", c.RepoID, c.RepoGit, c.OldForgeID, c.NewForgeID,
						c.FirstObservedAt.UTC().Format("2006-01-02"), state)
				}
				if len(repoIDs) == 0 {
					return nil
				}
			}

			ghKeys, glKeys, err := loadKeys(ctx, cfg, store, false, logger)
			if err != nil {
				return fmt.Errorf("loading API keys: %w", err)
			}
			gh := github.New(cfg.GitHub.GitHubAPIBase(), ghKeys, logger)
			clientFor := func(r *model.Repo) (repoInfoFetcher, error) {
				return adoptClientFor(r, gh, cfg.GitLab.BaseURL, glKeys, logger)
			}
			adoptedBy := "operator"
			if u, uerr := user.Current(); uerr == nil && u.Username != "" {
				adoptedBy = u.Username
			}
			return adoptEach(ctx, repoIDs, logger, func(ctx context.Context, id int64) error {
				return adoptForgeIDFor(ctx, store, clientFor, id, adoptedBy, note, os.Stdout)
			})
		},
	}
	cmd.Flags().Int64SliceVar(&repoIDs, "repo-id", nil, "repository to adopt the forge's current ID for (repeatable)")
	cmd.Flags().BoolVar(&list, "list", false, "list the recorded forge-ID changes")
	cmd.Flags().StringVar(&note, "note", "", "optional operator note stored with the change")
	return cmd
}

// adoptEach adopts each --repo-id in turn. A failure is logged and the
// loop goes on; an interrupt ends it at once (no ERROR per remaining id,
// and none for the id in flight), and the exit error carries every failure
// seen so far as well as the interrupt (PR #212 review: returning only
// ctx.Err() dropped them).
func adoptEach(ctx context.Context, repoIDs []int64, logger *slog.Logger, adopt func(context.Context, int64) error) error {
	var failed []error
	for _, id := range repoIDs {
		if ctx.Err() != nil {
			return errors.Join(append(failed, ctx.Err())...)
		}
		if err := adopt(ctx, id); err != nil {
			if ctx.Err() != nil {
				return errors.Join(append(failed, ctx.Err())...)
			}
			logger.Error("adopt-forge-id failed", "repo_id", id, "error", err)
			failed = append(failed, err)
		}
	}
	return errors.Join(failed...)
}
