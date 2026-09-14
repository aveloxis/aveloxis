// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/forgekeys"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// forgeClients is what a collecting command needs to reach the forges: the
// GitHub client and key pool, the per-instance GitLab router, and — for a
// process that reloads keys live (serve; v0.30.0 Phase C) — the loader, the
// effective instances and what startup refused, so forgekeys.Maintainer
// continues from exactly the startup resolution.
type forgeClients struct {
	gh     *github.Client
	ghKeys *platform.KeyPool
	gl     *gitlab.Instances

	loader           *forgekeys.Loader
	instances        []config.GitLabInstance
	startupNotLoaded []forgekeys.NotLoaded
}

// buildForgeClients is the one place forge API clients and key pools are
// built for collecting commands (serve, collect, add-repo,
// backfill-repo-metadata, heal-collection-gaps). v0.30.0 (multi-instance
// GitLab): each registered GitLab instance gets its OWN key pool — from its
// config entry and the stored keys tagged with its web URL
// (forgekeys.PartitionGitLabTokens) — and its own client on its own API URL.
// Phase C: every registered instance gets a pool and a client even with no
// keys, so a key added at runtime can make it collectable; the router's
// KeyedClient gate keeps a keyless instance off the API. register syncs the
// platforms registry first (serve and migrate only); every other caller
// reads it, and a configured instance that is not registered yet is left out
// with a WARN (its repositories take the not-configured path).
func buildForgeClients(ctx context.Context, cfg *config.Config, store *db.PostgresStore, useAugurKeys, register bool, logger *slog.Logger) (*forgeClients, error) {
	instances, err := cfg.GitLab.EffectiveInstances()
	if err != nil {
		return nil, fmt.Errorf("gitlab config: %w", err)
	}

	loader := forgekeys.NewLoader(store.Pool(), useAugurKeys, logger)
	ghKeys, ghCount := loadGitHubKeyPool(ctx, cfg, loader, logger)

	stored, err := loader.Load(ctx, "gitlab")
	if err != nil {
		// Same degradation as the GitHub loader: config keys still load,
		// the failure is an ERROR, never a silent empty set.
		logger.Error("failed to load GitLab API keys from database — only config keys are used", "error", err)
		stored = forgekeys.Stored{}
	}
	pools, part, err := forgekeys.PartitionGitLabTokens(instances, stored)
	if err != nil {
		return nil, err
	}
	forgekeys.LogOrphans(logger, part.Orphans)
	forgekeys.LogConflicts(logger, part.NotLoaded)

	var ids map[string]model.Platform
	if register {
		ids, err = registerGitLabInstances(ctx, cfg, store, logger)
	} else {
		ids, err = store.LoadGitLabInstanceRegistry(ctx)
		if err != nil {
			err = fmt.Errorf("gitlab instance registry: %w", err)
		}
	}
	if err != nil {
		return nil, err
	}

	maxInflight := cfg.Collection.GitHubMaxInflightValue()
	reservePct := cfg.Collection.GitHubBudgetForegroundReservePctValue()
	// GitLab gets only the pool-wide ceiling as a backstop: the per-key
	// ceiling is derived from GitHub's per-key secondary limits, which
	// GitLab does not have in that shape (2026-09-12 admission review).
	const glPerKey = 0
	glCount := 0
	var list []*gitlab.InstanceSpec
	for _, in := range instances {
		id, ok := ids[in.WebBase]
		if !ok {
			logger.Warn("configured GitLab instance is not registered yet — run `aveloxis migrate`; its repositories are not collected over the API until then",
				"web_url", in.WebBase, "api_url", in.APIURL)
			continue
		}
		// The pool and the API URL come from the same instance (in):
		// TestKeyedClientBaseURLAllowlist accepts exactly this shape.
		pool := platform.NewKeyPool(pools[in.WebBase], logger)
		pool.SetAdmission(maxInflight, glPerKey, reservePct)
		client, err := gitlab.New(id, in.WebBase, in.APIURL, pool, logger)
		if err != nil {
			return nil, err
		}
		toks := len(pools[in.WebBase])
		glCount += toks
		list = append(list, &gitlab.InstanceSpec{ID: id, WebBase: in.WebBase, APIURL: in.APIURL, Primary: in.Primary, Client: client})
		logger.Info("GitLab instance",
			"platform_id", id, "web_url", in.WebBase, "api_url", in.APIURL, "keys", toks,
			"main", in.Primary, "max_inflight", maxInflight, "max_inflight_per_key", glPerKey,
			"foreground_reserve_pct", reservePct)
		if toks == 0 {
			logger.Warn("GitLab instance has no API keys — its repositories are collected git-only and each job records why",
				"web_url", in.WebBase, "platform_id", id)
		}
	}
	router, err := gitlab.NewInstances(list)
	if err != nil {
		return nil, err
	}
	if ghCount == 0 && glCount == 0 {
		return nil, fmt.Errorf("no API keys configured for any platform — add keys via 'aveloxis add-key <token> --platform github' or store them in the database. Collection is impossible without API keys")
	}
	return &forgeClients{
		gh:               github.New(cfg.GitHub.BaseURL, ghKeys, logger),
		ghKeys:           ghKeys,
		gl:               router,
		loader:           loader,
		instances:        instances,
		startupNotLoaded: part.NotLoaded,
	}, nil
}

// keyMaintainer returns serve's live key reload (v0.30.0 Phase C): the same
// loader, config tokens and instances startup resolved with, the pools it
// built, and this process's report identity.
func (c *forgeClients) keyMaintainer(cfg *config.Config, store *db.PostgresStore, logger *slog.Logger) *forgekeys.Maintainer {
	return forgekeys.NewMaintainer(forgekeys.MaintainerConfig{
		GitHubConfigTokens: cfg.GitHub.APIKeys,
		GitHubAPIURL:       cfg.GitHub.BaseURL,
		GitHub:             c.ghKeys,
		Instances:          c.instances,
		GitLab:             c.gl,
		Loader:             c.loader,
		Reports:            store,
		Reporter:           forgekeys.ReporterID("serve"),
		StartupNotLoaded:   c.startupNotLoaded,
		Logger:             logger,
	})
}

// loadGitHubKeyPool builds the GitHub key pool: config keys plus stored keys
// (aveloxis_ops, and Augur's when loader decided to fall back), each token
// once (forgekeys.GitHubKeys), with the 2026-09-12 admission settings. The
// effective admission values are logged at the point of use (SR-10). A read
// error loads config keys only, at ERROR. It returns the pool (possibly
// empty) and its size.
func loadGitHubKeyPool(ctx context.Context, cfg *config.Config, loader *forgekeys.Loader, logger *slog.Logger) (*platform.KeyPool, int) {
	stored, err := loader.Load(ctx, "github")
	if err != nil {
		logger.Error("failed to load GitHub API keys from database — only config keys are used", "error", err)
		stored = forgekeys.Stored{}
	} else if n := len(stored.Database) + len(stored.Augur); n > 0 {
		logger.Info("loaded GitHub keys from database", "count", n, "augur", len(stored.Augur))
	}
	tokens := forgekeys.Tokens(forgekeys.GitHubKeys(cfg.GitHub.APIKeys, stored))
	if len(tokens) == 0 {
		logger.Warn("no GitHub API keys configured — GitHub repos will not be collected")
	}
	pool := platform.NewKeyPool(tokens, logger)
	maxInflight := cfg.Collection.GitHubMaxInflightValue()
	maxPerKey := cfg.Collection.GitHubMaxInflightPerKeyValue()
	reservePct := cfg.Collection.GitHubBudgetForegroundReservePctValue()
	pool.SetAdmission(maxInflight, maxPerKey, reservePct)
	logger.Info("GitHub API key pool admission",
		"github_keys", len(tokens), "max_inflight", maxInflight,
		"max_inflight_per_key", maxPerKey, "foreground_reserve_pct", reservePct)
	return pool, len(tokens)
}

// loadGitHubKeys is loadGitHubKeyPool for the GitHub-only commands
// (data-verify, run-scorecard, backfill-identities, heal-messages, web org
// scans). It errors when no GitHub key is configured, returning the empty
// pool anyway so a caller that tolerates that still has one.
func loadGitHubKeys(ctx context.Context, cfg *config.Config, store *db.PostgresStore, useAugurKeys bool, logger *slog.Logger) (*platform.KeyPool, error) {
	pool, n := loadGitHubKeyPool(ctx, cfg, forgekeys.NewLoader(store.Pool(), useAugurKeys, logger), logger)
	if n == 0 {
		return pool, fmt.Errorf("no GitHub API keys configured — add keys via 'aveloxis add-key <token> --platform github'")
	}
	return pool, nil
}

// registerGitLabInstances syncs the platforms registry with the configured
// GitLab instances and then adopts repositories onto newly registered
// instances (db.AdoptReposForGitLabInstances) — serve startup and the migrate
// command are its only callers. It returns web base → platform_id.
func registerGitLabInstances(ctx context.Context, cfg *config.Config, store *db.PostgresStore, logger *slog.Logger) (map[string]model.Platform, error) {
	instances, err := cfg.GitLab.EffectiveInstances()
	if err != nil {
		return nil, fmt.Errorf("gitlab config: %w", err)
	}
	refs := make([]db.GitLabInstanceRef, 0, len(instances))
	for _, in := range instances {
		refs = append(refs, db.GitLabInstanceRef{WebBase: in.WebBase, Primary: in.Primary})
	}
	ids, err := store.SyncGitLabInstances(ctx, refs)
	if err != nil {
		return nil, fmt.Errorf("gitlab instance registry: %w", err)
	}
	for _, in := range instances {
		logger.Info("GitLab instance registered", "web_url", in.WebBase, "platform_id", ids[in.WebBase], "api_url", in.APIURL, "main", in.Primary)
	}
	adopted, err := store.AdoptReposForGitLabInstances(ctx)
	if err != nil {
		return nil, fmt.Errorf("adopt repositories onto GitLab instances: %w", err)
	}
	if adopted.Adopted > 0 || adopted.SkippedConflicts > 0 {
		logger.Info("GitLab instance adoption", "adopted", adopted.Adopted, "skipped_case_conflicts", adopted.SkippedConflicts)
	}
	return ids, nil
}

// configuredGitLabWebBases returns the configured GitLab instances' web URLs
// — the URL parser's hints — so a command that only parses (prioritize,
// recollect, the importers) accepts a self-hosted instance whose hostname
// lacks "gitlab" and strips a sub-path prefix. loadConfig already refused
// an invalid gitlab block, so an error here yields no hints.
func configuredGitLabWebBases(cfg *config.Config) []string {
	instances, err := cfg.GitLab.EffectiveInstances()
	if err != nil {
		return nil
	}
	bases := make([]string, 0, len(instances))
	for _, in := range instances {
		bases = append(bases, in.WebBase)
	}
	return bases
}
