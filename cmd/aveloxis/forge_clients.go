// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/github"
	"github.com/aveloxis/aveloxis/internal/platform/gitlab"
)

// forgeClients is what a collecting command needs to reach the forges: the
// GitHub client and key pool, and the per-instance GitLab router.
type forgeClients struct {
	gh     *github.Client
	ghKeys *platform.KeyPool
	gl     *gitlab.Instances
}

// buildForgeClients is the one place forge API clients and key pools are
// built for collecting commands (serve, collect, add-repo,
// backfill-repo-metadata, heal-collection-gaps). v0.30.0 (multi-instance
// GitLab): each configured GitLab instance gets its OWN key pool — from its
// config entry and the stored keys tagged with its web URL
// (partitionGitLabTokens) — and its own client on its own API URL. register
// syncs the platforms registry first (serve and migrate only); every other
// caller reads it, and a configured instance that is not registered yet is
// left out with a WARN (its repositories take the not-configured path).
func buildForgeClients(ctx context.Context, cfg *config.Config, store *db.PostgresStore, useAugurKeys, register bool, logger *slog.Logger) (*forgeClients, error) {
	instances, err := cfg.GitLab.EffectiveInstances()
	if err != nil {
		return nil, fmt.Errorf("gitlab config: %w", err)
	}

	ghKeys, ghCount := loadGitHubKeyPool(ctx, cfg, store, useAugurKeys, logger)

	stored, err := db.LoadAPIKeysByInstance(ctx, store.Pool(), "gitlab", useAugurKeys)
	if err != nil {
		// Same degradation as the GitHub loader: config keys still load,
		// the failure is an ERROR, never a silent empty set.
		logger.Error("failed to load GitLab API keys from database — only config keys are used", "error", err)
		stored = nil
	}
	pools, orphans, err := partitionGitLabTokens(instances, stored)
	if err != nil {
		return nil, err
	}
	for tag, n := range orphans {
		logger.Warn("stored GitLab keys name an instance that is not configured — not loaded",
			"instance_url", tag, "keys", n, "fix", "add the instance to gitlab.instances, or re-add the keys with add-key --instance")
	}

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
	var list []*gitlab.Instance
	for _, in := range instances {
		id, ok := ids[in.WebBase]
		if !ok {
			logger.Warn("configured GitLab instance is not registered yet — run `aveloxis migrate`; its repositories are not collected over the API until then",
				"web_url", in.WebBase, "api_url", in.APIURL)
			continue
		}
		entry := &gitlab.Instance{ID: id, WebBase: in.WebBase, APIURL: in.APIURL, Primary: in.Primary}
		toks := pools[in.WebBase]
		if len(toks) > 0 {
			// The pool and the API URL come from the same instance (in):
			// TestKeyedClientBaseURLAllowlist accepts exactly this shape.
			pool := platform.NewKeyPool(pools[in.WebBase], logger)
			pool.SetAdmission(maxInflight, glPerKey, reservePct)
			client, err := gitlab.New(id, in.WebBase, in.APIURL, pool, logger)
			if err != nil {
				return nil, err
			}
			entry.Client = client
			glCount += len(toks)
		}
		list = append(list, entry)
		logger.Info("GitLab instance",
			"platform_id", id, "web_url", in.WebBase, "api_url", in.APIURL, "keys", len(toks),
			"main", in.Primary, "max_inflight", maxInflight, "max_inflight_per_key", glPerKey,
			"foreground_reserve_pct", reservePct)
		if len(toks) == 0 {
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
		gh:     github.New(cfg.GitHub.BaseURL, ghKeys, logger),
		ghKeys: ghKeys,
		gl:     router,
	}, nil
}

// partitionGitLabTokens assigns every GitLab token to exactly one instance.
// An instance's pool is its config keys, then the stored keys tagged with
// its web base (tags compare after model.NormalizeInstanceWebBase), and for
// the main instance also the untagged stored keys (""). A tag no instance
// has is an orphan and is never loaded. A token found under two instances
// is an error naming both: loading it into either pool would send one
// instance's credential to the other. A token repeated within one instance
// is kept once. Every configured instance has an entry, empty when it has
// no keys.
func partitionGitLabTokens(instances []config.GitLabInstance, stored map[string][]string) (pools map[string][]string, orphans map[string]int, err error) {
	pools = make(map[string][]string, len(instances))
	orphans = map[string]int{}
	owner := map[string]string{} // token → web base
	add := func(base, tok string) error {
		if tok == "" {
			return nil
		}
		if prev, ok := owner[tok]; ok {
			if prev != base {
				return fmt.Errorf("a GitLab API key is configured for two instances (%s and %s) — a key belongs to the one instance that issued it; remove it from one of them", prev, base)
			}
			return nil
		}
		owner[tok] = base
		pools[base] = append(pools[base], tok)
		return nil
	}

	for _, in := range instances {
		pools[in.WebBase] = nil
		for _, tok := range in.APIKeys {
			if err := add(in.WebBase, tok); err != nil {
				return nil, nil, err
			}
		}
	}

	tags := make([]string, 0, len(stored))
	for tag := range stored {
		tags = append(tags, tag)
	}
	sort.Strings(tags) // deterministic pool order and error text
	for _, tag := range tags {
		base, ok := instanceForKeyTag(instances, tag)
		if !ok {
			orphans[tag] += len(stored[tag])
			continue
		}
		for _, tok := range stored[tag] {
			if err := add(base, tok); err != nil {
				return nil, nil, err
			}
		}
	}
	return pools, orphans, nil
}

// loadGitHubKeyPool builds the GitHub key pool: config keys plus stored keys
// (aveloxis_ops first, Augur's with useAugurKeys), with the 2026-09-12
// admission settings. The effective admission values are logged at the point
// of use (SR-10). It returns the pool (possibly empty) and its size.
func loadGitHubKeyPool(ctx context.Context, cfg *config.Config, store *db.PostgresStore, useAugurKeys bool, logger *slog.Logger) (*platform.KeyPool, int) {
	tokens := append([]string(nil), cfg.GitHub.APIKeys...)
	if dbGH, err := db.LoadAPIKeys(ctx, store.Pool(), "github", useAugurKeys); err != nil {
		logger.Error("failed to load GitHub API keys from database", "error", err)
	} else if len(dbGH) > 0 {
		logger.Info("loaded GitHub keys from database", "count", len(dbGH))
		tokens = append(tokens, dbGH...)
	}
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
	pool, n := loadGitHubKeyPool(ctx, cfg, store, useAugurKeys, logger)
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

// instanceForKeyTag is the ONE mapping from a stored key's instance tag to the
// configured instance it loads into (SR-17; partitionGitLabTokens and
// add-key's move check both use it): "" is the main instance; any other tag
// is normalized and compared scheme-less, so a key stored for http://host
// belongs to the instance now configured as https://host. ok is false for a
// tag no configured instance has.
func instanceForKeyTag(instances []config.GitLabInstance, tag string) (webBase string, ok bool) {
	if tag == "" {
		for _, in := range instances {
			if in.Primary {
				return in.WebBase, true
			}
		}
		return "", false
	}
	nb, err := model.NormalizeInstanceWebBase(tag)
	if err != nil {
		return "", false
	}
	for _, in := range instances {
		if model.SchemelessWebBase(in.WebBase) == model.SchemelessWebBase(nb) {
			return in.WebBase, true
		}
	}
	return "", false
}
