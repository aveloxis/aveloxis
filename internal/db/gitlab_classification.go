// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// ErrCrossInstanceRename refuses a URL update that would move a GitLab
// repository to another GitLab instance (or off GitLab): its identities,
// messages and platform_id belong to the instance it was collected from.
var ErrCrossInstanceRename = errors.New("URL change would move the repository to another GitLab instance")

// classifyRepoPlatform sets r.Platform from the GitLab instance registry
// (v0.30.0) — the one door every catalog write goes through (SR-18):
//
//   - GitHub is left alone.
//   - A URL under a registered instance's web base gets that instance's
//     platform_id, whatever the caller passed (a web paste parses a
//     self-hosted host without "gitlab" in its name as generic git).
//   - A GitLab-family id whose URL is under no registered instance becomes
//     generic git (3): no API identity namespace exists for that host, and
//     filing it under another instance would collect it with that
//     instance's keys. Before the first registry sync (platform 2 has no
//     web base yet) platform 2 is kept, as before v0.30.0.
//
// A registry read error is returned (SR-5), never treated as "no instance".
func (s *PostgresStore) classifyRepoPlatform(ctx context.Context, r *model.Repo) error {
	if r.Platform == model.PlatformGitHub {
		return nil
	}
	registry, err := s.LoadGitLabInstanceRegistry(ctx)
	if registryNotMigrated(err) {
		// A process on this binary before `aveloxis migrate` added the
		// registry column (e.g. web started first during a deploy): keep the
		// caller's platform, as before v0.30.0, and say so.
		s.logger.Warn("GitLab instance registry not migrated yet — keeping the requested platform_id; run `aveloxis migrate`",
			"repo_git", r.GitURL, "platform_id", r.Platform)
		return nil
	}
	if err != nil {
		return fmt.Errorf("classify %s: %w", r.GitURL, err)
	}
	if base, rest, ok := model.MatchInstanceWebBase(r.GitURL, registryBases(registry)); ok {
		// Owner/name come from the path AFTER the instance's web base: a
		// caller that parsed without the registry (web paste, imports,
		// prioritize) would otherwise store a sub-path prefix in
		// repo_owner, and every API call is built from owner/name. A URL
		// with no owner/repo after the base names no project on the
		// instance (a group page, the prefix plus one segment): refused.
		parts := strings.Split(rest, "/")
		if rest == "" || len(parts) < 2 {
			return fmt.Errorf("UpsertRepo: %s is on GitLab instance %s but has no owner/repo after it", r.GitURL, base)
		}
		r.Platform = registry[base]
		r.Owner = strings.Join(parts[:len(parts)-1], "/")
		r.Name = model.NormalizeRepoName(parts[len(parts)-1])
		return nil
	}
	if !r.Platform.IsGitLab() {
		return nil
	}
	if r.Platform == model.PlatformGitLab && historicalWebBase(registry) == "" {
		s.logger.Warn("GitLab instance registry not synced yet — keeping platform_id 2; run `aveloxis migrate`",
			"repo_git", r.GitURL)
		return nil
	}
	s.logger.Warn("repository is on a GitLab host that is not a configured instance — recorded git-only; add the instance to gitlab.instances (web_url, api_url, api_keys) and run `aveloxis migrate` to collect it over the API",
		"repo_git", r.GitURL, "requested_platform_id", r.Platform)
	r.Platform = model.PlatformGenericGit
	return nil
}

// registryNotMigrated reports a registry read that failed only because the
// v0.30.0 column does not exist yet (42703 undefined_column) — a definitive
// schema state, not a transient failure (SR-5 still applies to every other
// error).
func registryNotMigrated(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42703"
}

func registryBases(registry map[string]model.Platform) []string {
	bases := make([]string, 0, len(registry))
	for b := range registry {
		bases = append(bases, b)
	}
	sort.Strings(bases)
	return bases
}

// historicalWebBase returns platform 2's registered web base, or "" before
// the first registry sync.
func historicalWebBase(registry map[string]model.Platform) string {
	for base, id := range registry {
		if id == model.PlatformGitLab {
			return base
		}
	}
	return ""
}

// repoURLOwnerName is parseRepoURLOwnerName with the GitLab instance
// registry's web bases as hints, so a sub-path instance's prefix never lands
// in repo_owner on a URL update. A URL on a registered instance that the
// hinted parser refuses (no owner/repo after the web base, a non-http(s)
// scheme) is refused too, never re-parsed without hints. A registry read
// error is returned (SR-5).
func (s *PostgresStore) repoURLOwnerName(ctx context.Context, newURL string) (owner, name string, err error) {
	registry, err := s.LoadGitLabInstanceRegistry(ctx)
	if registryNotMigrated(err) {
		owner, name = parseRepoURLOwnerName(newURL)
		return owner, name, nil
	}
	if err != nil {
		return "", "", fmt.Errorf("owner/name for %s: %w", newURL, err)
	}
	bases := registryBases(registry)
	ru, perr := platform.ParseRepoURLWithHints(newURL, bases)
	if perr == nil {
		return ru.Owner, model.NormalizeRepoName(ru.Repo), nil
	}
	if _, _, onInstance := model.MatchInstanceWebBase(newURL, bases); onInstance {
		// The parser's own refusal (no owner/repo after the instance's web
		// base) stands — never re-parsed without hints into owner = the
		// prefix.
		return "", "", fmt.Errorf("owner/name for %s: %w", newURL, perr)
	}
	owner, name = parseRepoURLOwnerName(newURL)
	return owner, name, nil
}

// checkCrossInstanceRename refuses newURL for a GitLab repository when it is
// not under the web base registered for the repository's platform_id. A
// repository on a platform_id without a registered web base (platform 2
// before the first sync) is not checked.
func (s *PostgresStore) checkCrossInstanceRename(ctx context.Context, repoID int64, newURL string) error {
	var plat int16
	if err := s.pool.QueryRow(ctx, `SELECT platform_id FROM aveloxis_data.repos WHERE repo_id = $1`, repoID).Scan(&plat); err != nil {
		return fmt.Errorf("rename check for repo %d: %w", repoID, err)
	}
	p := model.Platform(plat)
	if !p.IsGitLab() {
		return nil
	}
	registry, err := s.LoadGitLabInstanceRegistry(ctx)
	if registryNotMigrated(err) {
		return nil // no instance is registered yet: nothing to cross
	}
	if err != nil {
		return fmt.Errorf("rename check for repo %d: %w", repoID, err)
	}
	own := ""
	for base, id := range registry {
		if id == p {
			own = base
		}
	}
	if own == "" {
		return nil
	}
	if base, _, ok := model.MatchInstanceWebBase(newURL, registryBases(registry)); ok && base == own {
		return nil
	}
	return fmt.Errorf("%w: repo %d (platform_id %d, %s) → %s", ErrCrossInstanceRename, repoID, p, own, newURL)
}

// AdoptResult counts one adoption pass.
type AdoptResult struct {
	Adopted          int // moved onto their instance, force_full_collect set
	SkippedConflicts int // a case variant already tracked on the instance
}

// AdoptReposForGitLabInstances moves repositories onto the GitLab instance
// they live under once that instance is registered (v0.30.0). It runs after
// every registry sync (serve startup, `aveloxis migrate`):
//
//   - generic-git rows (3) under a registered web base, and
//   - platform-2 rows under ANOTHER registered instance's web base that hold
//     no API data (no issues, pull requests, messages or repo_info)
//
// get the instance's platform_id and force_full_collect (git-only cycles
// already advanced last_collected past history the API never listed).
// Platform-2 rows under another instance that DO hold API data are left
// alone — that data came from the historical instance's API and may belong
// to a same-path project there (SR-6/SR-7); MisroutedGitLabRepos reports
// them. A move that collides with a case variant already tracked on the
// instance is skipped with a WARN pointing at dedup-repos. Rerunning is safe:
// moved rows no longer match.
func (s *PostgresStore) AdoptReposForGitLabInstances(ctx context.Context) (AdoptResult, error) {
	var res AdoptResult
	registry, err := s.LoadGitLabInstanceRegistry(ctx)
	if err != nil {
		return res, fmt.Errorf("adopt: %w", err)
	}
	if len(registry) == 0 {
		return res, nil
	}
	bases := registryBases(registry)
	row2 := historicalWebBase(registry)

	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.platform_id, r.repo_git,
		       (r.platform_id = $1 AND `+repoHasAPIDataSQL+`) AS has_api_data
		FROM aveloxis_data.repos r
		WHERE r.platform_id = ANY($2)
		ORDER BY r.repo_id`, int16(model.PlatformGitLab), []int16{int16(model.PlatformGitLab), int16(model.PlatformGenericGit)})
	if err != nil {
		return res, fmt.Errorf("adopt: candidates: %w", err)
	}
	type move struct {
		repoID int64
		from   model.Platform
		to     model.Platform
		url    string
	}
	var moves []move
	for rows.Next() {
		var id int64
		var plat int16
		var url string
		var hasData bool
		if err := rows.Scan(&id, &plat, &url, &hasData); err != nil {
			rows.Close()
			return res, fmt.Errorf("adopt: candidates: %w", err)
		}
		base, _, ok := model.MatchInstanceWebBase(url, bases)
		if !ok {
			continue
		}
		to := registry[base]
		from := model.Platform(plat)
		switch {
		case to == from:
			continue
		case from == model.PlatformGitLab && (row2 == "" || base == row2 || hasData):
			continue // unsynced, on its own instance, or reported (MisroutedGitLabRepos)
		}
		moves = append(moves, move{id, from, to, url})
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return res, fmt.Errorf("adopt: candidates: %w", err)
	}

	for _, m := range moves {
		err := s.withRetry(ctx, func(ctx context.Context) error {
			tx, err := s.pool.Begin(ctx)
			if err != nil {
				return err
			}
			defer func() { _ = tx.Rollback(ctx) }()
			tag, err := tx.Exec(ctx, `UPDATE aveloxis_data.repos SET platform_id = $1 WHERE repo_id = $2 AND platform_id = $3`,
				int16(m.to), m.repoID, int16(m.from))
			if err != nil {
				return err
			}
			if tag.RowsAffected() == 0 {
				return tx.Commit(ctx) // moved by a concurrent pass
			}
			if _, err := tx.Exec(ctx, `UPDATE aveloxis_ops.collection_queue SET force_full_collect = TRUE WHERE repo_id = $1`, m.repoID); err != nil {
				return err
			}
			return tx.Commit(ctx)
		})
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && isRepoGitCIUniqueViolation(pgErr) {
			res.SkippedConflicts++
			s.logger.Warn("adopt: a case variant of this repository is already tracked on its GitLab instance — not moved; merge them with `aveloxis dedup-repos`",
				"repo_id", m.repoID, "repo_git", m.url, "platform_id", m.to)
			continue
		}
		if err != nil {
			return res, fmt.Errorf("adopt repo %d onto platform_id %d: %w", m.repoID, m.to, err)
		}
		res.Adopted++
		s.logger.Info("adopted repository onto its GitLab instance — full collection next cycle",
			"repo_id", m.repoID, "repo_git", m.url, "from_platform_id", m.from, "platform_id", m.to)
	}
	return res, nil
}

// repoHasAPIDataSQL is "repository r already holds API-collected data" — the
// one spelling adoption and the misrouted report share (SR-17).
const repoHasAPIDataSQL = `(
	EXISTS (SELECT 1 FROM aveloxis_data.issues i WHERE i.repo_id = r.repo_id)
	OR EXISTS (SELECT 1 FROM aveloxis_data.pull_requests pr WHERE pr.repo_id = r.repo_id)
	OR EXISTS (SELECT 1 FROM aveloxis_data.messages m WHERE m.repo_id = r.repo_id)
	OR EXISTS (SELECT 1 FROM aveloxis_data.repo_info ri WHERE ri.repo_id = r.repo_id))`

// MisroutedRepo is a platform-2 repository under another registered GitLab
// instance's web base that already holds API data.
type MisroutedRepo struct {
	RepoID   int64
	GitURL   string
	Instance string // the web base it lives under
}

// MisroutedGitLabRepos reports platform-2 repositories that live under a
// registered GitLab instance other than platform 2's and already hold API
// data (issues, pull requests, messages or repo_info). That data was
// collected from the historical instance's API; consolidating it is an
// operator decision, so nothing here mutates it.
func (s *PostgresStore) MisroutedGitLabRepos(ctx context.Context) ([]MisroutedRepo, error) {
	registry, err := s.LoadGitLabInstanceRegistry(ctx)
	if err != nil {
		return nil, fmt.Errorf("misrouted gitlab repos: %w", err)
	}
	row2 := historicalWebBase(registry)
	if row2 == "" || len(registry) < 2 {
		return nil, nil
	}
	rows, err := s.pool.Query(ctx, `
		SELECT r.repo_id, r.repo_git FROM aveloxis_data.repos r
		WHERE r.platform_id = $1 AND `+repoHasAPIDataSQL+`
		ORDER BY r.repo_id`, int16(model.PlatformGitLab))
	if err != nil {
		return nil, fmt.Errorf("misrouted gitlab repos: %w", err)
	}
	defer rows.Close()
	bases := registryBases(registry)
	var out []MisroutedRepo
	for rows.Next() {
		var m MisroutedRepo
		if err := rows.Scan(&m.RepoID, &m.GitURL); err != nil {
			return nil, fmt.Errorf("misrouted gitlab repos: %w", err)
		}
		if base, _, ok := model.MatchInstanceWebBase(m.GitURL, bases); ok && base != row2 {
			m.Instance = base
			out = append(out, m)
		}
	}
	return out, rows.Err()
}

// CountReposByPlatform returns the number of repositories per platform_id.
func (s *PostgresStore) CountReposByPlatform(ctx context.Context) (map[model.Platform]int, error) {
	rows, err := s.pool.Query(ctx, `SELECT platform_id, count(*) FROM aveloxis_data.repos GROUP BY platform_id`)
	if err != nil {
		return nil, fmt.Errorf("count repos by platform: %w", err)
	}
	defer rows.Close()
	out := map[model.Platform]int{}
	for rows.Next() {
		var p int16
		var n int
		if err := rows.Scan(&p, &n); err != nil {
			return nil, fmt.Errorf("count repos by platform: %w", err)
		}
		out[model.Platform(p)] = n
	}
	return out, rows.Err()
}
