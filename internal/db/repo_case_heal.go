// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// repo_case_heal.go — Phase 0 case self-heal (v0.25.32).
//
// GitHub and GitLab accept case-variant owner/repo lookups but always
// RETURN the canonical spelling (nameWithOwner / full_name /
// path_with_namespace). When a repo entered the catalog with
// non-canonical casing (bulk paste, or a dedup-repos merge whose older
// winner happened to be the wrong-cased row), the staged collector's
// Phase 0 passes the forge-reported FullName here and the stored
// repo_git/repo_owner/repo_name get corrected in place.
//
// Deliberate carve-out from the "prelim owns rename detection" design
// decision: the forge treats case variants as the SAME resource, so a
// case-only correction does not change repo identity — unlike a real
// rename, where mutating identity mid-job risks split-identity data.
// Anything that differs by more than case is therefore logged and left
// for prelim's redirect handling.

package db

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// HealRepoCaseDrift corrects repos.repo_git / repo_owner / repo_name
// when the forge-reported fullName ("owner/name", canonical casing)
// differs from the stored values ONLY by case. Returns healed=true when
// a correction was written. Safe no-op in every other situation.
func (s *PostgresStore) HealRepoCaseDrift(ctx context.Context, repoID int64, fullName string) (healed bool, err error) {
	if fullName == "" {
		return false, nil
	}

	var gitURL, owner, name string
	err = s.pool.QueryRow(ctx,
		`SELECT repo_git, repo_owner, repo_name FROM aveloxis_data.repos WHERE repo_id = $1`,
		repoID).Scan(&gitURL, &owner, &name)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("load repo %d: %w", repoID, err)
	}

	storedPath := owner + "/" + name
	if storedPath == fullName {
		return false, nil // already canonical
	}
	if !strings.EqualFold(storedPath, fullName) {
		// More than case changed — a real rename/transfer. That is
		// prelim's job (redirect detection at job start); mutating repo
		// identity mid-collection risks split-identity data.
		s.logger.Info("repo name differs from forge-canonical by more than case — leaving to prelim rename detection",
			"repo_id", repoID, "stored", storedPath, "canonical", fullName)
		return false, nil
	}

	// Rebuild the URL with the canonical path, preserving scheme + host.
	scheme, host := schemeAndHost(gitURL)
	if host == "" {
		return false, fmt.Errorf("cannot derive host from stored repo_git %q", gitURL)
	}
	newURL := scheme + "://" + host + "/" + fullName
	if newURL == gitURL {
		return false, nil
	}

	// Occupancy guard: if ANOTHER row already holds the canonical URL,
	// this is an unmerged case-variant duplicate pair — correcting this
	// row would collide with it. Exact-match check excluding ourselves
	// (NOT the case-insensitive FindRepoByURL, which would match the
	// very row being healed).
	var occupant int64
	err = s.pool.QueryRow(ctx,
		`SELECT repo_id FROM aveloxis_data.repos WHERE repo_git = $1 AND repo_id <> $2`,
		newURL, repoID).Scan(&occupant)
	if err == nil {
		s.logger.Warn("canonical URL already tracked by another repo — case-variant duplicate pair",
			"repo_id", repoID, "stored", gitURL,
			"canonical_url", newURL, "occupant_repo_id", occupant,
			"hint", "run `aveloxis dedup-repos` to merge the pair")
		return false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return false, fmt.Errorf("occupancy check for %q: %w", newURL, err)
	}

	// Reuse the rename machinery: updates repo_git/repo_owner/repo_name
	// AND rewrites the owner/name path inside stored issue/PR/review/
	// release URLs.
	if err := s.UpdateRepoURLs(ctx, repoID, gitURL, newURL); err != nil {
		return false, fmt.Errorf("update repo URLs %q -> %q: %w", gitURL, newURL, err)
	}
	s.logger.Info("healed repo case drift to forge-canonical spelling",
		"repo_id", repoID, "old", gitURL, "new", newURL)
	return true, nil
}

// schemeAndHost extracts the scheme (defaulting to https) and host from
// a stored repo URL.
func schemeAndHost(gitURL string) (scheme, host string) {
	scheme = "https"
	rest := gitURL
	if s, r, found := strings.Cut(gitURL, "://"); found {
		if s == "http" {
			scheme = "http"
		}
		rest = r
	}
	host, _, _ = strings.Cut(rest, "/")
	return scheme, host
}
