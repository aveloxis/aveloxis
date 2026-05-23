// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package distribution

import (
	"context"
	"errors"
	"log/slog"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
	"github.com/aveloxis/aveloxis/internal/platform/depsdev"
	"github.com/aveloxis/aveloxis/internal/platform/ecosystems"
	"github.com/aveloxis/aveloxis/internal/platform/github"
)

// CompositeScanner composes the five v0.24.0 distribution sources
// into a single Scanner. Each source runs sequentially; per-source
// failures are aggregated best-effort rather than aborting the whole
// scan. The scan as a whole only fails when EVERY source failed
// AND zero evidence was collected — that's the signal worth a
// RecordDistributionFailure.
type CompositeScanner struct {
	DepsDev    *depsdev.Client
	Ecosystems *ecosystems.Client
	GitHub     *github.Client
	Logger     *slog.Logger
}

// NewCompositeScanner constructs a Scanner with the standard
// production fetchers wired in.
func NewCompositeScanner(
	depsDev *depsdev.Client,
	eco *ecosystems.Client,
	gh *github.Client,
	logger *slog.Logger,
) *CompositeScanner {
	if logger == nil {
		logger = slog.Default()
	}
	return &CompositeScanner{
		DepsDev:    depsDev,
		Ecosystems: eco,
		GitHub:     gh,
		Logger:     logger,
	}
}

// Scan runs all five fetchers and returns the combined evidence.
//
//   - deps.dev: registry reverse-lookup for the 7 major registries
//   - ecosyste.ms: long-tail registries (Conda, Homebrew, CRAN, ...)
//   - github_release_asset: catalog asset filename extensions
//   - github_packages: GitHub's own registry (best-effort, needs scope)
//   - github_contents: walk root + first-level dirs for well-known
//     manifests; parse declared name from each
//
// GitHub-only for v1: GitLab repos return empty evidence + nil
// error (the operator's design decision recorded in CLAUDE.md).
// Future work could add a GitLab path here without changing the
// outer Worker contract.
func (s *CompositeScanner) Scan(ctx context.Context, repoID int64, owner, repo, repoGit string) ([]model.PackageDistribution, []model.DistributionManifest, error) {
	// Platform gate: deps.dev expects github.com URLs; ecosyste.ms
	// can lookup gitlab.com too but the GitHub Contents API doesn't
	// work for GitLab. Easiest to opt in only for github.com hosts.
	if !strings.Contains(strings.ToLower(repoGit), "github.com") {
		s.Logger.Debug("distribution scanner: non-GitHub repo, skipping",
			"repo_id", repoID, "repo_git", repoGit)
		return nil, nil, nil
	}

	var (
		distributions []model.PackageDistribution
		manifests     []model.DistributionManifest
		errsList      []error
	)

	// Source 1: deps.dev
	if s.DepsDev != nil {
		dd, err := s.DepsDev.GetPackageVersions(ctx, owner, repo)
		if err != nil {
			class := platform.ClassifyError(err)
			s.Logger.Warn("distribution: deps.dev fetch failed",
				"repo_id", repoID, "class", class.String(), "error", err)
			errsList = append(errsList, err)
		} else {
			distributions = append(distributions, dd...)
		}
	}

	// Source 2: ecosyste.ms
	if s.Ecosystems != nil {
		em, err := s.Ecosystems.LookupPackages(ctx, repoGit)
		if err != nil {
			s.Logger.Warn("distribution: ecosyste.ms fetch failed",
				"repo_id", repoID, "class", platform.ClassifyError(err).String(), "error", err)
			errsList = append(errsList, err)
		} else {
			distributions = append(distributions, em...)
		}
	}

	// Sources 3, 4, 5: GitHub
	if s.GitHub != nil {
		// Release assets
		assets, err := s.GitHub.ListReleaseAssetExtensions(ctx, owner, repo)
		if err != nil {
			s.Logger.Warn("distribution: github release assets failed",
				"repo_id", repoID, "error", err)
			errsList = append(errsList, err)
		} else {
			distributions = append(distributions, assets...)
		}

		// GitHub Packages (best-effort)
		pkgs, err := s.GitHub.ListRepoPackages(ctx, owner, repo)
		if err != nil {
			s.Logger.Warn("distribution: github packages failed",
				"repo_id", repoID, "error", err)
			errsList = append(errsList, err)
		} else {
			distributions = append(distributions, pkgs...)
		}

		// Manifests + declared-name parsing
		rawManifests, err := s.GitHub.ListRootManifests(ctx, owner, repo)
		if err != nil {
			s.Logger.Warn("distribution: github manifests failed",
				"repo_id", repoID, "error", err)
			errsList = append(errsList, err)
		} else {
			for _, m := range rawManifests {
				// Best-effort: fetch content and parse declared
				// name. Failure to fetch any one manifest does NOT
				// drop the row — manifest_type alone is still
				// useful intent signal.
				content, fetchErr := s.GitHub.FetchManifestContent(ctx, owner, repo, m.ManifestPath)
				if fetchErr != nil {
					s.Logger.Debug("distribution: manifest content fetch failed",
						"repo_id", repoID, "path", m.ManifestPath, "error", fetchErr)
				} else if content != "" {
					m.PackageNameDeclared = ParseManifestName(m.ManifestPath, content)
				}
				manifests = append(manifests, m)
			}
		}
	}

	// Decide overall success/failure.
	//
	// Contract: if EVERY source failed AND no evidence was
	// collected, the scan failed (caller routes to
	// RecordDistributionFailure). Otherwise — even partial
	// evidence — the scan succeeded.
	//
	// This matches v0.20.8 Fix C / v0.20.9 partial-results
	// philosophy: collect what we can; only count it as failure
	// when nothing succeeded.
	if len(errsList) > 0 && len(distributions) == 0 && len(manifests) == 0 {
		return nil, nil, errors.Join(errsList...)
	}

	return distributions, manifests, nil
}
