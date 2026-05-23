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
// scan.
//
// v0.25.0 — failure contract loosened. Pre-v0.25.0 the gate
// `len(errsList) > 0 && zero data` failed the scan whenever ANY
// source errored and no other source produced data, conflating two
// distinct situations:
//
//   - "Repo legitimately doesn't publish anywhere we can see"
//     (success, stamp last_run, do not retry for 180 days), and
//   - "Sources we needed had transient failures"
//     (failure, quadratic backoff, retry sooner).
//
// The diagnostic that surfaced this: every chaoss.tv distribution
// failure on 2026-05-22/23 was a Julia or R repo where ecosyste.ms
// 500'd and every other source legitimately had nothing. The
// pre-v0.25.0 contract recorded these as failures, walked the
// backoff to the 10-strike sideline, and locked them out for 6
// months — even though the working sources had returned clean
// "no data" responses.
//
// New contract: fail only when EVERY enabled source actually
// errored AND no evidence was collected. At least one clean
// completion = scan succeeded (the empty-truth answer).
//
// Per-source-class diagnostic logging: external package-registry
// sources (deps.dev + ecosyste.ms) get distinct treatment from
// GitHub sources. When BOTH external sources fail in the same
// scan, the composite emits an ERROR-level log entry with each
// source's error labeled as its own slog key — never aggregated
// or swallowed — so operators can see per-source what failed
// while we build confidence the error classification is right.
type CompositeScanner struct {
	DepsDev    *depsdev.Client
	Ecosystems *ecosystems.Client
	GitHub     *github.Client
	Logger     *slog.Logger

	// CrossCheckSources, when true, ensures BOTH deps.dev and
	// ecosyste.ms are always queried for every repo even if one
	// has already returned non-empty data. Each source persists
	// its own rows into repo_distribution (source column
	// distinguishes them; UNIQUE constraint includes source so
	// both can coexist). Default in production: true. Set false
	// only when an operator wants to halve registry traffic at
	// the cost of single-source-of-truth dependence. v0.25.0.
	CrossCheckSources bool
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
		DepsDev:           depsDev,
		Ecosystems:        eco,
		GitHub:            gh,
		Logger:            logger,
		CrossCheckSources: true, // v0.25.0 default; see field doc
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
func (s *CompositeScanner) Scan(ctx context.Context, repoID int64, owner, repo, repoGit string) ([]model.PackageDistribution, []model.DistributionManifest, bool, error) {
	// Platform gate: deps.dev expects github.com URLs; ecosyste.ms
	// can lookup gitlab.com too but the GitHub Contents API doesn't
	// work for GitLab. Easiest to opt in only for github.com hosts.
	//
	// A non-GitHub repo is a "complete scan with no evidence" —
	// stamp it complete=true so the cadence gate applies normally.
	if !strings.Contains(strings.ToLower(repoGit), "github.com") {
		s.Logger.Debug("distribution scanner: non-GitHub repo, skipping",
			"repo_id", repoID, "repo_git", repoGit)
		return nil, nil, true, nil
	}

	var (
		distributions []model.PackageDistribution
		manifests     []model.DistributionManifest

		// Per-class error tracking (v0.25.0). External-registry
		// errors get distinct treatment from GitHub errors per
		// operator direction: we want explicit per-source visibility
		// when both external sources fail until we have confidence
		// the classification is right.
		depsDevErr     error
		ecosystemsErr  error
		githubErrs     []error // ListReleaseAssetExtensions, ListRepoPackages, ListRootManifests
		enabledSources int     // total number of sources actually attempted
		erroredSources int     // count of sources that returned non-nil err

		// scanIncomplete tracks whether ANY enabled external source
		// (deps.dev or ecosyste.ms) had a transient error or was
		// skipped due to an open circuit breaker. When true, the
		// store stamps distribution_scan_complete=FALSE on this
		// row, which makes the claim query treat it as immediately
		// re-eligible (bypassing the cadence gate). The 10 repos
		// that trip the ecosyste.ms breaker get full re-scans this
		// way; their partial-scan rows rotate to history on the
		// next snapshot replace.
		//
		// GitHub source errors do NOT set scanIncomplete — 403/404/
		// 304 from those endpoints are routinely benign (private
		// repos, missing OAuth scope, archived/empty repos) and
		// shouldn't force a re-scan.
		scanIncomplete bool
	)

	// Source 1: deps.dev (external package registry, primary)
	if s.DepsDev != nil {
		enabledSources++
		dd, err := s.DepsDev.GetPackageVersions(ctx, owner, repo)
		if err != nil {
			erroredSources++
			depsDevErr = err
			class := platform.ClassifyError(err)
			// Transient/rate-limit class from an external registry
			// marks the scan incomplete so the row gets re-collected
			// once the source recovers (v0.25.0).
			if class == platform.ClassTransient || class == platform.ClassRateLimit {
				scanIncomplete = true
			}
			s.Logger.Warn("distribution: deps.dev fetch failed",
				"repo_id", repoID, "owner", owner, "repo", repo,
				"class", class.String(), "error", err)
		} else {
			distributions = append(distributions, dd...)
		}
	}

	// Source 2: ecosyste.ms (external package registry, long-tail).
	//
	// CrossCheckSources gating: when false AND deps.dev returned
	// non-empty data, skip ecosyste.ms. Default true → always
	// query both so each writes its own (source-distinguished)
	// rows. The UNIQUE (repo_id, ecosystem, package_name, source)
	// constraint in repo_distribution makes two-source rows for the
	// same package legal — and useful for cross-checking.
	skipEcosystems := !s.CrossCheckSources && len(distributions) > 0
	if s.Ecosystems != nil && !skipEcosystems {
		enabledSources++
		em, err := s.Ecosystems.LookupPackages(ctx, repoGit)
		if err != nil {
			// v0.25.0: ErrCircuitOpen is a typed sentinel from the
			// ecosystems client indicating the source-level circuit
			// breaker is currently tripped and the call short-
			// circuited without consulting upstream. Treat it as a
			// "source unavailable" skip (NOT an error) — but mark
			// the scan incomplete so the row gets re-collected once
			// the breaker closes. The 10 repos that trip the breaker
			// in the first place ALSO go through this path on their
			// second attempt (the partial-scan rows get rotated to
			// history on the re-collection).
			if errors.Is(err, ecosystems.ErrCircuitOpen) {
				scanIncomplete = true
				s.Logger.Debug("distribution: ecosyste.ms skipped — circuit open, scan marked incomplete",
					"repo_id", repoID, "owner", owner, "repo", repo)
			} else {
				erroredSources++
				ecosystemsErr = err
				class := platform.ClassifyError(err)
				if class == platform.ClassTransient || class == platform.ClassRateLimit {
					scanIncomplete = true
				}
				s.Logger.Warn("distribution: ecosyste.ms fetch failed",
					"repo_id", repoID, "owner", owner, "repo", repo,
					"class", class.String(), "error", err)
			}
		} else {
			distributions = append(distributions, em...)
		}
	}

	// When BOTH external registries failed in the same scan, emit
	// an ERROR-level log line with each source's error labeled as
	// its own slog key (NOT joined / aggregated). This is the
	// operator-mandated diagnostic visibility: per-source errors
	// while we build confidence the classification is right.
	bothExternalAttempted := s.DepsDev != nil && s.Ecosystems != nil && !skipEcosystems
	if bothExternalAttempted && depsDevErr != nil && ecosystemsErr != nil {
		s.Logger.Error("distribution: BOTH external registries failed (per-source detail follows)",
			"repo_id", repoID, "owner", owner, "repo", repo,
			"repo_git", repoGit,
			"deps_dev_class", platform.ClassifyError(depsDevErr).String(),
			"deps_dev_error", depsDevErr.Error(),
			"ecosystems_class", platform.ClassifyError(ecosystemsErr).String(),
			"ecosystems_error", ecosystemsErr.Error())
	}

	// Sources 3, 4, 5: GitHub (release assets, packages, manifests).
	// Errors here keep WARN level — these are platform-of-record
	// signals where 403/404/304 are common (private repos, missing
	// OAuth scope, archived/empty repos) and routinely benign.
	if s.GitHub != nil {
		// Release assets
		enabledSources++
		assets, err := s.GitHub.ListReleaseAssetExtensions(ctx, owner, repo)
		if err != nil {
			erroredSources++
			githubErrs = append(githubErrs, err)
			s.Logger.Warn("distribution: github release assets failed",
				"repo_id", repoID, "owner", owner, "repo", repo, "error", err)
		} else {
			distributions = append(distributions, assets...)
		}

		// GitHub Packages (best-effort)
		enabledSources++
		pkgs, err := s.GitHub.ListRepoPackages(ctx, owner, repo)
		if err != nil {
			erroredSources++
			githubErrs = append(githubErrs, err)
			s.Logger.Warn("distribution: github packages failed",
				"repo_id", repoID, "owner", owner, "repo", repo, "error", err)
		} else {
			distributions = append(distributions, pkgs...)
		}

		// Manifests + declared-name parsing
		enabledSources++
		rawManifests, err := s.GitHub.ListRootManifests(ctx, owner, repo)
		if err != nil {
			erroredSources++
			githubErrs = append(githubErrs, err)
			s.Logger.Warn("distribution: github manifests failed",
				"repo_id", repoID, "owner", owner, "repo", repo, "error", err)
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

	// v0.25.0 contract: fail the scan ONLY when EVERY enabled
	// source actually errored AND no evidence was collected. Empty-
	// but-clean responses from at least one source represent the
	// truthful "this repo doesn't publish anywhere we can see"
	// answer — that's a success, last_run gets stamped, no
	// backoff, no 180-day sideline. The pre-v0.25.0 contract
	// (`len(errsList) > 0 && zero data` = failure) conflated this
	// with "sources we needed had transient failures" and stuck
	// legitimate-no-data repos in an indefinite retry loop until
	// the 10-strike sideline.
	if enabledSources > 0 && erroredSources == enabledSources && len(distributions) == 0 && len(manifests) == 0 {
		allErrs := make([]error, 0, 2+len(githubErrs))
		if depsDevErr != nil {
			allErrs = append(allErrs, depsDevErr)
		}
		if ecosystemsErr != nil {
			allErrs = append(allErrs, ecosystemsErr)
		}
		allErrs = append(allErrs, githubErrs...)
		// Failed scan: complete is moot — the worker routes to
		// RecordDistributionFailure and doesn't touch
		// distribution_scan_complete. Return false defensively.
		return nil, nil, false, errors.Join(allErrs...)
	}

	// Success path. complete = !scanIncomplete. The store stamps
	// distribution_scan_complete with this value; the claim query
	// treats scan_complete=FALSE as immediately re-eligible.
	return distributions, manifests, !scanIncomplete, nil
}

// Healthy reports whether the CompositeScanner's external sources
// are currently in a healthy state. Returns false when ecosyste.ms
// is enabled AND its source-level circuit breaker is currently open
// — the DistributionWorker's dispatcher consults this before each
// claim and pauses while unhealthy, so we don't dispatch new repos
// into a known-bad upstream and end up stamping their cadence with
// partial-scan data.
//
// deps.dev does not (yet) have a source-level circuit breaker; if
// one is added in the future, OR its IsCircuitOpen() in here.
//
// v0.25.0.
func (s *CompositeScanner) Healthy() bool {
	if s.Ecosystems != nil && s.Ecosystems.IsCircuitOpen() {
		return false
	}
	return true
}
