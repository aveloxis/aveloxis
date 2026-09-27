// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"

	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// v0.24.0 — GitHub-side fetchers for the DistributionWorker.
//
// ListReleaseAssetExtensions, ListRepoPackages, and ListRootManifests
// each return a slice (or empty slice) and handle 404 / 403 / 410 (and a
// 409, ClassSkip since v0.29.68) as "no signal here" rather than failures, mirroring the existing
// platform.isOptionalEndpointSkip contract used elsewhere in the
// collector. Repos with disabled features, private visibility, or
// missing OAuth scopes produce empty results — not errors.

// extensionToEcosystem maps a release-asset filename extension (or
// compound extension) to a normalized ecosystem identifier. Returns
// "" when the extension carries no distribution signal (e.g.
// generic source archives like .tar.gz that don't tell us anything
// about packaging intent).
func extensionToEcosystem(filename string) string {
	lower := strings.ToLower(filename)
	switch {
	case strings.HasSuffix(lower, ".whl"):
		return "pypi"
	case strings.HasSuffix(lower, ".jar"),
		strings.HasSuffix(lower, ".war"),
		strings.HasSuffix(lower, ".ear"):
		return "maven"
	case strings.HasSuffix(lower, ".gem"):
		return "rubygems"
	case strings.HasSuffix(lower, ".nupkg"):
		return "nuget"
	case strings.HasSuffix(lower, ".crate"):
		return "cargo"
	case strings.HasSuffix(lower, ".tgz") && strings.Contains(lower, "-"):
		// npm publishes .tgz tarballs named "<pkg>-<version>.tgz".
		// Heuristic: the dash distinguishes them from raw source
		// archives that are also commonly .tgz. Conservative; a few
		// false positives are acceptable since the registry-side
		// deps.dev signal is authoritative for npm.
		return "npm"
	case strings.HasSuffix(lower, ".deb"):
		return "deb"
	case strings.HasSuffix(lower, ".rpm"):
		return "rpm"
	case strings.HasSuffix(lower, ".apk"):
		return "apk"
	}
	return ""
}

// ghReleaseAsset is the subset of fields we need from a release
// asset for distribution-evidence purposes. Lives here (not in
// types.go) so the distribution-tracking subsystem is largely
// self-contained — the asset list field is added to the existing
// ghRelease type rather than introducing a second type.
type ghReleaseAsset struct {
	Name string `json:"name"`
}

// ListReleaseAssetExtensions enumerates the file extensions of every
// GitHub release asset and groups them by inferred ecosystem. A
// repo that ships .whl files in its releases is treated as having
// pypi distribution evidence with source="github_release_asset",
// even if PyPI itself doesn't carry the package.
//
// Optional endpoint: 404 / 403 / 410 → empty result, not error.
//
// v0.25.0: routes through platform.WithoutETag(ctx) to suppress the
// HTTPClient's If-None-Match conditional layer. A 304 response from
// the distribution path would otherwise silently delete prior rows
// via the v0.24.0 snapshot-replace semantics in
// MarkDistributionComplete — see WithoutETag's docstring for the
// silent-data-loss rationale.
func (c *Client) ListReleaseAssetExtensions(ctx context.Context, owner, repo string) ([]model.PackageDistribution, error) {
	ctx = platform.WithoutETag(ctx)
	path := fmt.Sprintf("/repos/%s/%s/releases?per_page=100", owner, repo)
	var releases []ghRelease
	if err := c.http.GetJSON(ctx, path, &releases); err != nil {
		// A 304 is never solicited here (GetJSON is ETag-free); one that
		// arrives anyway is a non-answer, returned (round 8 of items 22–25).
		if emptyAnswer(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list releases for %s/%s: %w", owner, repo, err)
	}

	// Aggregate: one row per (ecosystem) with version_count = number
	// of releases containing at least one matching asset.
	ecoCount := map[string]int{}
	for _, r := range releases {
		seenInThisRelease := map[string]bool{}
		for _, a := range r.Assets {
			eco := extensionToEcosystem(a.Name)
			if eco == "" || seenInThisRelease[eco] {
				continue
			}
			ecoCount[eco]++
			seenInThisRelease[eco] = true
		}
	}

	out := make([]model.PackageDistribution, 0, len(ecoCount))
	for eco, count := range ecoCount {
		out = append(out, model.PackageDistribution{
			Ecosystem:    eco,
			PackageName:  repo, // best we can do without parsing asset filenames
			VersionCount: count,
			Source:       "github_release_asset",
		})
	}
	return out, nil
}

// ghPackage is the subset of GitHub Packages API fields we need.
type ghPackage struct {
	Name         string           `json:"name"`
	PackageType  string           `json:"package_type"`
	Repository   ghPackageRepoRef `json:"repository"`
	VersionCount int              `json:"version_count"`
}

type ghPackageRepoRef struct {
	Name     string `json:"name"`
	FullName string `json:"full_name"`
}

// supportedPackageTypes lists the package_type values we iterate when
// enumerating an owner's GitHub Packages. Per GitHub docs, valid values
// are: container, docker, maven, npm, nuget, rubygems.
var supportedPackageTypes = []string{
	"container", "docker", "maven", "npm", "nuget", "rubygems",
}

// gitHubPackageTypeToEcosystem maps GitHub's package_type to the
// normalized ecosystem identifier used in repo_distribution.
func gitHubPackageTypeToEcosystem(pt string) string {
	switch pt {
	case "container":
		return "container"
	case "docker":
		return "docker"
	case "maven":
		return "maven"
	case "npm":
		return "npm"
	case "nuget":
		return "nuget"
	case "rubygems":
		return "rubygems"
	}
	return strings.ToLower(pt)
}

// ListRepoPackages enumerates GitHub Packages associated with the
// given repo. Best-effort signal:
//
//   - Tries /users/{owner}/packages first; on 404 falls back to
//     /orgs/{owner}/packages (handles user vs. organization without
//     a separate type-detection call).
//   - Returns empty + nil on 403 (token lacks read:packages scope).
//   - Filters response by repository.name == repo so only this
//     repo's packages show up.
//
// Cost: 1–2 calls × 6 package_types = up to 12 calls per repo. Cheap
// at our cadence (180 days). The filter happens client-side because
// GitHub does not support a repository-name query parameter on these
// endpoints.
func (c *Client) ListRepoPackages(ctx context.Context, owner, repo string) ([]model.PackageDistribution, error) {
	// v0.25.0: bypass ETag so no 304 is solicited (a solicited one read
	// as "empty" was the silent-data-loss path under snapshot-replace; an
	// unsolicited one is a non-answer since round 8). See WithoutETag.
	ctx = platform.WithoutETag(ctx)
	var out []model.PackageDistribution

	for _, pt := range supportedPackageTypes {
		// Try user endpoint first.
		userPath := fmt.Sprintf("/users/%s/packages?package_type=%s&per_page=%d",
			url.PathEscape(owner), url.QueryEscape(pt), packagesPageSize)
		var pkgs []ghPackage
		err := c.http.GetJSON(ctx, userPath, &pkgs)
		if err != nil {
			// ClassNotModified treated like ClassSkip (defense in depth).
			if emptyAnswer(err) {
				// 404 on user endpoint: try org endpoint.
				if errors.Is(err, platform.ErrNotFound) {
					orgPath := fmt.Sprintf("/orgs/%s/packages?package_type=%s&per_page=%d",
						url.PathEscape(owner), url.QueryEscape(pt), packagesPageSize)
					if err := c.http.GetJSON(ctx, orgPath, &pkgs); err != nil {
						if emptyAnswer(err) {
							continue
						}
						return nil, fmt.Errorf("list org packages for %s (type=%s): %w", owner, pt, err)
					}
				} else {
					// 403/410 — scope or visibility: an empty answer. Skip.
					continue
				}
			} else {
				return nil, fmt.Errorf("list user packages for %s (type=%s): %w", owner, pt, err)
			}
		}

		if len(pkgs) >= packagesPageSize {
			// One page is read by decision (worklist item 24; review round 6
			// of items 22–25): a large owner's registry is not walked once
			// per repository. A full page says the listing may be incomplete
			// for THIS owner and type — logged, never silent.
			c.logger.Warn("distribution: packages listing may be incomplete — one page read",
				"owner", owner, "repo", repo, "package_type", pt, "page_size", packagesPageSize)
		}
		for _, p := range pkgs {
			if p.Repository.Name != repo {
				continue
			}
			out = append(out, model.PackageDistribution{
				Ecosystem:    gitHubPackageTypeToEcosystem(p.PackageType),
				PackageName:  p.Name,
				VersionCount: p.VersionCount,
				Source:       "github_packages",
			})
		}
	}

	return out, nil
}

// ghContentsEntry mirrors a row in the GitHub Contents API directory
// listing.
type ghContentsEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
	Type string `json:"type"` // "file" | "dir" | "symlink"
}

// wellKnownManifests maps the basename (or extension) of a manifest
// file to its normalized manifest_type. Used by ListRootManifests
// to identify which files in a contents listing are package manifests.
//
// v0.25.0 added Julia (Project.toml, JuliaProject.toml), R/CRAN
// (DESCRIPTION), and conda (meta.yaml) recognition after a
// 2026-05-23 production diagnostic showed every distribution-scan
// failure on the chaoss.tv aveloxis fleet was a Julia or R repo —
// repos whose manifests existed in the GitHub Contents listing but
// were dropped on the floor because classifyManifestFilename
// returned "" for them. Pre-v0.25.0 these ecosystems had no working
// source whenever ecosyste.ms had an outage (deps.dev doesn't index
// Julia or CRAN/Bioconductor).
var wellKnownManifests = map[string]string{
	"package.json":      "npm",
	"setup.py":          "pypi",
	"setup.cfg":         "pypi",
	"pyproject.toml":    "pypi",
	"cargo.toml":        "cargo",
	"go.mod":            "go",
	"pom.xml":           "maven",
	"build.gradle":      "maven",
	"build.gradle.kts":  "maven",
	"gemfile":           "rubygems",
	"composer.json":     "composer",
	"mix.exs":           "elixir",
	"package.swift":     "swift",
	"pubspec.yaml":      "dart",
	"conanfile.txt":     "cpp",
	"conanfile.py":      "cpp",
	"project.toml":      "julia", // Julia primary manifest
	"juliaproject.toml": "julia", // Julia secondary (used in some monorepos)
	"description":       "cran",  // R/CRAN/Bioconductor — no extension by convention
	"meta.yaml":         "conda", // conda-build recipe
	"recipe.yaml":       "conda", // rattler-build recipe (newer conda variant)
}

// suffixManifests lists file SUFFIXES (case-insensitive) that map to a
// manifest type. Used for *.gemspec / *.csproj / *.cabal patterns where
// the basename varies.
var suffixManifests = []struct {
	suffix string
	typ    string
}{
	{".gemspec", "rubygems"},
	{".csproj", "nuget"},
	{".fsproj", "nuget"},
	{".vbproj", "nuget"},
	{".cabal", "haskell"},
	{".podspec", "swift"}, // CocoaPods (Swift/Obj-C)
}

// classifyManifestFilename returns the manifest_type for a given file
// basename, or "" if the file is not a recognized manifest. Case-
// insensitive on the basename.
func classifyManifestFilename(name string) string {
	lower := strings.ToLower(name)
	if typ, ok := wellKnownManifests[lower]; ok {
		return typ
	}
	for _, s := range suffixManifests {
		if strings.HasSuffix(lower, s.suffix) {
			return s.typ
		}
	}
	return ""
}

// ListRootManifests enumerates well-known package manifests at the
// root of the repo AND one level deep (to catch monorepo cases like
// `packages/foo/package.json`). Returns model.DistributionManifest
// rows with manifest_path + manifest_type populated. The declared
// package name is left empty here — content parsing happens in Phase
// D's manifest_parser.go.
//
// Optional endpoint: 404 on contents (a repository deleted or made private
// since its last collection; an empty repository is observed to answer it
// too — the Contents status table lists 404 without saying for what) →
// empty result + nil error. A 409 is ClassSkip too (v0.29.68; documented for
// the Git Database and Commits endpoints — "empty or unavailable" — not for
// contents), but the tree fallback treats it as a non-answer (fetchRootTree).
//
// v0.25.0: ctx wrapped in platform.WithoutETag so no 304 is solicited (a
// solicited one, read as "empty", was the silent-data-loss path via
// snapshot-replace in MarkDistributionComplete); an unsolicited one is a
// non-answer (round 8).
func (c *Client) ListRootManifests(ctx context.Context, owner, repo string) ([]model.DistributionManifest, error) {
	ctx = platform.WithoutETag(ctx)
	rootEntries, err := c.fetchContentsDir(ctx, owner, repo, "")
	if err != nil {
		if emptyAnswer(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("list root contents for %s/%s: %w", owner, repo, err)
	}

	var manifests []model.DistributionManifest
	var firstLevelDirs []string

	for _, e := range rootEntries {
		switch e.Type {
		case "file":
			if typ := classifyManifestFilename(e.Name); typ != "" {
				manifests = append(manifests, model.DistributionManifest{
					ManifestPath: e.Path,
					ManifestType: typ,
				})
			}
		case "dir":
			// Limit recursion to one level. Common monorepo shapes:
			// packages/, libs/, services/, apps/. We descend into
			// every first-level dir; the Contents API is cheap and
			// the cadence is 180 days.
			firstLevelDirs = append(firstLevelDirs, e.Path)
		}
	}

	// Bound: descend into at most the first 50 first-level dirs to
	// keep the per-claim API budget predictable on pathological
	// monorepos.
	if len(firstLevelDirs) > 50 {
		firstLevelDirs = firstLevelDirs[:50]
	}

	for _, dir := range firstLevelDirs {
		entries, err := c.fetchContentsDir(ctx, owner, repo, dir)
		if err != nil {
			// An answer about this directory (platform.IsDefinitiveAnswer:
			// 404/410, a non-rate-limit 403, a rejected request) or a 304:
			// nothing to list, keep the rest. The SAME rule as the scanner's
			// githubErrorIsNonAnswer — round 6 caught a 422 here failing the
			// listing while the scanner, counting it as an answer, stored the
			// scan complete with no manifests.
			if emptyAnswer(err) || errors.Is(err, platform.ErrRequestRejected) {
				// An answer for this ONE directory (worklist items 24/25's
				// boundary): the scan completes without its manifests. WARN
				// (review rounds 2–3 of items 22–25; it was silent): the root
				// listed this directory seconds ago, so a 404 here is the
				// forge contradicting itself — the round-5/6 escaping defect
				// showed up exactly this way — and the HTTP layer logs nothing
				// on a 404.
				c.logger.Warn("distribution: first-level directory skipped", "owner", owner, "repo", repo, "dir", dir, "error", err)
				continue
			}
			// Any other failure says nothing about the directory. Returning
			// the partial list with a nil error let the scanner store it as
			// the repo's complete manifest snapshot (v0.29.55 review round 3);
			// the error fails the scan instead, so nothing is replaced.
			return nil, fmt.Errorf("list %s contents for %s/%s: %w", dir, owner, repo, err)
		}
		for _, e := range entries {
			if e.Type != "file" {
				continue
			}
			if typ := classifyManifestFilename(e.Name); typ != "" {
				manifests = append(manifests, model.DistributionManifest{
					ManifestPath: e.Path,
					ManifestType: typ,
				})
			}
		}
	}

	return manifests, nil
}

// fetchContentsDir lists a single directory in the repo. Empty
// dirPath means root.
func (c *Client) fetchContentsDir(ctx context.Context, owner, repo, dirPath string) ([]ghContentsEntry, error) {
	apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s", owner, repo, contentsPath(dirPath))
	apiPath = strings.TrimRight(apiPath, "/")
	var entries []ghContentsEntry
	if err := c.http.GetJSON(ctx, apiPath, &entries); err != nil {
		return nil, err
	}
	if dirPath == "" && len(entries) >= contentsCap {
		// The Contents API stops silently at contentsCap (live:
		// DefinitelyTyped/types), and the partial listing was stored as a
		// complete scan (worklist item 24). The root is re-read through the
		// Git Trees API, which has no such cap.
		return c.fetchRootTree(ctx, owner, repo)
	}
	return entries, nil
}

// contentsCap is the most entries the GitHub Contents API returns for one
// directory; a listing that long is not known to be complete.
const contentsCap = 1000

// packagesPageSize is the one page of an owner's packages read per package
// type (GitHub's maximum per_page). A full page is logged as possibly
// incomplete; see ListRepoPackages.
const packagesPageSize = 100

// fetchRootTree lists the repository's root through the Git Trees API
// (HEAD, non-recursive), mapping blobs and trees to the Contents shape. It
// runs only after the Contents API listed 1,000 entries — the repository is
// readable and populated — so NO answer from the tree read can mean "no
// files": every error, the definitive ones included (a 404, a 409 the Git
// Database endpoints document for a repository "empty or unavailable"), is
// a NON-answer (review round 5 of items 22–25: a 404 here satisfied
// emptyAnswer, read as no manifests, and the scan completed — wiping the
// repository's snapshot for the cadence). A tree GitHub truncates (100k
// entries) is a non-answer for the same reason. A 304 is never SOLICITED
// by GetJSON (ETag-free, WithoutETag); one that arrives anyway surfaces as
// ErrNotModified and is read as a non-answer here as everywhere in these
// readers (review rounds 6 and 8; emptyAnswer).
func (c *Client) fetchRootTree(ctx context.Context, owner, repo string) ([]ghContentsEntry, error) {
	var tree struct {
		Truncated bool `json:"truncated"`
		Tree      []struct {
			Path string `json:"path"`
			Type string `json:"type"` // "blob" | "tree" | "commit"
		} `json:"tree"`
	}
	if err := c.http.GetJSON(ctx, fmt.Sprintf("/repos/%s/%s/git/trees/HEAD", owner, repo), &tree); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, err
		}
		if platform.IsDefinitiveAnswer(err) || errors.Is(err, platform.ErrNotModified) {
			return nil, fmt.Errorf("root tree for %s/%s after a capped Contents listing: %v — the listing is not complete: %w", owner, repo, err, platform.ErrTransient)
		}
		return nil, fmt.Errorf("root tree for %s/%s: %w", owner, repo, err)
	}
	if tree.Truncated {
		return nil, fmt.Errorf("root tree for %s/%s truncated by GitHub — the listing is not complete: %w", owner, repo, platform.ErrTransient)
	}
	entries := make([]ghContentsEntry, 0, len(tree.Tree))
	for _, e := range tree.Tree {
		switch e.Type {
		case "blob":
			entries = append(entries, ghContentsEntry{Name: e.Path, Path: e.Path, Type: "file"})
		case "tree":
			entries = append(entries, ghContentsEntry{Name: e.Path, Path: e.Path, Type: "dir"})
		}
	}
	return entries, nil
}

// FetchManifestContent fetches the raw text of a single manifest file
// from the repo via the GitHub Contents API. Used by Phase D's
// manifest_parser.go to extract declared package names. Returns the
// decoded UTF-8 string; binary or oversized manifests return ""
// (per the GitHub API's `content` field being base64'd for files
// under 1MB and absent for larger files — we treat both cases as
// "no content available" rather than retrying with the blobs API).
func (c *Client) FetchManifestContent(ctx context.Context, owner, repo, filePath string) (string, error) {
	// v0.25.0: bypass ETag — same rationale as ListRootManifests.
	ctx = platform.WithoutETag(ctx)
	apiPath := fmt.Sprintf("/repos/%s/%s/contents/%s", owner, repo, contentsPath(path.Clean(filePath)))
	var entry struct {
		Content  string `json:"content"`
		Encoding string `json:"encoding"`
	}
	if err := c.http.GetJSON(ctx, apiPath, &entry); err != nil {
		if emptyAnswer(err) {
			return "", nil
		}
		return "", fmt.Errorf("fetch manifest %s: %w", filePath, err)
	}
	if entry.Encoding != "base64" || entry.Content == "" {
		return "", nil
	}
	// GitHub returns base64 with embedded newlines.
	clean := strings.ReplaceAll(entry.Content, "\n", "")
	clean = strings.ReplaceAll(clean, "\r", "")
	decoded, err := base64.StdEncoding.DecodeString(clean)
	if err != nil {
		// Not "no content": the manifest would be stored with its declared
		// name blanked (v0.29.55 review round 3).
		return "", fmt.Errorf("decode manifest %s: %w", filePath, err)
	}
	return string(decoded), nil
}

// contentsPath escapes each segment of a repository path for a Contents API
// URL. Names come from the repository tree, so a file or directory may
// contain %, #, ? or spaces. Unescaped, "100%" failed to parse as a URL (no
// request sent) — since v0.29.55 a non-answer that fails the distribution
// scan, striking the repo toward the sideline every cycle; "C#" and "a?b"
// requested "C" and "a" instead (usually a 404, so that directory's
// manifests were silently skipped, or a scan failure when a same-named file
// sits beside it), and "a?b c" drew a 400 (review rounds 5 and 6).
func contentsPath(p string) string {
	segs := strings.Split(p, "/")
	for i, seg := range segs {
		segs[i] = url.PathEscape(seg)
	}
	return strings.Join(segs, "/")
}

// emptyAnswer is the ONE rule (SR-17) the GitHub distribution readers use to
// turn an error into "nothing to read here": an answer about the item
// (platform.IsDefinitiveAnswer) other than a rejected request. A rejected
// request is an answer but not an empty one — the readers return it and the
// scanner counts the source as errored; the first-level directory arm alone
// also skips it (review round 6 on v0.29.55). The client's own off-host
// refusal is NOT an answer, so it is returned and fails the scan (Copilot
// review on PR #209; these readers swallowed it as ClassSkip before). A 304
// is not an answer either (review round 8 of items 22–25): these readers
// never solicit one (GetJSON is ETag-free), so one that arrives is a
// misbehaving intermediary's, says nothing about the repository, and read
// as "empty" it was the very snapshot-wipe the ETag bypass exists to
// prevent; it is returned and the scanner treats it as a non-answer
// (githubErrorIsNonAnswer — the same rule, one spelling each side).
func emptyAnswer(err error) bool {
	return platform.IsDefinitiveAnswer(err) && !errors.Is(err, platform.ErrRequestRejected)
}
