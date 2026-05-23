// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package github

import (
	"context"
	"net/http"
	"strings"
	"testing"
)

// v0.24.0 — GitHub-side fetchers for the DistributionWorker.
//
// Three signals from GitHub itself, distinct from deps.dev /
// ecosyste.ms (which are registry-side reverse lookups):
//
//   1. ListReleaseAssetExtensions — catalogs file-extension signals
//      from /repos/{o}/{r}/releases assets. A repo with a .whl or
//      .gem in its release assets is distributing software through
//      GitHub Releases regardless of whether it's on PyPI/RubyGems.
//
//   2. ListRepoPackages — GitHub Packages API. Best-effort: requires
//      `read:packages` scope; 403/404 → empty result, not error.
//
//   3. ListRootManifests — Contents API at the repo root + first-
//      level directories. Returns paths and detected manifest types;
//      declared-name parsing happens in Phase D.

// TestListReleaseAssetExtensions verifies that release assets with
// known package-like file extensions are reported as
// PackageDistribution rows with source="github_release_asset".
func TestListReleaseAssetExtensions(t *testing.T) {
	releasesResp := `[
        {
            "id": 1, "tag_name": "v1.0.0", "name": "v1.0.0",
            "published_at": "2024-01-15T10:00:00Z",
            "assets": [
                {"name": "mypackage-1.0.0-py3-none-any.whl", "browser_download_url": "https://github.com/x/y/releases/download/v1.0.0/mypackage-1.0.0-py3-none-any.whl"},
                {"name": "mypackage-1.0.0.tar.gz", "browser_download_url": "https://..."},
                {"name": "README.md", "browser_download_url": "https://..."}
            ]
        },
        {
            "id": 2, "tag_name": "v1.1.0", "name": "v1.1.0",
            "published_at": "2024-06-15T10:00:00Z",
            "assets": [
                {"name": "mypackage-1.1.0-py3-none-any.whl", "browser_download_url": "https://..."},
                {"name": "mypackage-1.1.0.jar", "browser_download_url": "https://..."}
            ]
        }
    ]`

	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "/releases") {
			t.Errorf("unexpected URL: %s", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(releasesResp))
	}))

	pkgs, err := client.ListReleaseAssetExtensions(context.Background(), "x", "y")
	if err != nil {
		t.Fatalf("ListReleaseAssetExtensions: %v", err)
	}

	// Expect: pypi (whl extension), maven (jar extension). The
	// generic .tar.gz is ignored (too ambiguous — source archives,
	// not necessarily a distribution package). The .md is ignored.
	gotEcos := make(map[string]int)
	for _, p := range pkgs {
		if p.Source != "github_release_asset" {
			t.Errorf("Source = %q, want github_release_asset", p.Source)
		}
		gotEcos[p.Ecosystem] = p.VersionCount
	}

	if gotEcos["pypi"] != 2 {
		t.Errorf("pypi version count = %d, want 2 (whl in two releases)", gotEcos["pypi"])
	}
	if gotEcos["maven"] != 1 {
		t.Errorf("maven version count = %d, want 1 (.jar in one release)", gotEcos["maven"])
	}
	if _, hasTarGz := gotEcos["unknown"]; hasTarGz {
		t.Error(".tar.gz should not produce a row — too ambiguous to claim packaging intent")
	}
}

// TestListReleaseAssetExtensionsHandlesEmptyReleases ensures no
// failure on repos with no releases at all (common for libraries
// that never cut a tagged release).
func TestListReleaseAssetExtensionsHandlesEmptyReleases(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[]`))
	}))

	pkgs, err := client.ListReleaseAssetExtensions(context.Background(), "x", "y")
	if err != nil {
		t.Fatalf("empty releases must not be an error: %v", err)
	}
	if len(pkgs) != 0 {
		t.Errorf("empty releases must produce zero rows, got %d", len(pkgs))
	}
}

// TestListReleaseAssetExtensionsHandles404Gracefully covers the
// "this repo has releases disabled or is GitHub Generic Git" case.
// 404 must surface as empty result, not error — matches the rest of
// the optional-endpoint contract.
func TestListReleaseAssetExtensionsHandles404Gracefully(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))

	pkgs, err := client.ListReleaseAssetExtensions(context.Background(), "x", "y")
	if err != nil {
		t.Errorf("404 on releases must be non-fatal, got %v", err)
	}
	if len(pkgs) != 0 {
		t.Errorf("404 must produce zero rows, got %d", len(pkgs))
	}
}

// TestListRepoPackagesUsesUserEndpoint verifies the GitHub Packages
// API enumeration. We try /users/{owner}/packages first; the response
// is filtered by repository.name == repo. Packages with a different
// repository.name are excluded — those belong to other repos of the
// same owner.
func TestListRepoPackagesUsesUserEndpoint(t *testing.T) {
	pkgsResp := `[
        {
            "id": 1, "name": "mypackage", "package_type": "npm",
            "visibility": "public",
            "repository": {"name": "y", "full_name": "x/y"},
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-06-01T00:00:00Z",
            "version_count": 5
        },
        {
            "id": 2, "name": "otherpackage", "package_type": "npm",
            "visibility": "public",
            "repository": {"name": "other-repo", "full_name": "x/other-repo"},
            "version_count": 3
        }
    ]`

	// pkgs_response must come back keyed by package_type query
	// parameter. Track which package_types the client iterates.
	queried := make(map[string]int)
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		pt := r.URL.Query().Get("package_type")
		queried[pt]++
		w.Header().Set("Content-Type", "application/json")
		if pt == "npm" {
			_, _ = w.Write([]byte(pkgsResp))
			return
		}
		// Other package_types return empty
		_, _ = w.Write([]byte(`[]`))
	}))

	pkgs, err := client.ListRepoPackages(context.Background(), "x", "y")
	if err != nil {
		t.Fatalf("ListRepoPackages: %v", err)
	}

	// We expect ONE PackageDistribution row: mypackage (npm) filtered
	// from the response by repository.name == "y".
	if len(pkgs) != 1 {
		t.Fatalf("want 1 package (filtered to repo y), got %d", len(pkgs))
	}
	p := pkgs[0]
	if p.PackageName != "mypackage" {
		t.Errorf("PackageName = %q, want mypackage", p.PackageName)
	}
	if p.Ecosystem != "npm" {
		t.Errorf("Ecosystem = %q, want npm", p.Ecosystem)
	}
	if p.Source != "github_packages" {
		t.Errorf("Source = %q, want github_packages", p.Source)
	}
	if p.VersionCount != 5 {
		t.Errorf("VersionCount = %d, want 5", p.VersionCount)
	}

	// At least one package_type must have been queried — that's the
	// signal the iteration happened.
	if len(queried) == 0 {
		t.Error("no package_types queried; expected at least one (npm)")
	}
}

// TestListRepoPackagesGracefulOn403 verifies that when the OAuth
// token lacks read:packages scope, GitHub returns 403 — we want
// empty result, not error. This makes GitHub Packages a strictly
// best-effort signal.
func TestListRepoPackagesGracefulOn403(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Bad credentials (read:packages required)", http.StatusForbidden)
	}))

	pkgs, err := client.ListRepoPackages(context.Background(), "x", "y")
	if err != nil {
		t.Errorf("403 on packages (missing read:packages scope) must be non-fatal, got %v", err)
	}
	if len(pkgs) != 0 {
		t.Errorf("403 must produce zero rows, got %d", len(pkgs))
	}
}

// TestListRepoPackagesFallsBackToOrg verifies the user→org fallback
// chain. Per GitHub's API, /users/{owner}/packages may 404 if the
// owner is actually an organization; the client falls through to
// /orgs/{owner}/packages.
func TestListRepoPackagesFallsBackToOrg(t *testing.T) {
	orgPkgsResp := `[
        {
            "id": 1, "name": "container-image", "package_type": "container",
            "visibility": "public",
            "repository": {"name": "y", "full_name": "myorg/y"},
            "version_count": 12
        }
    ]`

	hitOrgEndpoint := false
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/users/"):
			http.Error(w, "not found", http.StatusNotFound)
		case strings.HasPrefix(r.URL.Path, "/orgs/"):
			hitOrgEndpoint = true
			if r.URL.Query().Get("package_type") == "container" {
				_, _ = w.Write([]byte(orgPkgsResp))
				return
			}
			_, _ = w.Write([]byte(`[]`))
		}
	}))

	pkgs, err := client.ListRepoPackages(context.Background(), "myorg", "y")
	if err != nil {
		t.Fatalf("ListRepoPackages: %v", err)
	}
	if !hitOrgEndpoint {
		t.Error("client did not fall back to /orgs/ after user-endpoint 404")
	}
	if len(pkgs) != 1 || pkgs[0].PackageName != "container-image" {
		t.Errorf("want one container-image row from /orgs/, got %+v", pkgs)
	}
}

// TestListRootManifests verifies the Contents API walker enumerates
// well-known manifest files at the repo root.
func TestListRootManifests(t *testing.T) {
	contentsResp := `[
        {"name": "README.md", "path": "README.md", "type": "file"},
        {"name": "package.json", "path": "package.json", "type": "file"},
        {"name": "pyproject.toml", "path": "pyproject.toml", "type": "file"},
        {"name": "Cargo.toml", "path": "Cargo.toml", "type": "file"},
        {"name": "src", "path": "src", "type": "dir"},
        {"name": ".gitignore", "path": ".gitignore", "type": "file"}
    ]`

	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// Root listing
		if r.URL.Path == "/repos/x/y/contents/" || r.URL.Path == "/repos/x/y/contents" {
			_, _ = w.Write([]byte(contentsResp))
			return
		}
		// First-level dir (we expect the walker to descend into src)
		if strings.HasSuffix(r.URL.Path, "/contents/src") {
			_, _ = w.Write([]byte(`[]`))
			return
		}
		http.NotFound(w, r)
	}))

	manifests, err := client.ListRootManifests(context.Background(), "x", "y")
	if err != nil {
		t.Fatalf("ListRootManifests: %v", err)
	}

	// Expect 3 manifests: package.json, pyproject.toml, Cargo.toml.
	// README.md and .gitignore are not manifests.
	gotTypes := make(map[string]string) // type -> path
	for _, m := range manifests {
		gotTypes[m.ManifestType] = m.ManifestPath
	}

	expected := map[string]string{
		"npm":   "package.json",
		"pypi":  "pyproject.toml",
		"cargo": "Cargo.toml",
	}
	for typ, path := range expected {
		if got := gotTypes[typ]; got != path {
			t.Errorf("expected manifest type %s at %s, got %q", typ, path, got)
		}
	}
}

// TestListRootManifestsHandles404 confirms the empty-repo / Generic
// Git case is graceful.
func TestListRootManifestsHandles404(t *testing.T) {
	client := testGHClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "not found", http.StatusNotFound)
	}))

	manifests, err := client.ListRootManifests(context.Background(), "x", "y")
	if err != nil {
		t.Errorf("404 on contents must be non-fatal, got %v", err)
	}
	if len(manifests) != 0 {
		t.Errorf("404 must produce zero manifests, got %d", len(manifests))
	}
}
