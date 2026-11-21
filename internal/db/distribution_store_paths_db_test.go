// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"context"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/model"
)

// The distribution worker's completion writes what it found, and the three
// operator readings of it (the repository's detail, the orphans, the
// fleet's rollup) read that back.
func TestADistributionScanIsStoredRotatedAndReadBack(t *testing.T) {
	store := openTestStore(t)
	ctx := context.Background()
	repoID, owner, name := pathsRepo(t, store, "dist",
		"aveloxis_data.repo_distribution_history", "aveloxis_data.repo_distribution_manifest_history",
		"aveloxis_data.repo_distribution", "aveloxis_data.repo_distribution_manifest")
	slug := owner + "/" + name
	// The claim takes a repository that has been collected.
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_ops.collection_queue (repo_id, status, due_at, last_collected)
		VALUES ($1, 'queued', NOW() + interval '7 days', NOW() - interval '1 day')`, repoID)
	before, err := store.GetDistributionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}

	job := claimMine(t, store, ctx, repoID)
	if job == nil {
		t.Fatal("the seeded repository was not claimed")
	}
	published := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	if err := store.MarkDistributionComplete(ctx, job,
		[]model.PackageDistribution{
			{Ecosystem: "pypi", PackageName: "zz-paths-pkg", VersionCount: 7, FirstPublishedAt: published, LatestPublishedAt: published.Add(time.Hour), Source: "deps.dev", Extra: map[string]any{"k": "v"}},
			{Ecosystem: "", PackageName: "skipped-no-ecosystem", Source: "deps.dev"},
		},
		[]model.DistributionManifest{
			{ManifestPath: "pyproject.toml", ManifestType: "pypi", PackageNameDeclared: "zz-paths-pkg"},
			{ManifestPath: "web/package.json", ManifestType: "npm", PackageNameDeclared: "zz-paths-web"},
			{ManifestPath: "", ManifestType: "npm"},
		}, true); err != nil {
		t.Fatal(err)
	}

	detail, err := store.GetRepoDistribution(ctx, slug)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Distributions) != 1 || detail.Distributions[0] != (DistributionRow{Ecosystem: "pypi", PackageName: "zz-paths-pkg", VersionCount: 7, Source: "deps.dev"}) {
		t.Errorf("the stored distributions read back as %+v; want the one with an ecosystem", detail.Distributions)
	}
	if len(detail.Manifests) != 2 || detail.Manifests[0] != (ManifestRow{ManifestPath: "pyproject.toml", ManifestType: "pypi", PackageNameDeclared: "zz-paths-pkg"}) || detail.Manifests[1].ManifestPath != "web/package.json" {
		t.Errorf("the stored manifests read back as %+v; want the two with a path, by path", detail.Manifests)
	}
	if got := pathsText(t, store, `SELECT extra->>'k' FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, repoID); got != "v" {
		t.Errorf("the distribution's extra holds k = %q, want v", got)
	}
	if got := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_distribution WHERE repo_id = $1 AND first_published_at = $2 AND latest_published_at = $3`, repoID, published, published.Add(time.Hour)); got != 1 {
		t.Errorf("the publication dates were not stored as given")
	}
	if _, err := store.GetRepoDistribution(ctx, slug+"-no-such"); err == nil {
		t.Error("a repository that does not exist was read without error")
	}
	// The slug is matched whole: the owner and the slash alone, which this
	// repository's slug begins with, name no repository.
	if _, err := store.GetRepoDistribution(ctx, owner+"/"); err == nil {
		t.Error("the beginning of a repository's slug was read as the repository")
	}

	// The npm manifest has no registry evidence for npm: it is an orphan.
	// The pypi one has, and is not.
	orphans, err := store.ListDistributionOrphans(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	mine := 0
	for _, o := range orphans {
		if o.RepoSlug == slug {
			mine++
			if o != (DistributionOrphan{RepoSlug: slug, ManifestType: "npm", ManifestPath: "web/package.json", PackageNameDeclared: "zz-paths-web"}) {
				t.Errorf("the repository's orphan is %+v", o)
			}
		}
	}
	if mine != 1 {
		t.Errorf("%d orphan manifests listed for the repository, want the npm one alone", mine)
	}
	if one, err := store.ListDistributionOrphans(ctx, 1); err != nil || len(one) != 1 {
		t.Errorf("a limit of one gave %d orphans (%v)", len(one), err)
	}

	after, err := store.GetDistributionStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after.TotalRepos != before.TotalRepos || after.ScannedRepos != before.ScannedRepos+1 {
		t.Errorf("repositories %d -> %d, scanned %d -> %d; want the same total and one more scanned", before.TotalRepos, after.TotalRepos, before.ScannedRepos, after.ScannedRepos)
	}
	if after.ReposWithRegistry != before.ReposWithRegistry+1 || after.ReposWithManifest != before.ReposWithManifest+1 || after.ReposManifestNoEvidence != before.ReposManifestNoEvidence+1 {
		t.Errorf("with registry %d -> %d, with manifest %d -> %d, manifest without evidence %d -> %d; want one more of each",
			before.ReposWithRegistry, after.ReposWithRegistry, before.ReposWithManifest, after.ReposWithManifest, before.ReposManifestNoEvidence, after.ReposManifestNoEvidence)
	}
	count := func(r *DistributionStatsRollup, eco string) int {
		for _, e := range r.PerEcosystem {
			if e.Ecosystem == eco {
				return e.Repos
			}
		}
		return 0
	}
	if count(after, "pypi") != count(before, "pypi")+1 {
		t.Errorf("repositories with a pypi distribution %d -> %d, want one more", count(before, "pypi"), count(after, "pypi"))
	}

	// A neighbour with a distribution row of its own: this repository's
	// scan rotates this repository's rows and no other's (round 8: the test
	// looked at its own repository's history only, and a rotation of every
	// repository's rows passed).
	neighbour, _, _ := pathsRepo(t, store, "dist-nb", "aveloxis_data.repo_distribution_history", "aveloxis_data.repo_distribution")
	mustExecRetry(ctx, t, store, `INSERT INTO aveloxis_data.repo_distribution (repo_id, ecosystem, package_name, source) VALUES ($1, 'pypi', 'zz-paths-neighbour', 'deps.dev')`, neighbour)

	// A second scan replaces the rows and rotates the first ones into
	// history; a row for the same key is updated in place within a scan.
	mustExecRetry(ctx, t, store, `UPDATE aveloxis_data.repos SET distribution_last_run = NULL WHERE repo_id = $1`, repoID)
	job = claimMine(t, store, ctx, repoID)
	if job == nil {
		t.Fatal("the repository was not claimed a second time")
	}
	if err := store.MarkDistributionComplete(ctx, job,
		[]model.PackageDistribution{
			{Ecosystem: "npm", PackageName: "zz-paths-web", VersionCount: 1, Source: "deps.dev"},
			{Ecosystem: "npm", PackageName: "zz-paths-web", VersionCount: 3, Source: "deps.dev"},
		},
		[]model.DistributionManifest{
			{ManifestPath: "web/package.json", ManifestType: "npm", PackageNameDeclared: "first"},
			{ManifestPath: "web/package.json", ManifestType: "npm", PackageNameDeclared: "second"},
		}, false); err != nil {
		t.Fatal(err)
	}
	detail, err = store.GetRepoDistribution(ctx, slug)
	if err != nil {
		t.Fatal(err)
	}
	if len(detail.Distributions) != 1 || detail.Distributions[0].VersionCount != 3 || len(detail.Manifests) != 1 || detail.Manifests[0].PackageNameDeclared != "second" {
		t.Errorf("after the second scan: %+v / %+v; want one distribution with 3 versions and one manifest declared 'second'", detail.Distributions, detail.Manifests)
	}
	if h := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_distribution_history WHERE repo_id = $1 AND package_name = 'zz-paths-pkg'`, repoID); h != 1 {
		t.Errorf("%d history rows of the first scan's distribution, want 1", h)
	}
	if h := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_distribution_manifest_history WHERE repo_id = $1`, repoID); h != 2 {
		t.Errorf("%d history rows of the first scan's manifests, want 2", h)
	}
	if h := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_distribution_history WHERE package_name = 'zz-paths-neighbour'`); h != 0 {
		t.Errorf("%d history rows of the NEIGHBOUR's distribution after this repository's scan, want 0", h)
	}
	if n := pathsInt(t, store, `SELECT COUNT(*) FROM aveloxis_data.repo_distribution WHERE repo_id = $1`, neighbour); n != 1 {
		t.Errorf("the neighbour has %d distribution rows after this repository's scan, want its 1", n)
	}
	if got := pathsText(t, store, `SELECT distribution_scan_complete::text FROM aveloxis_data.repos WHERE repo_id = $1`, repoID); got != "false" {
		t.Errorf("distribution_scan_complete after a scan that says it is not complete: %q", got)
	}
}
