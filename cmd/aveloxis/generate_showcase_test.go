// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
)

// generate-showcase (growth plan phase 4) — source-contract pins +
// the AVELOXIS_TEST_DB end-to-end run.

func showcaseSrc(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("generate_showcase.go")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestGenerateShowcaseCommandRegistered(t *testing.T) {
	b, err := os.ReadFile("main.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "generateShowcaseCmd(&cfgPath)") {
		t.Error("generate-showcase must be registered in main.go's AddCommand block")
	}
}

func TestGenerateShowcaseFlagsAndShape(t *testing.T) {
	src := showcaseSrc(t)
	for _, needle := range []string{
		`Use:   "generate-showcase"`,
		`"out"`, `"base-url"`, `"gui-root"`,
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate_showcase.go must contain %s", needle)
		}
	}
}

func TestGenerateShowcaseDoesNotMigrate(t *testing.T) {
	// Comment-stripped scan (the v0.21.5 contract pin — the comment
	// EXPLAINING the rule would otherwise false-match it).
	src := showcaseSrc(t)
	var code []string
	for _, line := range strings.Split(src, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		code = append(code, line)
	}
	if strings.Contains(strings.Join(code, "\n"), "store.Migrate(") {
		t.Error("generate-showcase must NOT call store.Migrate (v0.21.5: only serve and migrate run migrations)")
	}
}

func TestGenerateShowcaseAnonymousAndAtomic(t *testing.T) {
	src := showcaseSrc(t)
	// Anonymous reads: both collection reads pass userID 0.
	for _, needle := range []string{
		"ListCollections(ctx, 0)",
		"GetCollectionRepos(ctx, c.CollectionID, 0,",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate-showcase must query anonymously (userID 0): missing %q", needle)
		}
	}
	// Atomic writes + prune: tmp+rename, and stale slugs removed.
	for _, needle := range []string{"writeAtomic(", ".tmp", "os.Rename(", "os.Remove("} {
		if !strings.Contains(src, needle) {
			t.Errorf("generate-showcase must write atomically and prune stale pages: missing %q", needle)
		}
	}
}

// TestGenerateShowcaseRepoPages pins the repo snapshot-page surface
// (2026-08-02 operator decision: the top 5 repos of each collection
// get a PUBLIC static page; everything else needs sign-in).
func TestGenerateShowcaseRepoPages(t *testing.T) {
	src := showcaseSrc(t)
	// The per-collection featured count is 5 — a deliberate scope cap
	// (59K repos across the public collections; snapshot pages exist
	// for the handful a visitor recognizes, sign-in unlocks the rest).
	if !strings.Contains(src, "RepoPages") {
		t.Fatal("showcaseOpts must carry RepoPages (featured repos per collection)")
	}
	if !strings.Contains(src, "RepoPages: 5") && !strings.Contains(src, "opts.RepoPages = 5") {
		t.Error("the featured-repo count must default to 5")
	}
	// Detail reads — all repo-level, none user-scoped.
	for _, needle := range []string{
		"GetRepoShowcaseMeta(", "GetRepoScorecard(",
		"CountRepoVulnerabilities(", "HasDependencyData(",
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("repo snapshot pages must read %s", needle)
		}
	}
	// Pages land under <out>/repos/ and get their own prune pass.
	if !strings.Contains(src, `"repos"`) {
		t.Error("repo snapshot pages must render into the repos/ subdirectory")
	}
	// Forks are excluded from the featured selection (2026-08-02
	// operator decision) — size-based ordering surfaces
	// high-commit mirrors/forks (flatironinstitute/nixpkgs,
	// sys-bio/llvm-*) that aren't the collection's own flagship work.
	// The filter reads repos.forked_from via the batch helper.
	if !strings.Contains(src, "GetForkStatusBatch(") {
		t.Error("featured-repo selection must consult GetForkStatusBatch and skip forks")
	}
}

// TestGenerateShowcaseStaticCharts pins the v0.27.80 chart surface:
// each snapshot page gets a static weekly-activity SVG, and the
// showcase carries ONE static comparison page — four featured repos
// with approximately the same activity level (operator decision),
// completely static (no JS, no endpoints, the whole showcase
// premise).
func TestGenerateShowcaseStaticCharts(t *testing.T) {
	src := showcaseSrc(t)
	for _, needle := range []string{
		"GetRepoTimeSeries(",        // repo pages: weekly activity data
		"MetricWeeklySeries(",       // metric line charts + compare demo
		"PickSimilarActivity(",      // the similar-activity-window picker
		`"compare.html"`,            // the demo page itself
		"RenderLineChart(",          // charts are baked SVG, never JS
		"Trend: true",               // the live trend+tube grammar on every line chart
		"RenderStackedBarChart(",    // activity + retention use the stacked grammar
		"YLabel:",                   // catalog units ride the y-axis like the live charts
		// The computed metrics route through the SAME functions the
		// compare API uses — public snapshots can never disagree with
		// the signed-in charts.
		"api.BurstinessSeries(", "api.VelocitySeries(",
		"ContributorRetentionSeries(", "api.DefaultRetentionThreshold",
		"buildMetricCharts(", // every signed-in line graph, statically
		"ChipText()",         // the slope/R² header chip (trend.js wording)
		`slugSeen := map[string]int{"compare": 1}`, // reserved slug
	} {
		if !strings.Contains(src, needle) {
			t.Errorf("static-chart surface missing %s", needle)
		}
	}
	// The CTA row position rides CollectionData (the sign-in reminder
	// row under the featured block).
	if !strings.Contains(src, "CTARowAfter:") {
		t.Error("collection pages must position the sign-in CTA row after the featured block")
	}
}

// TestGenerateShowcaseEndToEnd (AVELOXIS_TEST_DB): seed a collection
// with a group + repos + cached queue counts, generate into a temp
// dir, and assert the pages exist with the right content and ZERO
// user-data leakage; then delete the collection and prove the prune.
func TestGenerateShowcaseEndToEnd(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_TEST_DB")
	if dsn == "" {
		t.Skip("AVELOXIS_TEST_DB not set")
	}
	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(store.Close)
	pool := store.Pool()

	clean := func() {
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collections WHERE name LIKE '_avshow%'`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_repos WHERE group_id IN (SELECT group_id FROM aveloxis_ops.user_groups WHERE name LIKE '_avshow%')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.user_groups WHERE name LIKE '_avshow%'`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.collection_queue WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avshow')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_scorecard WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avshow')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_deps_vulnerabilities WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avshow')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repo_dependencies WHERE repo_id IN (SELECT repo_id FROM aveloxis_data.repos WHERE repo_owner = '_avshow')`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_data.repos WHERE repo_owner = '_avshow'`)
		_, _ = pool.Exec(ctx, `DELETE FROM aveloxis_ops.users WHERE login_name LIKE '_avshow%'`)
	}
	clean()
	t.Cleanup(clean)

	suffix := time.Now().UnixNano()
	var adminID int
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.users (login_name, admin) VALUES ($1, TRUE) RETURNING user_id`,
		fmt.Sprintf("_avshow_admin_%d", suffix)).Scan(&adminID); err != nil {
		t.Fatal(err)
	}
	var gid int64
	if err := pool.QueryRow(ctx, `
		INSERT INTO aveloxis_ops.user_groups (user_id, name) VALUES ($1, '_avshow_grp') RETURNING group_id`, adminID).Scan(&gid); err != nil {
		t.Fatal(err)
	}
	// n drives BOTH last_issues and last_commits — the public table
	// and featured selection order by ISSUES (operator decision
	// 2026-08-02: commit counts are bot-floodable), and the page
	// content assertions read the commits tile.
	mkRepo := func(name string, n int64) int64 {
		var rid int64
		if err := pool.QueryRow(ctx, `
			INSERT INTO aveloxis_data.repos (repo_git, repo_owner, repo_name, platform_id)
			VALUES ($1, '_avshow', $2, 1) RETURNING repo_id`,
			"https://github.com/_avshow/"+name, name).Scan(&rid); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.collection_queue (repo_id, status, last_issues, last_prs, last_commits, last_collected)
			VALUES ($1, 'queued', $2, 7, $2, NOW())`, rid, n); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO aveloxis_ops.user_repos (group_id, repo_id) VALUES ($1, $2)`, gid, rid); err != nil {
			t.Fatal(err)
		}
		return rid
	}
	mkRepo("alpha", 500)
	betaID := mkRepo("beta", 900)
	gammaID := mkRepo("gamma", 800) // marked as a FORK below — excluded from snapshots
	mkRepo("delta", 700)
	mkRepo("epsilon", 600)
	mkRepo("zeta", 400) // slides INTO the top 5 because the gamma fork is skipped
	mkRepo("eta", 300)  // below the cut

	// gamma is a fork: it must be skipped by the featured selection
	// even though it ranks #2 by issues, and the next non-fork slides
	// into the freed slot (2026-08-02 operator decision).
	if _, err := pool.Exec(ctx, `
		UPDATE aveloxis_data.repos SET forked_from = 'upstream/gamma' WHERE repo_id = $1`, gammaID); err != nil {
		t.Fatal(err)
	}

	// beta (the top repo) gets the full detail surface: description +
	// language, a scorecard with headline, one CRITICAL vulnerability,
	// and a dependency row (so DepsScanned=true).
	if _, err := pool.Exec(ctx, `
		UPDATE aveloxis_data.repos SET repo_description = 'Fast <script>x</script> streaming',
		       primary_language = 'Go' WHERE repo_id = $1`, betaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_deps_scorecard (repo_id, name, score, data_collection_date)
		VALUES ($1, 'Maintained', '8', NOW()), ($1, '__overall__', '7.5', NOW())`, betaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_deps_vulnerabilities (repo_id, vuln_id, package_name, severity)
		VALUES ($1, 'GHSA-avshow-test', 'left-pad', 'CRITICAL')`, betaID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO aveloxis_data.repo_dependencies (repo_id, dep_name) VALUES ($1, 'left-pad')`, betaID); err != nil {
		t.Fatal(err)
	}

	collID, err := store.CreateCollection(ctx, "_avshow Ecosystem <One>", "the showcase test set", 1, adminID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AddGroupToCollection(ctx, collID, gid); err != nil {
		t.Fatal(err)
	}

	out := t.TempDir()
	sum, err := runGenerateShowcase(ctx, store, logger, showcaseOpts{
		OutDir: out, BaseURL: "https://aveloxis.io", GUIRoot: out, RepoCap: 100, PageSize: 100,
	})
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if sum.Collections < 1 || !sum.Sitemap {
		t.Fatalf("summary: %+v", sum)
	}

	slug := "avshow-ecosystem-one"
	page, err := os.ReadFile(filepath.Join(out, slug+".html"))
	if err != nil {
		// Slug collision suffixing or other collections in the scratch
		// DB may shift the exact name — find it by content.
		entries, _ := os.ReadDir(out)
		t.Fatalf("expected %s.html (dir has %d entries): %v", slug, len(entries), err)
	}
	html := string(page)
	for _, needle := range []string{"_avshow Ecosystem", "beta", "900", "canonical", "showcase-login-cta"} {
		if !strings.Contains(html, needle) {
			t.Errorf("collection page missing %q", needle)
		}
	}
	// beta (900 issues) sorts before alpha (500) — issues desc.
	if strings.Index(html, "beta") > strings.Index(html, ">alpha<") && strings.Contains(html, ">alpha<") {
		t.Error("repos must order by issues desc (beta before alpha)")
	}
	// ZERO user-data leakage.
	lower := strings.ToLower(html)
	for _, banned := range []string{"_avshow_admin", "_avshow_grp", "login_name", "starred"} {
		if strings.Contains(lower, banned) {
			t.Errorf("public page leaked %q", banned)
		}
	}
	// ---- Repo snapshot pages (top 5 non-fork by issues) ----
	// Issues order: beta(900) gamma(800,FORK) delta(700) epsilon(600)
	// alpha(500) zeta(400) eta(300). The gamma fork is skipped, so the
	// featured five are beta delta epsilon alpha zeta; eta stays out.
	for _, name := range []string{"beta", "delta", "epsilon", "alpha", "zeta"} {
		if _, err := os.Stat(filepath.Join(out, "repos", "avshow-"+name+".html")); err != nil {
			t.Errorf("featured repo %s must get a snapshot page: %v", name, err)
		}
	}
	for _, name := range []string{"gamma", "eta"} {
		if _, err := os.Stat(filepath.Join(out, "repos", "avshow-"+name+".html")); !os.IsNotExist(err) {
			t.Errorf("repo %s must NOT get a snapshot page (fork / below the cut)", name)
		}
	}
	// The collection page links featured names to the snapshot pages
	// and never invents links for forks or below-the-cut rows.
	if !strings.Contains(html, `href="/showcase/repos/avshow-beta.html"`) {
		t.Error("collection page must link featured repos to their snapshot pages")
	}
	for _, name := range []string{"gamma", "eta"} {
		if strings.Contains(html, `/showcase/repos/avshow-`+name+`.html`) {
			t.Errorf("collection page must not link %s internally (fork / below the cut)", name)
		}
	}

	// The sign-in CTA row renders under the featured block (v0.27.80)
	// with the featured rows visually distinct.
	if !strings.Contains(html, `class="cta-row"`) {
		t.Error("collection page must carry the sign-in reminder row under the featured block")
	}
	if !strings.Contains(html, `<tr class="featured">`) {
		t.Error("featured rows must be visually distinct")
	}

	// The static comparison demo exists: 4 of the 5 featured repos
	// (the tightest activity window: delta 700, epsilon 600, alpha
	// 500, zeta 400 — beta's 900 falls outside) with baked SVG charts.
	comparePage, err := os.ReadFile(filepath.Join(out, "compare.html"))
	if err != nil {
		t.Fatalf("compare demo page must exist: %v", err)
	}
	compareHTML := string(comparePage)
	for _, needle := range []string{"_avshow/delta", "_avshow/zeta", "<svg", "showcase-login-cta"} {
		if !strings.Contains(compareHTML, needle) {
			t.Errorf("compare demo missing %q", needle)
		}
	}
	if strings.Contains(compareHTML, "_avshow/beta") {
		t.Error("beta (900 issues) sits outside the tightest 4-repo activity window and must not be picked")
	}
	// And the index links it.
	indexPage, err := os.ReadFile(filepath.Join(out, "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(indexPage), "/showcase/compare.html") {
		t.Error("showcase index must link the comparison demo")
	}

	// The rich repo page: detail fields, scorecard, vuln posture, SEO,
	// escaping, zero user leakage.
	betaPage, err := os.ReadFile(filepath.Join(out, "repos", "avshow-beta.html"))
	if err != nil {
		t.Fatal(err)
	}
	betaHTML := string(betaPage)
	if strings.Contains(betaHTML, "<script>x</script>") {
		t.Error("repo description must be HTML-escaped")
	}
	for _, needle := range []string{
		"Fast", "Go", "Maintained", "Overall score", "7.5",
		"1 critical", "900",
		`<link rel="canonical" href="https://aveloxis.io/showcase/repos/avshow-beta.html" />`,
		"SoftwareSourceCode", "showcase-login-cta",
		"_avshow Ecosystem", // featured-in link back to the collection
	} {
		if !strings.Contains(betaHTML, needle) {
			t.Errorf("repo page missing %q", needle)
		}
	}
	lowerBeta := strings.ToLower(betaHTML)
	for _, banned := range []string{"_avshow_admin", "_avshow_grp", "login_name", "starred"} {
		if strings.Contains(lowerBeta, banned) {
			t.Errorf("public repo page leaked %q", banned)
		}
	}
	// A featured repo with no scorecard / dependency data renders the
	// honest empty states — never a fabricated clean bill.
	alphaPage, err := os.ReadFile(filepath.Join(out, "repos", "avshow-alpha.html"))
	if err != nil {
		t.Fatal(err)
	}
	for _, needle := range []string{"not yet scanned", "analysis pending", "No collected activity"} {
		if !strings.Contains(string(alphaPage), needle) {
			t.Errorf("unscanned repo page missing honest empty state %q", needle)
		}
	}
	// Every signed-in line graph appears statically (2026-08-02
	// operator ask): the seven base metrics + burstiness + velocity +
	// retention — flat-zero windows still chart (honest data).
	for _, needle := range []string{
		"CHAOSS metrics", "Contributors", "Change Requests Merged",
		"Issues Closed", "Committers", "Burstiness", "Project Velocity",
		"Contributor Retention",
	} {
		if !strings.Contains(string(alphaPage), needle) {
			t.Errorf("repo page missing metric section %q", needle)
		}
	}
	if got := strings.Count(string(alphaPage), "<svg"); got < 10 {
		t.Errorf("repo page must carry all 10 metric charts, got %d svgs", got)
	}

	// Sitemap exists and carries the pages.
	sm, err := os.ReadFile(filepath.Join(out, "sitemap.xml"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(sm), "/showcase/"+slug+".html") {
		t.Error("sitemap must include the generated showcase page")
	}
	if !strings.Contains(string(sm), "/showcase/repos/avshow-beta.html") {
		t.Error("sitemap must include the repo snapshot pages")
	}

	// Prune: delete the collection, regenerate, pages disappear —
	// including the repo snapshot pages.
	if err := store.DeleteCollection(ctx, collID); err != nil {
		t.Fatal(err)
	}
	sum2, err := runGenerateShowcase(ctx, store, logger, showcaseOpts{
		OutDir: out, BaseURL: "https://aveloxis.io", GUIRoot: out, RepoCap: 100, PageSize: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(out, slug+".html")); !os.IsNotExist(err) {
		t.Errorf("deleted collection's page must be pruned (pruned=%d)", sum2.Pruned)
	}
	if _, err := os.Stat(filepath.Join(out, "repos", "avshow-beta.html")); !os.IsNotExist(err) {
		t.Errorf("deleted collection's repo snapshot pages must be pruned (pruned=%d)", sum2.Pruned)
	}
}
