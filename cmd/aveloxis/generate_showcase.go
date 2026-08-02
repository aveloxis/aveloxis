// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
	"html/template"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/showcase"
	"github.com/spf13/cobra"
)

// generate-showcase (growth plan phase 4, 2026-08-02) — renders the
// PUBLIC collection showcase pages + sitemap.xml as static HTML into
// the nginx docroot. Runs on a systemd timer (hourly); see
// aveloxis-gui/deploy/aveloxis-showcase.{service,timer}.
//
// Design decisions (the plan's one-way doors):
//   - Static generation over live public endpoints: full SEO + social
//     unfurls with ZERO auth-surface change — the api publicPaths
//     allowlist stays at 2 entries.
//   - URL scheme /showcase/{slug}.html; slug = Slugify(name) with a
//     -2/-3… suffix on collisions. Renaming a collection changes its
//     slug (documented; the pruner removes the old page).
//   - The generator is the SINGLE WRITER of sitemap.xml once
//     deployed (static core + blog/*.html glob + showcase pages).
//   - PRIVACY: queries run with userID=0 and only admin-curated
//     collection metadata + per-repo cached counts are rendered —
//     never user names, stars, or group ownership.

// showcaseOpts parameterizes one generation run (testable core).
type showcaseOpts struct {
	OutDir   string // showcase pages land here (…/showcase)
	BaseURL  string // canonical origin, no trailing slash
	GUIRoot  string // docroot for sitemap.xml + blog glob ("" = skip sitemap)
	RepoCap  int    // per-collection table cap (top-N by collected issues)
	PageSize int    // store page size while accumulating rows
	// RepoPages: the top-N repos of EACH collection get a public repo
	// snapshot page under <out>/repos/ (2026-08-02 operator decision:
	// 5 — the public collections span ~59K repos, so per-repo pages
	// exist only for the handful a visitor recognizes; sign-in unlocks
	// the rest). 0 = default 5; negative = disabled.
	RepoPages int
}

// showcaseSummary reports what a run did.
type showcaseSummary struct {
	Collections int
	RepoPages   int
	ComparePage bool
	Pruned      int
	Sitemap     bool
}

// repoTarget accumulates one featured repository across every
// collection that ranks it in its top-N (deduped by repo_id — a repo
// featured in two collections gets ONE page listing both).
type repoTarget struct {
	repoID        int64
	slug          string
	row           showcase.RepoRow
	lastCollected string
	collections   []showcase.RepoLink
}

func generateShowcaseCmd(cfgPath *string) *cobra.Command {
	var (
		outDir  string
		baseURL string
		guiRoot string
	)
	cmd := &cobra.Command{
		Use:   "generate-showcase",
		Short: "Render the public collection showcase pages + sitemap.xml (static SEO pages)",
		Long: `Renders every admin-curated collection as a public, SEO-indexable
static HTML page under <out>/, plus a showcase index, plus the site
sitemap.xml (when --gui-root is given). Deployed behind nginx as plain
files — no API surface is opened and nothing personal is rendered
(no user names, stars, or group ownership; queries run anonymously).

Idempotent and atomic: pages write via tmp+rename; pages whose
collection no longer exists are pruned. Run it from the
aveloxis-showcase.timer systemd unit (hourly) or by hand after
editing collections.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
			cfg := loadConfig(*cfgPath, bootLog)
			logger := newLogger(cfg)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
			defer cancel()

			store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
			if err != nil {
				return fmt.Errorf("connecting to database: %w", err)
			}
			defer store.Close()

			// v0.21.5: store.Migrate(ctx) intentionally NOT called here.
			// generate-showcase is read-only on the schema and trusts
			// that the operator has already run migrations.

			sum, err := runGenerateShowcase(ctx, store, logger, showcaseOpts{
				OutDir: outDir, BaseURL: strings.TrimRight(baseURL, "/"), GUIRoot: guiRoot,
				RepoCap: 100, PageSize: 100, RepoPages: 5,
			})
			if err != nil {
				return err
			}
			logger.Info("showcase generated",
				"collections", sum.Collections, "repo_pages", sum.RepoPages, "compare", sum.ComparePage,
				"pruned", sum.Pruned, "sitemap", sum.Sitemap, "out", outDir)
			return nil
		},
	}
	cmd.Flags().StringVar(&outDir, "out", "./showcase", "output directory for the generated showcase pages")
	cmd.Flags().StringVar(&baseURL, "base-url", "https://aveloxis.io", "canonical site origin for links/OG/sitemap")
	cmd.Flags().StringVar(&guiRoot, "gui-root", "", "site docroot: sitemap.xml is written here and blog/*.html globbed from it (empty = skip sitemap)")
	return cmd
}

// runGenerateShowcase is the testable core: reads collections, renders
// pages atomically, prunes stale slugs, and (optionally) rewrites the
// site sitemap.
func runGenerateShowcase(ctx context.Context, store *db.PostgresStore, logger *slog.Logger, opts showcaseOpts) (showcaseSummary, error) {
	var sum showcaseSummary
	if opts.RepoCap <= 0 {
		opts.RepoCap = 100
	}
	if opts.PageSize <= 0 || opts.PageSize > 100 {
		opts.PageSize = 100
	}
	if opts.RepoPages == 0 {
		opts.RepoPages = 5
	}
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return sum, fmt.Errorf("creating out dir: %w", err)
	}

	cols, err := store.ListCollections(ctx, 0) // userID 0: anonymous — no per-user star ordering
	if err != nil {
		return sum, fmt.Errorf("listing collections: %w", err)
	}

	now := time.Now()
	// "compare" is reserved for the static comparison demo page
	// (v0.27.80) — a collection literally named "Compare" gets the
	// -2 suffix instead of colliding with it.
	slugSeen := map[string]int{"compare": 1}
	var cards []showcase.CollectionCard
	emitted := map[string]bool{"index.html": true}
	targets := map[int64]*repoTarget{} // featured repos, deduped by repo_id
	repoSlugToID := map[string]int64{} // snapshot slug → repo_id (collision suffixing)

	for _, c := range cols {
		slug := showcase.Slugify(c.Name)
		slugSeen[slug]++
		if n := slugSeen[slug]; n > 1 {
			slug = fmt.Sprintf("%s-%d", slug, n)
		}

		groups, err := store.GetCollectionGroups(ctx, c.CollectionID)
		if err != nil {
			return sum, fmt.Errorf("collection %d groups: %w", c.CollectionID, err)
		}

		// Top-N by collected ISSUES (the public table). Issues, not
		// commits, on operator decision 2026-08-02: commit counts are
		// bot-floodable (conda-forge automation repos carry millions of
		// machine commits and headlined the NumFocus/SciOSS pages);
		// issue counts track human community engagement and surface the
		// flagship projects (pytorch, pandas, kubernetes) on real data.
		// userID 0 → starred is FALSE everywhere; the templates have no
		// field to render it anyway (the structural privacy guarantee).
		var rows []showcase.RepoRow
		total := 0
		featured := 0
		lastFeaturedRow := -1 // where the sign-in CTA row renders (v0.27.80)
		for page := 1; len(rows) < opts.RepoCap; page++ {
			repos, tot, err := store.GetCollectionRepos(ctx, c.CollectionID, 0, page, opts.PageSize, "issues", "desc")
			if err != nil {
				return sum, fmt.Errorf("collection %d repos page %d: %w", c.CollectionID, page, err)
			}
			total = tot
			// Fork status for the page, fetched only while featured
			// slots remain open. Forks are EXCLUDED from the snapshot
			// selection (2026-08-02 operator decision): size-based
			// ordering surfaces high-commit mirrors/forks
			// (flatironinstitute/nixpkgs, sys-bio/llvm-*) that aren't
			// the collection's own flagship work — the next non-fork
			// slides into the freed slot.
			var forkStatus map[int64]bool
			if opts.RepoPages > 0 && featured < opts.RepoPages {
				ids := make([]int64, 0, len(repos))
				for _, r := range repos {
					ids = append(ids, r.RepoID)
				}
				if forkStatus, err = store.GetForkStatusBatch(ctx, ids); err != nil {
					return sum, fmt.Errorf("collection %d fork status: %w", c.CollectionID, err)
				}
			}
			for _, r := range repos {
				if len(rows) >= opts.RepoCap {
					break
				}
				row := showcase.RepoRow{
					Owner: r.Owner, Name: r.Name, ForgeURL: r.GitURL,
					Issues: r.Issues, PRs: r.PRs, Commits: r.Commits,
				}
				if r.LastActivity != nil {
					row.LastActivity = r.LastActivity.UTC().Format("2006-01-02")
				}
				// The collection's top-N NON-FORK repos get a public
				// snapshot page; the row's name links there (rows are
				// already issues-desc, so scan order == rank).
				if opts.RepoPages > 0 && featured < opts.RepoPages && !forkStatus[r.RepoID] {
					row.PageSlug = registerFeaturedRepo(targets, repoSlugToID, r, row,
						showcase.RepoLink{Slug: slug, Name: c.Name})
					featured++
					lastFeaturedRow = len(rows)
				}
				rows = append(rows, row)
			}
			if len(repos) < opts.PageSize {
				break // last page
			}
		}

		var b strings.Builder
		err = showcase.RenderCollection(&b, showcase.CollectionData{
			BaseURL: opts.BaseURL, Slug: slug, Name: c.Name, Description: c.Description,
			Groups: len(groups), TotalRepos: total, GeneratedAt: now, Repos: rows,
			CTARowAfter: lastFeaturedRow,
		})
		if err != nil {
			return sum, fmt.Errorf("rendering %s: %w", slug, err)
		}
		if err := writeAtomic(filepath.Join(opts.OutDir, slug+".html"), []byte(b.String())); err != nil {
			return sum, err
		}
		emitted[slug+".html"] = true
		cards = append(cards, showcase.CollectionCard{
			Slug: slug, Name: c.Name, Description: c.Description,
			Groups: len(groups), Repos: total,
		})
		sum.Collections++
	}

	// ---- Repo snapshot pages (top-N per collection, deduped) ----
	reposDir := filepath.Join(opts.OutDir, "repos")
	if err := os.MkdirAll(reposDir, 0o755); err != nil {
		return sum, fmt.Errorf("creating repos dir: %w", err)
	}
	repoSlugs := make([]string, 0, len(repoSlugToID))
	for s := range repoSlugToID {
		repoSlugs = append(repoSlugs, s)
	}
	sort.Strings(repoSlugs) // deterministic render + sitemap order
	emittedRepos := map[string]bool{}
	for _, rslug := range repoSlugs {
		t := targets[repoSlugToID[rslug]]
		page, err := buildRepoPage(ctx, store, opts.BaseURL, now, t)
		if err != nil {
			return sum, fmt.Errorf("repo page %s (repo %d): %w", rslug, t.repoID, err)
		}
		var rb strings.Builder
		if err := showcase.RenderRepo(&rb, page); err != nil {
			return sum, fmt.Errorf("rendering repo page %s: %w", rslug, err)
		}
		if err := writeAtomic(filepath.Join(reposDir, rslug+".html"), []byte(rb.String())); err != nil {
			return sum, err
		}
		emittedRepos[rslug+".html"] = true
		sum.RepoPages++
	}
	// Prune repo pages whose repo fell out of every top-N (or whose
	// collection was deleted/renamed).
	repoEntries, err := os.ReadDir(reposDir)
	if err != nil {
		return sum, fmt.Errorf("reading repos dir for prune: %w", err)
	}
	for _, e := range repoEntries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".html") || emittedRepos[e.Name()] {
			continue
		}
		if err := os.Remove(filepath.Join(reposDir, e.Name())); err != nil {
			logger.Warn("showcase repo-page prune failed", "file", e.Name(), "error", err)
			continue
		}
		sum.Pruned++
	}

	// ---- Static comparison demo (v0.27.80): four featured repos with
	// approximately the same activity level, compared on real data. ----
	compareEmitted, err := buildCompareDemo(ctx, store, opts, now, targets, repoSlugToID)
	if err != nil {
		return sum, err
	}
	if compareEmitted {
		emitted["compare.html"] = true
		sum.ComparePage = true
	}

	// Index page — rendered after the compare demo so it can link it.
	var b strings.Builder
	if err := showcase.RenderIndex(&b, showcase.IndexData{
		BaseURL: opts.BaseURL, GeneratedAt: now, Collections: cards,
		HasCompare: compareEmitted,
	}); err != nil {
		return sum, fmt.Errorf("rendering index: %w", err)
	}
	if err := writeAtomic(filepath.Join(opts.OutDir, "index.html"), []byte(b.String())); err != nil {
		return sum, err
	}

	// Prune pages whose collection no longer exists (renames included
	// — a renamed collection gets a fresh slug and the old page goes).
	entries, err := os.ReadDir(opts.OutDir)
	if err != nil {
		return sum, fmt.Errorf("reading out dir for prune: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".html") || emitted[e.Name()] {
			continue
		}
		if err := os.Remove(filepath.Join(opts.OutDir, e.Name())); err != nil {
			logger.Warn("showcase prune failed", "file", e.Name(), "error", err)
			continue
		}
		sum.Pruned++
	}

	// Sitemap — the generator is the single writer once deployed.
	if opts.GUIRoot != "" {
		var blog []string
		matches, _ := filepath.Glob(filepath.Join(opts.GUIRoot, "blog", "*.html"))
		for _, m := range matches {
			base := filepath.Base(m)
			if strings.HasPrefix(base, "_") { // _template.html scaffold
				continue
			}
			blog = append(blog, "blog/"+base)
		}
		var slugs []string
		for name := range emitted {
			if name != "index.html" {
				slugs = append(slugs, strings.TrimSuffix(name, ".html"))
			}
		}
		sort.Strings(slugs)
		xml := showcase.BuildSitemap(opts.BaseURL,
			[]string{"history.html", "augur.html"}, blog, slugs, repoSlugs, now)
		if err := writeAtomic(filepath.Join(opts.GUIRoot, "sitemap.xml"), xml); err != nil {
			return sum, err
		}
		sum.Sitemap = true
	}
	return sum, nil
}

// registerFeaturedRepo records a collection's top-N repo for snapshot
// rendering and returns its page slug. Deduped by repo_id: a repo
// featured in several collections keeps one slug and accumulates the
// "featured in" links. Distinct repos whose slugs collide (e.g.
// "a-b/c" vs "a/b-c") get a -2/-3… suffix.
func registerFeaturedRepo(targets map[int64]*repoTarget, repoSlugToID map[string]int64,
	r db.CollectionRepo, row showcase.RepoRow, coll showcase.RepoLink) string {
	if t, ok := targets[r.RepoID]; ok {
		t.collections = append(t.collections, coll)
		return t.slug
	}
	base := showcase.RepoSlug(r.Owner, r.Name)
	slug := base
	for i := 2; ; i++ {
		if _, taken := repoSlugToID[slug]; !taken {
			break
		}
		slug = fmt.Sprintf("%s-%d", base, i)
	}
	repoSlugToID[slug] = r.RepoID
	t := &repoTarget{repoID: r.RepoID, slug: slug, row: row,
		collections: []showcase.RepoLink{coll}}
	if r.LastCollected != nil {
		t.lastCollected = r.LastCollected.UTC().Format("2006-01-02")
	}
	targets[r.RepoID] = t
	return slug
}

// buildRepoPage assembles one repo snapshot page's data from the
// repo-level detail reads. All queries are repo-scoped — nothing
// user-scoped can reach the page (the showcase privacy contract).
func buildRepoPage(ctx context.Context, store *db.PostgresStore, baseURL string, now time.Time, t *repoTarget) (showcase.RepoPageData, error) {
	d := showcase.RepoPageData{
		BaseURL: baseURL, Slug: t.slug,
		Owner: t.row.Owner, Name: t.row.Name, ForgeURL: t.row.ForgeURL,
		Issues: t.row.Issues, PRs: t.row.PRs, Commits: t.row.Commits,
		LastActivity: t.row.LastActivity, LastCollected: t.lastCollected,
		GeneratedAt: now, Collections: t.collections,
	}
	var err error
	if d.Description, d.PrimaryLanguage, d.Archived, err = store.GetRepoShowcaseMeta(ctx, t.repoID); err != nil {
		return d, fmt.Errorf("showcase meta: %w", err)
	}
	checks, overall, asOf, err := store.GetRepoScorecard(ctx, t.repoID)
	if err != nil {
		return d, fmt.Errorf("scorecard: %w", err)
	}
	d.ScorecardOverall = overall
	if !asOf.IsZero() {
		d.ScorecardAsOf = asOf.UTC().Format("2006-01-02")
	}
	for _, c := range checks {
		d.ScorecardChecks = append(d.ScorecardChecks, showcase.RepoScorecardRow{Name: c.Name, Score: c.Score})
	}
	if d.DepsScanned, err = store.HasDependencyData(ctx, t.repoID); err != nil {
		return d, fmt.Errorf("dependency presence: %w", err)
	}
	if d.VulnTotal, d.VulnCritical, err = store.CountRepoVulnerabilities(ctx, t.repoID); err != nil {
		return d, fmt.Errorf("vulnerability counts: %w", err)
	}
	if d.ActivityChart, err = buildActivityChart(ctx, store, t.repoID, now); err != nil {
		return d, fmt.Errorf("activity chart: %w", err)
	}
	return d, nil
}

// weekStartUTC returns the Monday 00:00 UTC of t's ISO week — the
// same buckets date_trunc('week', … AT TIME ZONE 'UTC') produces, so
// DensifyWeekly's date-string join always lands.
func weekStartUTC(t time.Time) time.Time {
	d := t.UTC().Truncate(24 * time.Hour)
	return d.AddDate(0, 0, -((int(d.Weekday()) + 6) % 7))
}

// showcaseChartWindow is the static charts' trailing-12-month weekly
// window: 52 Monday buckets plus the current partial week.
func showcaseChartWindow(now time.Time) (since, until time.Time) {
	until = weekStartUTC(now).AddDate(0, 0, 7)
	since = until.AddDate(0, 0, -52*7)
	return since, until
}

func weeklyChartPoints(pts []db.WeeklyDataPoint, since, until time.Time) []showcase.ChartPoint {
	cp := make([]showcase.ChartPoint, 0, len(pts))
	for _, p := range pts {
		cp = append(cp, showcase.ChartPoint{T: p.WeekStart, V: float64(p.Count)})
	}
	return showcase.DensifyWeekly(cp, since, until)
}

// buildActivityChart renders a repo snapshot page's static weekly
// activity chart (v0.27.80). Nil chart = zero collected activity in
// the window — the template renders the honest empty state.
func buildActivityChart(ctx context.Context, store *db.PostgresStore, repoID int64, now time.Time) (*showcase.RepoChart, error) {
	since, until := showcaseChartWindow(now)
	ts, err := store.GetRepoTimeSeries(ctx, repoID, since, until)
	if err != nil {
		return nil, err
	}
	if len(ts.Commits)+len(ts.Issues)+len(ts.PRsOpened)+len(ts.PRsMerged) == 0 {
		return nil, nil
	}
	series := []showcase.ChartSeries{
		{Label: "Commits", Color: showcase.ChartPalette[0], Points: weeklyChartPoints(ts.Commits, since, until)},
		{Label: "Issues opened", Color: showcase.ChartPalette[1], Points: weeklyChartPoints(ts.Issues, since, until)},
		{Label: "PRs opened", Color: showcase.ChartPalette[2], Points: weeklyChartPoints(ts.PRsOpened, since, until)},
		{Label: "PRs merged", Color: showcase.ChartPalette[3], Points: weeklyChartPoints(ts.PRsMerged, since, until)},
	}
	svg := showcase.RenderLineChartSVG(series, 720, 220)
	if svg == "" {
		return nil, nil
	}
	chart := &showcase.RepoChart{
		Caption: "Weekly activity, trailing 12 months — sign in for 3y/5y windows and every CHAOSS metric.",
		SVG:     template.HTML(svg), // produced only by RenderLineChartSVG
	}
	for _, s := range series {
		chart.Legend = append(chart.Legend, showcase.LegendItem{Label: s.Label, Color: s.Color})
	}
	return chart, nil
}

// buildCompareDemo renders the static 4-repo comparison page from the
// featured pool: the four repos with the most comparable activity
// levels (2026-08-02 operator decision), one chart per headline
// metric, all data baked in at generation time.
func buildCompareDemo(ctx context.Context, store *db.PostgresStore, opts showcaseOpts, now time.Time,
	targets map[int64]*repoTarget, repoSlugToID map[string]int64) (bool, error) {
	cands := make([]showcase.CompareCandidate, 0, len(repoSlugToID))
	for slug, id := range repoSlugToID {
		t := targets[id]
		cands = append(cands, showcase.CompareCandidate{
			Slug: slug, Label: t.row.Owner + "/" + t.row.Name, Activity: t.row.Issues,
		})
	}
	sort.Slice(cands, func(i, j int) bool { return cands[i].Slug < cands[j].Slug }) // map order → deterministic
	picked := showcase.PickSimilarActivity(cands, 4)
	if len(picked) < 2 {
		return false, nil // nothing comparable to show
	}

	since, until := showcaseChartWindow(now)
	refs := make([]showcase.CompareRepoRef, 0, len(picked))
	legend := make([]showcase.LegendItem, 0, len(picked))
	for i, c := range picked {
		color := showcase.ChartPalette[i%len(showcase.ChartPalette)]
		refs = append(refs, showcase.CompareRepoRef{Label: c.Label, Slug: c.Slug, Color: color})
		legend = append(legend, showcase.LegendItem{Label: c.Label, Color: color})
	}
	metrics := []struct{ key, title string }{
		{"code_change_commits", "Code Change Commits"},
		{"issues", "Issues"},
		{"change_requests", "Change Requests"},
		{"contributors", "Contributors"},
	}
	var charts []showcase.RepoChart
	for _, m := range metrics {
		series := make([]showcase.ChartSeries, 0, len(picked))
		for i, c := range picked {
			pts, err := store.MetricWeeklySeries(ctx, []int64{repoSlugToID[c.Slug]}, m.key, "week", since, until)
			if err != nil {
				return false, fmt.Errorf("compare demo %s for %s: %w", m.key, c.Slug, err)
			}
			cp := make([]showcase.ChartPoint, 0, len(pts))
			for _, p := range pts {
				cp = append(cp, showcase.ChartPoint{T: p.Bucket, V: p.Value})
			}
			series = append(series, showcase.ChartSeries{
				Label: c.Label, Color: refs[i].Color,
				Points: showcase.DensifyWeekly(cp, since, until),
			})
		}
		svg := showcase.RenderLineChartSVG(series, 720, 220)
		if svg == "" {
			continue
		}
		charts = append(charts, showcase.RepoChart{Title: m.title, Legend: legend, SVG: template.HTML(svg)})
	}
	if len(charts) == 0 {
		return false, nil
	}

	var b strings.Builder
	if err := showcase.RenderComparePage(&b, showcase.ComparePageData{
		BaseURL: opts.BaseURL, GeneratedAt: now,
		WindowLabel: "trailing 12 months, weekly",
		Repos:       refs, Charts: charts,
	}); err != nil {
		return false, fmt.Errorf("rendering compare demo: %w", err)
	}
	if err := writeAtomic(filepath.Join(opts.OutDir, "compare.html"), []byte(b.String())); err != nil {
		return false, err
	}
	return true, nil
}

// writeAtomic writes via tmp+rename so nginx never serves a torn page.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("writing %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("renaming %s: %w", path, err)
	}
	return nil
}
