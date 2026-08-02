// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"fmt"
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
	RepoCap  int    // per-collection table cap (top-N by commits)
	PageSize int    // store page size while accumulating rows
}

// showcaseSummary reports what a run did.
type showcaseSummary struct {
	Collections int
	Pruned      int
	Sitemap     bool
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
				RepoCap: 100, PageSize: 100,
			})
			if err != nil {
				return err
			}
			logger.Info("showcase generated",
				"collections", sum.Collections, "pruned", sum.Pruned, "sitemap", sum.Sitemap, "out", outDir)
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
	if err := os.MkdirAll(opts.OutDir, 0o755); err != nil {
		return sum, fmt.Errorf("creating out dir: %w", err)
	}

	cols, err := store.ListCollections(ctx, 0) // userID 0: anonymous — no per-user star ordering
	if err != nil {
		return sum, fmt.Errorf("listing collections: %w", err)
	}

	now := time.Now()
	slugSeen := map[string]int{}
	var cards []showcase.CollectionCard
	emitted := map[string]bool{"index.html": true}

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

		// Top-N by commits (the public table). userID 0 → starred is
		// FALSE everywhere; the templates have no field to render it
		// anyway (the structural privacy guarantee).
		var rows []showcase.RepoRow
		total := 0
		for page := 1; len(rows) < opts.RepoCap; page++ {
			repos, tot, err := store.GetCollectionRepos(ctx, c.CollectionID, 0, page, opts.PageSize, "commits", "desc")
			if err != nil {
				return sum, fmt.Errorf("collection %d repos page %d: %w", c.CollectionID, page, err)
			}
			total = tot
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

	var b strings.Builder
	if err := showcase.RenderIndex(&b, showcase.IndexData{
		BaseURL: opts.BaseURL, GeneratedAt: now, Collections: cards,
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
			[]string{"history.html", "augur.html"}, blog, slugs, now)
		if err := writeAtomic(filepath.Join(opts.GUIRoot, "sitemap.xml"), xml); err != nil {
			return sum, err
		}
		sum.Sitemap = true
	}
	return sum, nil
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
