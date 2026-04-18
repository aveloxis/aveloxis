package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/augurlabs/aveloxis/internal/db"
	"github.com/augurlabs/aveloxis/internal/importers"
	"github.com/augurlabs/aveloxis/internal/importers/apache"
	"github.com/augurlabs/aveloxis/internal/importers/cncf"
	"github.com/augurlabs/aveloxis/internal/platform"
	"github.com/spf13/cobra"
)

// importFoundationsCmd registers `aveloxis import-foundations`.
//
// Behavior:
//   - Fetches CNCF landscape.yml (graduated / incubating / sandbox).
//   - Fetches Apache projects.json (graduated TLPs) + podlings.json (incubating).
//   - For each project: UpsertRepo + EnqueueRepo (same path as add-repo).
//   - Records foundation membership in aveloxis_ops.foundation_membership.
//   - If --dashboard-user is set, adds each repo to per-status user groups
//     on that operator's dashboard (e.g., "CNCF Graduated", "Apache Incubating").
func importFoundationsCmd(cfgPath *string) *cobra.Command {
	var (
		priority      int
		dryRun        bool
		cncfOnly      bool
		apacheOnly    bool
		dashboardUser string
		cncfURL       string
		apacheProjURL string
		apachePodURL  string
	)

	cmd := &cobra.Command{
		Use:   "import-foundations",
		Short: "Import CNCF and Apache foundation projects into the collection queue",
		Long: `Fetches the canonical project lists from the CNCF landscape
(cncf/landscape/landscape.yml) and Apache projects.json + podlings.json,
then enqueues every member repo for collection.

CNCF statuses covered: graduated, incubating, sandbox.
Apache statuses covered: graduated (TLPs), incubating (podlings).

Use --dashboard-user <gh-login> to also attach the imported repos to
foundation-named groups on that operator's web dashboard.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cncfOnly && apacheOnly {
				return fmt.Errorf("--cncf-only and --apache-only are mutually exclusive")
			}
			return runImportFoundations(*cfgPath, runOpts{
				priority:      priority,
				dryRun:        dryRun,
				cncfOnly:      cncfOnly,
				apacheOnly:    apacheOnly,
				dashboardUser: dashboardUser,
				cncfURL:       cncfURL,
				apacheProjURL: apacheProjURL,
				apachePodURL:  apachePodURL,
			})
		},
	}

	cmd.Flags().IntVar(&priority, "priority", 100, "queue priority (lower = collected sooner)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "print planned imports without writing to the database")
	cmd.Flags().BoolVar(&cncfOnly, "cncf-only", false, "import only CNCF projects")
	cmd.Flags().BoolVar(&apacheOnly, "apache-only", false, "import only Apache projects")
	cmd.Flags().StringVar(&dashboardUser, "dashboard-user", "",
		"GitHub login of an aveloxis web user — imported repos are also added to their dashboard groups (must have logged in once via OAuth)")
	cmd.Flags().StringVar(&cncfURL, "cncf-url", cncf.DefaultLandscapeURL, "override CNCF landscape.yml URL")
	cmd.Flags().StringVar(&apacheProjURL, "apache-projects-url", apache.DefaultProjectsURL, "override Apache projects.json URL")
	cmd.Flags().StringVar(&apachePodURL, "apache-podlings-url", apache.DefaultPodlingsURL, "override Apache podlings.json URL")

	return cmd
}

type runOpts struct {
	priority                       int
	dryRun, cncfOnly, apacheOnly   bool
	dashboardUser                  string
	cncfURL, apacheProjURL, apachePodURL string
}

func runImportFoundations(cfgPath string, opts runOpts) error {
	bootLog := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := loadConfig(cfgPath, bootLog)
	logger := newLogger(cfg)
	ctx := context.Background()

	store, err := db.NewPostgresStore(ctx, cfg.Database.ConnectionString(), logger)
	if err != nil {
		return fmt.Errorf("connecting to database: %w", err)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		return fmt.Errorf("migrating database: %w", err)
	}

	// Fetch both sources up front so we can report totals in one place.
	var projects []importers.Project
	if !opts.apacheOnly {
		logger.Info("fetching CNCF landscape", "url", opts.cncfURL)
		cncfProjects, ferr := cncf.Fetch(ctx, opts.cncfURL)
		if ferr != nil {
			return fmt.Errorf("fetching CNCF landscape: %w", ferr)
		}
		logger.Info("fetched CNCF projects", "count", len(cncfProjects))
		projects = append(projects, cncfProjects...)
	}
	if !opts.cncfOnly {
		logger.Info("fetching Apache projects", "projects_url", opts.apacheProjURL, "podlings_url", opts.apachePodURL)
		apacheProjects, ferr := apache.Fetch(ctx, opts.apacheProjURL, opts.apachePodURL)
		if ferr != nil {
			return fmt.Errorf("fetching Apache projects: %w", ferr)
		}
		logger.Info("fetched Apache projects", "count", len(apacheProjects))
		projects = append(projects, apacheProjects...)
	}

	// Resolve dashboard user up front if requested — we want to fail fast
	// rather than halfway through the import.
	var dashboardUserID int
	if opts.dashboardUser != "" && !opts.dryRun {
		id, err := store.GetUserIDByGHLogin(ctx, opts.dashboardUser)
		if err != nil {
			return err
		}
		dashboardUserID = id
	}

	// Pre-create dashboard groups so we only hit CreateUserGroup a handful
	// of times instead of once per repo. Key: "cncf:graduated" -> group_id.
	groupByKey := map[string]int64{}
	if dashboardUserID > 0 {
		for _, p := range projects {
			key := p.Foundation + ":" + p.Status
			if _, ok := groupByKey[key]; ok {
				continue
			}
			name := groupDisplayName(p.Foundation, p.Status)
			gid, err := store.CreateUserGroup(ctx, dashboardUserID, name)
			if err != nil {
				logger.Warn("failed to create dashboard group", "name", name, "error", err)
				continue
			}
			groupByKey[key] = gid
		}
	}

	// Tally per (foundation, status). Keys look like "cncf:graduated".
	tallies := map[string]*foundationTally{}
	getTally := func(p importers.Project) *foundationTally {
		key := p.Foundation + ":" + p.Status
		t := tallies[key]
		if t == nil {
			t = &foundationTally{}
			tallies[key] = t
		}
		return t
	}

	for _, p := range projects {
		t := getTally(p)
		t.projects++
		for _, rurl := range p.RepoURLs {
			t.repos++
			parsed, perr := platform.ParseRepoURL(rurl)
			if perr != nil {
				logger.Warn("skipping unparseable repo URL", "url", rurl, "project", p.Name, "error", perr)
				t.skipped++
				continue
			}
			if opts.dryRun {
				fmt.Printf("  [%s/%s] %s  %s\n", p.Foundation, p.Status, p.Name, rurl)
				continue
			}
			// Upsert + enqueue (same path add-repo uses).
			addOneRepo(ctx, store, logger, rurl, parsed.Owner, parsed.Repo, parsed.Platform, opts.priority)
			t.added++

			// Record foundation membership.
			if err := store.UpsertFoundationMembership(ctx, p.Foundation, p.Status, p.Name, p.Homepage, rurl); err != nil {
				logger.Warn("foundation_membership write failed", "project", p.Name, "repo", rurl, "error", err)
			}

			// Attach to dashboard group if requested.
			if dashboardUserID > 0 {
				if gid, ok := groupByKey[p.Foundation+":"+p.Status]; ok {
					repoID, rerr := store.FindRepoByURL(ctx, rurl)
					if rerr != nil || repoID == 0 {
						continue
					}
					if err := store.AddRepoToGroupByID(ctx, gid, repoID); err != nil {
						logger.Warn("failed to add repo to dashboard group", "repo", rurl, "error", err)
					}
				}
			}
		}
	}

	// Report.
	fmt.Println()
	fmt.Println("Import summary:")
	for _, key := range orderedKeys(tallies) {
		t := tallies[key]
		parts := strings.SplitN(key, ":", 2)
		foundation, status := parts[0], parts[1]
		fmt.Printf("  %-10s %-11s  projects=%-4d  repos=%-4d  queued=%-4d  skipped=%-4d\n",
			foundation, status, t.projects, t.repos, t.added, t.skipped)
	}
	if opts.dryRun {
		fmt.Println("\n(dry-run — nothing written to the database)")
	}
	return nil
}

// groupDisplayName returns the human-readable name used for dashboard groups,
// e.g. "CNCF Graduated", "Apache Incubating". Consistent with what appears
// in the /groups list in the web UI.
func groupDisplayName(foundation, status string) string {
	f := strings.ToUpper(foundation[:1]) + foundation[1:]
	if foundation == "cncf" {
		f = "CNCF"
	}
	s := strings.ToUpper(status[:1]) + status[1:]
	return f + " " + s
}

type foundationTally struct{ projects, repos, added, skipped int }

// orderedKeys returns tally keys in a stable display order:
// CNCF first (graduated → incubating → sandbox), then Apache (graduated → incubating).
func orderedKeys(m map[string]*foundationTally) []string {
	order := []string{
		"cncf:graduated", "cncf:incubating", "cncf:sandbox",
		"apache:graduated", "apache:incubating",
	}
	out := make([]string, 0, len(m))
	seen := map[string]bool{}
	for _, k := range order {
		if _, ok := m[k]; ok {
			out = append(out, k)
			seen[k] = true
		}
	}
	// Any unexpected keys get appended at the end.
	for k := range m {
		if !seen[k] {
			out = append(out, k)
		}
	}
	return out
}
