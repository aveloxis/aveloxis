// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// Local dev/build-deps canary harness (v0.27.48, summary/19 P5).
//
// Exercises the two summary/19 volume knobs against a curated repo
// set BEFORE any fleet enablement, using the REAL pipeline machinery
// for exactly the phases the knobs affect: full bare clone →
// AnalyzeRepo → ScanVulnerabilities. No GitHub API keys are needed —
// clones are anonymous, registry lookups and OSV are unauthenticated.
//
// Each repo runs TWICE: knobs OFF (baseline) then knobs ON
// (expanded). The analysis rotation replaces the snapshot between
// passes, so the printed delta IS the knob's contribution — rows by
// scope/ecosystem and vulnerability findings by scope.
//
// Gated on AVELOXIS_CANARY_DB (a postgres DSN for a DEDICATED scratch
// database — the harness migrates it and writes real rows; never
// point it at production). Optional AVELOXIS_CANARY_REPOS overrides
// the default profile set (comma-separated github URLs).
//
//	createdb aveloxis_canary19
//	AVELOXIS_CANARY_DB="postgres://aveloxis:...@localhost:5432/aveloxis_canary19?sslmode=prefer" \
//	  go test ./internal/collector/ -run TestLocalDevBuildDepsCanary -v -timeout 180m
//
// Deliberately NOT in the set: C/C++ (no libyear registry resolver
// exists — conan/vcpkg resolution is an honest, documented coverage
// gap, so a canary repo would only prove the absence); GitLab hosts
// (analysis is clone-based and platform-agnostic; Actions is
// GitHub-only by definition); a real-world PEP 735 adopter (the
// fixture test carries that family until adoption spreads).
//
// What this canary CANNOT measure (needs the chaoss.tv small-DB run):
// cross-repo OSV cache convergence under one long-lived serve process
// (each ScanVulnerabilities call here shares one in-process cache,
// but the fleet's convergence ratios need fleet-shaped input), and
// operator digest volume over real cadence.

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/model"
	"github.com/aveloxis/aveloxis/internal/platform"
)

// canaryProfileRepos is the default 15-repo set, chosen so every
// profile the knobs touch has at least one representative:
//
//	numpy         — THE case: [build-system].requires, requirement
//	                variants, test extras, ~25 workflow actions
//	flask         — [project.optional-dependencies]
//	requests      — setup.py/setup.cfg extras
//	pipenv        — Pipfile [dev-packages]
//	poetry        — [tool.poetry.group.*] groups
//	pytest        — Python + heavy workflow usage
//	viper/cobra   — Go C1 (testify imported only from _test.go)
//	react-router  — npm peerDependencies
//	serde         — Cargo [dev-dependencies]
//	rubocop       — Bundler group blocks
//	commons-lang  — Maven <scope>test</scope>
//	ecto          — Mix only: options
//	changed-files — the tj-actions repo itself (actions + the
//	                canonical GHSA self-advisory shape)
//	checkout      — an action repository (workflows all the way down)
var canaryProfileRepos = []string{
	"https://github.com/numpy/numpy",
	"https://github.com/pallets/flask",
	"https://github.com/psf/requests",
	"https://github.com/pypa/pipenv",
	"https://github.com/python-poetry/poetry",
	"https://github.com/pytest-dev/pytest",
	"https://github.com/spf13/viper",
	"https://github.com/spf13/cobra",
	"https://github.com/remix-run/react-router",
	"https://github.com/serde-rs/serde",
	"https://github.com/rubocop/rubocop",
	"https://github.com/apache/commons-lang",
	"https://github.com/elixir-ecto/ecto",
	"https://github.com/tj-actions/changed-files",
	"https://github.com/actions/checkout",
	// Coverage expansion (2026-07-21, operator ask): the seven
	// ecosystems the first wave left fixture-tested but never ran
	// through the full clone→analysis→scan path.
	"https://github.com/composer/composer",       // PHP: composer.json require-dev
	"https://github.com/junit-team/junit5",       // Gradle (.kts): test*-configuration dev stamps
	"https://github.com/Alamofire/Alamofire",     // Swift: Package.swift → swiftpm resolver
	"https://github.com/dart-lang/http",          // Dart: pubspec dev_dependencies → pub.dev
	"https://github.com/sbt/sbt",                 // Scala: build.sbt % Test/Provided (deps split across .scala files are a KNOWN line-parser limit — the canary shows what's actually caught)
	"https://github.com/JamesNK/Newtonsoft.Json", // .NET: csproj PackageReference → nuget
	"https://github.com/commercialhaskell/stack", // Haskell: package.yaml (hpack) tests: sections → hackage
}

// phase 3 (AVELOXIS_CANARY_TRANSITIVE=1): expanded + transitive —
// the "everything on" future state (summary/19 knobs + the v0.27.21
// Phase C lockfile-closure knob). Kept OUT of the default run so the
// base report's delta attributes cleanly to the two summary/19 knobs
// against production's actual transitive=off baseline.

type canaryCounts struct {
	rowsByScope     map[string]int
	rowsByManager   map[string]int
	findsByScope    map[string]int
	findsSelf       int
	findsTotal      int
	findsTransitive int
}

func TestLocalDevBuildDepsCanary(t *testing.T) {
	dsn := os.Getenv("AVELOXIS_CANARY_DB")
	if dsn == "" {
		t.Skip("local canary harness; set AVELOXIS_CANARY_DB to a DEDICATED scratch DB to run")
	}
	repoList := canaryProfileRepos
	if env := os.Getenv("AVELOXIS_CANARY_REPOS"); env != "" {
		repoList = strings.Split(env, ",")
	}

	ctx := context.Background()
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	store, err := db.NewPostgresStore(ctx, dsn, logger)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(store.Close)
	testMigrate(ctx, t, store)

	bareDir := filepath.Join(os.TempDir(), "aveloxis-canary19-bare")
	if err := os.MkdirAll(bareDir, 0o755); err != nil {
		t.Fatal(err)
	}

	cache := NewOSVCache()
	withTransitive := os.Getenv("AVELOXIS_CANARY_TRANSITIVE") == "1"
	type repoReport struct {
		slug               string
		baseline, expanded canaryCounts
		expandedTransitive canaryCounts
		scanErr            string
	}
	var reports []repoReport

	for _, url := range repoList {
		url = strings.TrimSpace(url)
		parsed, perr := platform.ParseAnyRepoURL(url)
		if perr != nil {
			t.Errorf("parse %s: %v", url, perr)
			continue
		}
		slug := parsed.Owner + "/" + parsed.Repo
		repoID, uerr := store.UpsertRepo(ctx, &model.Repo{
			Platform: model.PlatformGitHub, GitURL: url,
			Owner: parsed.Owner, Name: parsed.Repo,
		})
		if uerr != nil {
			t.Errorf("upsert %s: %v", slug, uerr)
			continue
		}

		barePath := filepath.Join(bareDir, fmt.Sprintf("repo_%d", repoID))
		if _, serr := os.Stat(filepath.Join(barePath, "HEAD")); serr != nil {
			fmt.Printf("== cloning %s (bare, full) ...\n", slug)
			cloneCtx, cancel := context.WithTimeout(ctx, 30*time.Minute)
			cmd := exec.CommandContext(cloneCtx, "git", "clone", "--bare", "--", url, barePath)
			cmd.Env = append(os.Environ(), "GIT_LFS_SKIP_SMUDGE=1", "GIT_TERMINAL_PROMPT=0")
			out, cerr := cmd.CombinedOutput()
			cancel()
			if cerr != nil {
				t.Errorf("clone %s: %v: %s", slug, cerr, string(out))
				continue
			}
		}

		type canaryPhase struct {
			name                 string
			expanded, transitive bool
		}
		phases := []canaryPhase{{"baseline", false, false}, {"expanded", true, false}}
		if withTransitive {
			phases = append(phases, canaryPhase{"exp+trans", true, true})
		}
		rep := repoReport{slug: slug}
		for _, ph := range phases {
			ac := NewAnalysisCollector(store, logger, bareDir)
			ac.DevBuildDeps = ph.expanded
			ac.GitHubActionsDeps = ph.expanded
			ac.TransitiveLockfiles = ph.transitive
			if _, aerr := ac.AnalyzeRepo(ctx, repoID); aerr != nil {
				t.Errorf("analyze %s (%s): %v", slug, ph.name, aerr)
				break
			}
			res, verr := ScanVulnerabilities(ctx, store, repoID, logger, cache, ph.transitive)
			if verr != nil {
				// Scan failure is data too (OSV hiccup) — record, keep going.
				rep.scanErr = verr.Error()
			}
			counts, qerr := canaryReadout(ctx, store, repoID)
			if qerr != nil {
				t.Errorf("readout %s: %v", slug, qerr)
				break
			}
			switch ph.name {
			case "baseline":
				rep.baseline = counts
			case "expanded":
				rep.expanded = counts
			default:
				rep.expandedTransitive = counts
			}
			if res != nil {
				fmt.Printf("   %s %-9s rows=%v findings=%d (transitive=%d) osv_q=%d/%d osv_d=%d/%d\n",
					slug, ph.name, counts.rowsByScope, counts.findsTotal, counts.findsTransitive,
					res.OSVQueryHits, res.OSVQueryMisses, res.OSVDetailHits, res.OSVDetailMisses)
			}
		}
		reports = append(reports, rep)
	}

	// ── The report ──
	fmt.Println("\n================ summary/19 LOCAL CANARY REPORT ================")
	fmt.Printf("%-28s %-30s %-30s %s\n", "repo", "rows base→expanded (by scope)", "findings base→expanded (scope)", "actions")
	totalBaseRows, totalExpRows, totalBaseFinds, totalExpFinds := 0, 0, 0, 0
	for _, r := range reports {
		bRows, eRows := sumMap(r.baseline.rowsByScope), sumMap(r.expanded.rowsByScope)
		totalBaseRows += bRows
		totalExpRows += eRows
		totalBaseFinds += r.baseline.findsTotal
		totalExpFinds += r.expanded.findsTotal
		fmt.Printf("%-28s %4d→%-4d %-20s %3d→%-3d %-20s %d\n",
			r.slug, bRows, eRows, fmtScopeDelta(r.baseline.rowsByScope, r.expanded.rowsByScope),
			r.baseline.findsTotal, r.expanded.findsTotal,
			fmtScopeDelta(r.baseline.findsByScope, r.expanded.findsByScope),
			r.expanded.rowsByManager["githubactions"])
		if r.scanErr != "" {
			fmt.Printf("%-28s   scan error: %s\n", "", r.scanErr)
		}
	}
	fmt.Printf("\nTOTALS: dep rows %d → %d (+%.0f%%), findings %d → %d\n",
		totalBaseRows, totalExpRows, pctGrowth(totalBaseRows, totalExpRows),
		totalBaseFinds, totalExpFinds)
	if withTransitive {
		fmt.Println("\n-- phase 3 (expanded + vuln_scan_transitive) --")
		for _, r := range reports {
			fmt.Printf("%-28s findings %d → %d (%d transitive) %s\n",
				r.slug, r.expanded.findsTotal, r.expandedTransitive.findsTotal,
				r.expandedTransitive.findsTransitive,
				fmtScopeDelta(r.expanded.findsByScope, r.expandedTransitive.findsByScope))
		}
	}
	fmt.Println("(dev-scope findings are digest-gated by default: mail.vuln_digest_include_dev=false)")
}

func canaryReadout(ctx context.Context, store *db.PostgresStore, repoID int64) (canaryCounts, error) {
	c := canaryCounts{rowsByScope: map[string]int{}, rowsByManager: map[string]int{}, findsByScope: map[string]int{}}
	rows, err := store.Pool().Query(ctx, `
		SELECT COALESCE(NULLIF(type,''),'runtime'), package_manager, COUNT(*)
		FROM aveloxis_data.repo_deps_libyear WHERE repo_id = $1 GROUP BY 1, 2`, repoID)
	if err != nil {
		return c, err
	}
	for rows.Next() {
		var scope, mgr string
		var n int
		if err := rows.Scan(&scope, &mgr, &n); err != nil {
			rows.Close()
			return c, err
		}
		c.rowsByScope[scope] += n
		c.rowsByManager[mgr] += n
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return c, err
	}

	frows, err := store.Pool().Query(ctx, `
		SELECT COALESCE(NULLIF(dependency_scope,''),'runtime'),
		       COALESCE(dependency_kind,''), COUNT(*)
		FROM aveloxis_data.repo_deps_vulnerabilities
		WHERE repo_id = $1 AND resolved_at IS NULL GROUP BY 1, 2`, repoID)
	if err != nil {
		return c, err
	}
	defer frows.Close()
	for frows.Next() {
		var scope, kind string
		var n int
		if err := frows.Scan(&scope, &kind, &n); err != nil {
			return c, err
		}
		if kind == "self" {
			c.findsSelf += n
			continue
		}
		if kind == "transitive" {
			c.findsTransitive += n
		}
		c.findsByScope[scope] += n
		c.findsTotal += n
	}
	return c, frows.Err()
}

func sumMap(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

func pctGrowth(base, exp int) float64 {
	if base == 0 {
		return 0
	}
	return 100 * float64(exp-base) / float64(base)
}

func fmtScopeDelta(base, exp map[string]int) string {
	keys := map[string]bool{}
	for k := range base {
		keys[k] = true
	}
	for k := range exp {
		keys[k] = true
	}
	var names []string
	for k := range keys {
		names = append(names, k)
	}
	sort.Strings(names)
	var parts []string
	for _, k := range names {
		if base[k] == exp[k] {
			parts = append(parts, fmt.Sprintf("%s:%d", k, exp[k]))
		} else {
			parts = append(parts, fmt.Sprintf("%s:%d→%d", k, base[k], exp[k]))
		}
	}
	return strings.Join(parts, " ")
}
