// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package db

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/aveloxis/aveloxis/internal/srctest"
)

// The repository-page cache's invalidation contract, enforced (v0.29.73).
//
// The API keeps a repository's "exact" answers — vulnerabilities, scorecard,
// scancode, licenses, dependencies, libyear, SBOM — until RepoCacheState
// moves; they have no time limit. So every function that writes a table
// those answers read must be covered by a stamp, one of:
//
//   - stamps:  it stamps data_changed_at itself, in the same transaction or
//     statement as the data (stampRepoCacheStateSQL in its body);
//   - helper:  it runs only inside another function that stamps;
//   - job:     it runs only inside a queued collection job (the analysis
//     phase, constructed only by the scheduler), whose CompleteJob moves
//     the queue row at the end;
//   - migrate: it runs during `aveloxis migrate`, after which the API
//     restarts on a binary whose version is in every cache key.
//
// A new writer of these tables fails this test until it is classified here.
// The answers bounded by time — the time series and contributor answers,
// keyed by the UTC day or aged by the enrichment interval — are not listed:
// their staleness is bounded without a stamp.

// pageCacheTables are the tables the exact answers read.
var pageCacheTables = []string{
	"repo_deps_vulnerabilities", "repo_deps_scorecard",
	"scancode_file_results", "scancode_scans",
	"repo_dependencies", "repo_deps_libyear",
	"repo_lockfiles", "repo_lockfile_packages", "repo_lockfile_edges",
	// The SBOM's license (GetRepoForSBOM reads the latest repo_info row).
	"repo_info",
}

type pageWriterCoverage struct {
	how    string // "stamps", "helper", "job", "migrate"
	reason string
}

var pageCacheWriters = map[string]pageWriterCoverage{
	"InsertVulnerabilityBatch":         {"stamps", "a vulnerability scan (the job's, heal-vulnerabilities)"},
	"MarkStaleVulnerabilitiesResolved": {"stamps", "a vulnerability scan; stamps only when rows were resolved"},
	"UpdateCVSSScoreForVector":         {"stamps", "heal-vulnerabilities --rescore-only"},
	"ReplaceScorecard":                 {"stamps", "the job's scorecard phase and run-scorecard"},
	"ReplaceScancodeSnapshot":          {"stamps", "the decoupled scancode worker"},
	"HealUnknownLibyear":               {"stamps", "heal-libyear --apply"},
	"rotateScancodeRows":               {"helper", "ReplaceScancodeSnapshot"},
	"ClearRepoDependencies":            {"job", "analysis phase"},
	"InsertRepoDependency":             {"job", "analysis phase"},
	"InsertRepoDependencyBatch":        {"job", "analysis phase"},
	"RotateLibyearToHistory":           {"job", "analysis phase"},
	"InsertRepoLibyear":                {"job", "analysis phase"},
	"InsertRepoLibyearBatch":           {"job", "analysis phase"},
	"ReplaceRepoLockfileSnapshot":      {"job", "analysis phase (lockfile scan)"},
	"migrateStage10RecentReleases":     {"migrate", "a migration step"},
	// The staged processor reaches these outside any job too (heal-messages,
	// heal-collection-gaps, the leftover drain), so they stamp themselves.
	"RotateRepoInfoToHistory":   {"stamps", "staged repo_info processing, in or out of a job"},
	"InsertRepoInfo":            {"stamps", "staged repo_info processing, in or out of a job"},
	"BackfillGitLabCommitCount": {"job", "the scheduler's facade phase (commit_count, which no cached answer reads)"},
}

// jobCallers says where each job-classified writer may be called from, per
// writer (whole-branch review: one shared file list let every job writer be
// called from anywhere in scheduler.go, sweeps and lock release included).
// "file" allows the whole file — the analysis collector, which only the
// scheduler's job constructs (checked below); "file|func" allows one
// function.
var jobCallers = map[string][]string{
	"analysis":                  {"internal/collector/analysis.go", "internal/collector/lockfile_scan.go"},
	"BackfillGitLabCommitCount": {"internal/scheduler/scheduler.go|runFacadeAndAnalysis"},
}

// jobCallersOf is the allowance for writer fn: its own entry, else the
// analysis collector's.
func jobCallersOf(fn string) []string {
	if c, ok := jobCallers[fn]; ok {
		return c
	}
	return jobCallers["analysis"]
}

func TestPageCacheTableWritersAreCovered(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 20)
	tableRE := regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:aveloxis_\w+\.)?(` + strings.Join(pageCacheTables, "|") + `)\b`)
	nameRE := regexp.MustCompile(`^(?:\([^)]*\)\s*)?(\w+)\(`)
	bodies := map[string]string{}
	found := map[string]bool{}
	for name, src := range files {
		src = srctest.StripGoComments(src)
		for _, part := range strings.Split(src, "\nfunc ")[1:] {
			m := nameRE.FindStringSubmatch(part)
			if m == nil {
				continue
			}
			fn := m[1]
			bodies[fn] = part
			if tableRE.MatchString(part) {
				found[fn] = true
				if _, ok := pageCacheWriters[fn]; !ok {
					t.Errorf("%s: %s writes a table the repository page's exact answers read, and is not classified in pageCacheWriters — stamp data_changed_at in the same transaction (stampRepoCacheStateSQL), or classify it with the reason it is covered",
						name, fn)
				}
			}
		}
	}
	srctest.MinCount(t, "writers of the page-cache tables", len(found), 12)
	for fn, cov := range pageCacheWriters {
		if !found[fn] {
			t.Errorf("pageCacheWriters lists %s, which no longer writes a page-cache table — remove it", fn)
			continue
		}
		switch cov.how {
		case "stamps":
			if msg := stampPlacement(bodies[fn]); msg != "" {
				t.Errorf("%s is classified as stamping but %s", fn, msg)
			}
		case "helper":
			if !strings.Contains(bodies[cov.reason], fn+"(") || !strings.Contains(bodies[cov.reason], "stampRepoCacheStateSQL") {
				t.Errorf("%s is classified as a helper of %s, which must call it and stamp", fn, cov.reason)
			}
		case "job", "migrate":
		default:
			t.Errorf("%s: unknown coverage %q", fn, cov.how)
		}
	}

	// The job classification holds only while these writers are called from
	// the analysis collector alone, and that collector is constructed only by
	// the scheduler (the queued job).
	root := srctest.Root(t)
	var jobWriters []string
	for fn, cov := range pageCacheWriters {
		if cov.how == "job" {
			jobWriters = append(jobWriters, fn)
		}
	}
	callRE := regexp.MustCompile(`\.(` + strings.Join(jobWriters, "|") + `)\(`)
	funcName := regexp.MustCompile(`^(?:\([^)]*\)\s*)?(\w+)[\[(]`)
	ctorRE := regexp.MustCompile(`\bNewAnalysisCollector\(`)
	examined := 0
	for _, dir := range []string{"internal", "cmd", "scripts"} {
		_ = filepath.WalkDir(filepath.Join(root, dir), func(path string, d os.DirEntry, err error) error {
			if err != nil || d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if strings.HasPrefix(rel, "internal/db/") {
				return nil
			}
			b, rerr := os.ReadFile(path)
			if rerr != nil {
				return rerr
			}
			src := srctest.StripGoComments(string(b))
			examined++
			for _, part := range strings.Split(src, "\nfunc ") {
				enclosing := ""
				if m := funcName.FindStringSubmatch(part); m != nil {
					enclosing = m[1]
				}
				for _, m := range callRE.FindAllStringSubmatch(part, -1) {
					ok := false
					for _, a := range jobCallersOf(m[1]) {
						if a == rel || a == rel+"|"+enclosing {
							ok = true
						}
					}
					if !ok {
						t.Errorf("%s (%s) calls %s — a job-classified page-cache writer outside its allowed job phase (jobCallers): stamp, or reclassify", rel, enclosing, m[1])
					}
				}
			}
			if ctorRE.MatchString(src) && rel != "internal/scheduler/scheduler.go" && rel != "internal/collector/analysis.go" {
				t.Errorf("%s constructs the analysis collector outside the scheduler's job: its writes would not be covered by CompleteJob", rel)
			}
			return nil
		})
	}
	srctest.MinCount(t, "non-db Go files examined", examined, 50)
}

// stampPlacement checks a stamping writer's body for a stamp in the SAME
// transaction or statement as its data (whole-branch review: only the
// token was checked, and a stamp moved after the commit — the shape PR
// review 5402109493 removed — passed). Accepted: the stamp inlined into the
// writer's SQL (a data-modifying CTE), or a tx.Exec / batch.Queue of it
// before the body's Commit. A stamp on the pool is a separate statement
// that can fail after the data commits. "" means placed correctly.
func stampPlacement(body string) string {
	// The stamp as a statement of its own on the pool (inlined into a pool
	// statement's SQL is that statement, judged below).
	if regexp.MustCompile(`\.pool\.\w+\(ctx,\s*stampRepoCacheStateSQL`).MatchString(body) {
		return "it stamps on the pool, outside its data's transaction"
	}
	commit := strings.Index(body, ".Commit(")
	inTx := strings.Contains(body, ".Begin(")
	call := regexp.MustCompile(`\b(tx|pool)\.(?:Exec|Query|QueryRow)\(`)
	if inl := regexp.MustCompile("`\\s*\\+\\s*stampRepoCacheStateSQL\\s*\\+\\s*`").FindStringIndex(body); inl != nil {
		// Inlined into a statement: the writer's own one-statement write,
		// or, beside a transaction, a statement of that transaction before
		// its Commit (whole-branch review: an inlined stamp on the pool
		// after the Commit was accepted).
		if inTx {
			calls := call.FindAllStringSubmatchIndex(body[:inl[0]], -1)
			if len(calls) == 0 || body[calls[len(calls)-1][2]:calls[len(calls)-1][3]] != "tx" {
				return "its stamp is inlined into a statement outside its transaction"
			}
			if commit >= 0 && commit < inl[0] {
				return "its stamp comes after the transaction's Commit"
			}
		}
		return ""
	}
	loc := regexp.MustCompile(`(?:\btx\.Exec\(ctx,\s*|\bbatch\.Queue\()stampRepoCacheStateSQL`).FindStringIndex(body)
	if loc == nil {
		return "its body has no stamp in its transaction (tx.Exec or batch.Queue of stampRepoCacheStateSQL) nor in its statement"
	}
	if commit < 0 {
		return "its stamp is in no committed transaction"
	}
	if commit < loc[0] {
		return "its stamp comes after the transaction's Commit"
	}
	return ""
}

func TestStampPlacement(t *testing.T) {
	for body, ok := range map[string]bool{
		"tx.Exec(ctx, stampRepoCacheStateSQL+` WHERE`)\n tx.Commit(ctx)":                                                                                                  true,
		"batch.Queue(stampRepoCacheStateSQL+` WHERE`)\n br := tx.SendBatch(); tx.Commit(ctx)":                                                                             true,
		"q := `WITH a AS (...), s AS (` + stampRepoCacheStateSQL + ` WHERE ...)`":                                                                                         true,
		"tx.Commit(ctx)\n tx.Exec(ctx, stampRepoCacheStateSQL+` WHERE`)":                                                                                                  false,
		"tx.Commit(ctx)\n s.pool.Exec(ctx, stampRepoCacheStateSQL+` WHERE`)":                                                                                              false,
		"s.pool.Exec(ctx, stampRepoCacheStateSQL+` WHERE`)":                                                                                                               false,
		"s.pool.Exec(ctx, `WITH r AS (UPDATE ...), s AS (`+stampRepoCacheStateSQL+` WHERE ...)`)":                                                                         true,
		"tx, _ := s.pool.Begin(ctx)\n tx.Exec(ctx, `UPDATE x`)\n tx.Commit(ctx)\n s.pool.Exec(ctx, `WITH n AS (SELECT 1) `+stampRepoCacheStateSQL+` WHERE repo_id = $1`)": false,
		"tx, _ := s.pool.Begin(ctx)\n s.pool.Exec(ctx, `WITH n AS (SELECT 1) `+stampRepoCacheStateSQL+` WHERE x`)\n tx.Commit(ctx)":                                       false,
		"tx, _ := s.pool.Begin(ctx)\n tx.Exec(ctx, `WITH d AS (DELETE ...) `+stampRepoCacheStateSQL+` WHERE x`)\n tx.Commit(ctx)":                                         true,
		"tx.Exec(ctx, stampRepoCacheStateSQL+` WHERE`)":                                                                                                                   false,
		"x := stampRepoCacheStateSQL": false,
	} {
		if got := stampPlacement(body) == ""; got != ok {
			t.Errorf("stampPlacement(%q) accepted=%t, want %t (%s)", body, got, ok, stampPlacement(body))
		}
	}
}
