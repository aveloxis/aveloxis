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
}

// jobPhaseFiles are where the job-classified writers may be called from: the
// analysis collector, which only the scheduler's job constructs (checked
// below).
var jobPhaseFiles = []string{"internal/collector/analysis.go", "internal/collector/lockfile_scan.go"}

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
			if !strings.Contains(bodies[fn], "stampRepoCacheStateSQL") {
				t.Errorf("%s is classified as stamping but its body does not use stampRepoCacheStateSQL", fn)
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
	ctorRE := regexp.MustCompile(`\bNewAnalysisCollector\(`)
	allowed := map[string]bool{}
	for _, f := range jobPhaseFiles {
		allowed[f] = true
	}
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
			if m := callRE.FindString(src); m != "" && !allowed[rel] {
				t.Errorf("%s calls %s — a job-classified page-cache writer outside the analysis phase: stamp, or reclassify", rel, m)
			}
			if ctorRE.MatchString(src) && rel != "internal/scheduler/scheduler.go" && rel != "internal/collector/analysis.go" {
				t.Errorf("%s constructs the analysis collector outside the scheduler's job: its writes would not be covered by CompleteJob", rel)
			}
			return nil
		})
	}
	srctest.MinCount(t, "non-db Go files examined", examined, 50)
}
