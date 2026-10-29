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

// The repository-page cache's invalidation contract, enforced (v0.29.73;
// every cached answer since 0.29.120).
//
// The API keeps a repository's answers until RepoCacheState moves; none has a
// time limit (D6, 0.29.120, summary/63: the time series, metric series and
// contributor answers were keyed by the UTC day or aged by the enrichment
// interval before, and were left out of this registry for it). So every
// function that writes a table a cached answer reads must be covered, one of:
//
//   - stamps:    it stamps data_changed_at itself, in the same transaction or
//     statement as the data (stampRepoCacheStateSQL in its body, or the
//     column set inline on the same repos row);
//   - helper:    it runs only inside another function that stamps;
//   - job:       it runs only inside a queued collection job (the analysis
//     and facade phases, constructed only by the scheduler, or by the
//     one-shot collect, which stamps), whose CompleteJob moves the queue row
//     at the end;
//   - processor: it is written only by the staged processor (staged.go),
//     whose every ProcessRepo caller is covered
//     (TestProcessRepoCallersAreCovered);
//   - drain:     it runs only in the mailing-list or Jira drain (which
//     stamps through refreshRepoActivity) or at a listed site that stamps;
//   - migrate:   it runs during `aveloxis migrate`, after which the API
//     restarts on a binary whose version is in every cache key;
//   - fleet:     a fleet-wide identity operation (contributor rows, merges,
//     identity backfills): DECIDED to wait for each repository's next
//     collection (operator, 2026-10-28, D6/D7 — a per-contributor stamp
//     fans out across the fleet);
//   - unread:    it writes only columns no cached answer reads (named in the
//     reason).
//
// A new writer of these tables fails this test until it is classified here.

// pageCacheTables are the tables the exact answers read.
var pageCacheTables = []string{
	"repo_deps_vulnerabilities", "repo_deps_scorecard",
	"scancode_file_results", "scancode_scans",
	"repo_dependencies", "repo_deps_libyear",
	"repo_lockfiles", "repo_lockfile_packages", "repo_lockfile_edges",
	// The SBOM's license (GetRepoForSBOM reads the latest repo_info row).
	"repo_info",
	// The traffic answer (v0.29.118): the daily clones and the five
	// GitHub traffic tables, all stored history, no time bound.
	"repo_clones", "repo_views", "repo_traffic_paths",
	"repo_traffic_referrers", "repo_traffic_windows", "repo_traffic_weeks",
	// The windowed and contributor answers (0.29.120, D6): the time series,
	// the metric series, top contributors and /contributions/*.
	"commits", "issues", "pull_requests", "messages", "issue_events", "pull_request_events",
	"pull_request_reviews", "issue_message_ref", "pull_request_message_ref",
	"contributors", "contributor_affiliations", "repo_commit_daily",
}

// repoColumnWriteRE matches a SET of one of the read repos columns in an
// UPDATE of repos (after withoutSQLComments: a ";" inside an SQL comment
// ended the match early and hid UpdateRepoMetadata's created_at write, round
// 1 finding 1 of 0.29.123), as `col =` or inside a multiple-column
// assignment's list, `SET (a, col) = (...)` (round 2 finding 3).
var repoColumnWriteRE = regexp.MustCompile("(?is)UPDATE\\s+(?:aveloxis_data\\.)?repos\\b[^;`]*?(?:\\bSET\\b[^;`]*?\\b(?:" + strings.Join(pageCacheRepoColumns, "|") + ")\\s*=|(?:\\bSET\\s*|,\\s*)\\([^()]*\\b(?:" + strings.Join(pageCacheRepoColumns, "|") + ")\\b[^()]*\\)\\s*=)")

// withoutSQLComments drops SQL line comments ("--" to the end of the line)
// from a function's source before the writer scan reads its statements.
func withoutSQLComments(src string) string {
	return regexp.MustCompile(`--[^\n]*`).ReplaceAllString(src, "")
}

// pageCacheRepoColumns are the repos columns a cached answer reads other than
// the identity, which the fingerprint itself carries: the first-activity
// floor and the daily table's completeness switch. Matched as a SET of an
// UPDATE of repos (the table itself has many unrelated writers).
var pageCacheRepoColumns = []string{"first_commit_at", "commit_daily_complete_at", "created_at"}

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
	"RotateLibyearToHistory":           {"job", "analysis phase"},
	"InsertRepoLibyear":                {"job", "analysis phase"},
	"ReplaceRepoLockfileSnapshot":      {"job", "analysis phase (lockfile scan)"},
	"migrateStage10RecentReleases":     {"migrate", "a migration step"},
	// The staged processor reaches these outside any job too (heal-messages,
	// heal-collection-gaps, the leftover drain), so they stamp themselves.
	"RotateRepoInfoToHistory":   {"stamps", "staged repo_info processing, in or out of a job"},
	"InsertRepoInfo":            {"stamps", "staged repo_info processing, in or out of a job"},
	"UpsertRepoClone":           {"stamps", "staged clone stats processing, in or out of a job"},
	"UpsertRepoClones":          {"stamps", "staged clone stats processing, in or out of a job"},
	"UpsertRepoTraffic":         {"stamps", "staged traffic processing, in or out of a job"},
	"BackfillGitLabCommitCount": {"job", "the scheduler's facade phase (commit_count, which no cached answer reads)"},

	// 0.29.120 (D6/D7): the tables of the windowed and contributor answers.
	"RecordCommitBounds":                {"stamps", "the facade and the gone-recheck sweep: data_changed_at in the same UPDATE, when a bound moves"},
	"UpdateRepoMetadata":                {"stamps", "Phase 0, the metadata backfill sweep and backfill-repo-metadata: data_changed_at in the same UPDATE, when it fills created_at"},
	"FillRepoCommitDailyFromCommits":    {"stamps", "heal-commit-daily, in its transaction"},
	"dedupOnePair":                      {"stamps", "dedup-repos / reconcile-repos: the winner, in the merge's transaction"},
	"UpsertCommit":                      {"job", "the facade phase (the job, or collect, which stamps)"},
	"UpsertCommitBatch":                 {"job", "the facade phase"},
	"ReplaceRepoCommitDaily":            {"job", "the facade phase"},
	"InvalidateCommitDailyComplete":     {"job", "the facade phase"},
	"BackfillCommitAuthorIDs":           {"job", "commit resolution (the job, or collect)"},
	"UpsertIssue":                       {"processor", "staged issues"},
	"UpsertPullRequest":                 {"processor", "staged pull requests"},
	"UpsertPRReview":                    {"processor", "staged reviews"},
	"UpsertIssueEvent":                  {"processor", "staged issue events"},
	"UpsertPREvent":                     {"processor", "staged pull request events"},
	"UpsertMessageBatch":                {"processor", "staged messages and their bridges"},
	"DeriveIssueClosedByFromEvents":     {"processor", "staged issue closers"},
	"DeleteStaleConversationRefs":       {"drain", "heal-messages, which stamps its batch's repositories"},
	"ApplyTrackerAction":                {"drain", "the mailing-list drain; backfill-mailing-list-projection (BackfillKeyedIssueProjection stamps its batch)"},
	"LinkOrCreateIssueFromEmail":        {"drain", "the mailing-list drain; the projection backfill (stamps its batch)"},
	"UpsertJiraComment":                 {"drain", "the Jira drain"},
	"UpsertJiraIssueFromAPI":            {"drain", "the Jira drain; backfill-jira-identities (stamps each registration's repository)"},
	"UpsertMailingListMessageBody":      {"drain", "the mailing-list drain"},
	"bridgeEmailToIssueNoRecount":       {"drain", "UpsertJiraComment and BridgeEmailToIssue (the mailing-list drain, the projection backfill)"},
	"BackfillSyntheticJiraState":        {"migrate", "stage 10"},
	"HealAutomationPhantomContributors": {"migrate", "a migration step"},
	"backfillMsgKinds":                  {"migrate", "stage 1"},
	"dedupLinkedMsgIDsTx":               {"migrate", "stage 9"},
	"deduplicateCommits":                {"migrate", "stage 4"},
	"migrateStage6CountBackfills":       {"migrate", "stage 6"},
	"migrateStage9DataQuality":          {"migrate", "stage 9"},
	"BackfillClosedByFromEvents":        {"fleet", "backfill-identities: issue closers across the fleet"},
	"BackfillMailingListSenderIDs":      {"fleet", "the sender-resolution sweep across the fleet"},
	"SetIssueClosedBy":                  {"fleet", "backfill-identities' closer sweep across the fleet"},
	"CreateEmailOnlyContributor":        {"fleet", "contributor rows"},
	"LinkContributorToGitHubUser":       {"fleet", "contributor rows (the search-resolve sweep)"},
	"MarkBreadthAttemptedBatch":         {"fleet", "contributor bookkeeping the coverage answer reads"},
	"MarkContributorEnriched":           {"fleet", "contributor bookkeeping the coverage answer reads (the enrichment sweep)"},
	"MarkContributorSearchAttempted":    {"fleet", "contributor bookkeeping the coverage answer reads"},
	"MigrateCntrbIDsBatch":              {"fleet", "migrate-cntrb-ids"},
	"MintJiraContributor":               {"fleet", "contributor rows (the Jira drain and its backfill)"},
	"PopulateAffiliations":              {"fleet", "contributor affiliations (the affiliations sweep)"},
	"RenameContributorGhLogin":          {"fleet", "contributor rows (the breadth sweep)"},
	"Resolve":                           {"fleet", "contributor rows (the staged processor's resolver, backfill-identities)"},
	"SetContributorCanonical":           {"fleet", "contributor rows"},
	"UpdateContributorActivityBatch":    {"fleet", "contributor activity class (the activity sweep)"},
	"UpsertContributorFull":             {"fleet", "contributor rows"},
	"backfillDenormalizedIdentity":      {"fleet", "contributor rows"},
	"backfillGHColumns":                 {"fleet", "contributor rows"},
	"mergeOnePair":                      {"fleet", "merge-cntrb-collisions"},
	"upsertOneContributor":              {"fleet", "contributor rows"},
	"BackfillIssueExternalKeys":         {"unread", "issues.external_key"},
	"MarkActivityCheckedBatch":          {"unread", "contributors.gh_activity_checked_at"},
	"MarkHistoryBackfilled":             {"unread", "contributors.gh_history_backfilled_at, gh_history_failed_at"},
	"MarkHistoryFetchFailed":            {"unread", "contributors.gh_history_failed_at"},
	"SetCommitAuthorLogin":              {"unread", "commits.cmt_author_platform_username"},
	"SetPRMetaLinks":                    {"unread", "pull_requests.meta_head_id, meta_base_id"},
	"StoreContributorActivityHistory":   {"unread", "contributor_activity_days and the gh_history_* columns"},
	"UpdateCommitWhitespaceBatch":       {"unread", "commits.cmt_added, cmt_removed, cmt_whitespace"},
	"UpdateMessageCleanBatch":           {"unread", "messages.msg_text_clean, msg_text_clean_rule"},
	"enrichNativeJiraIssue":             {"unread", "issues.jira_issue_id, external_key"},
	"recountIssueComments":              {"unread", "issues.comment_count"},
}

// jobCallers says where each job-, processor- or drain-classified writer may
// be called from outside internal/db, per writer (whole-branch review: one
// shared file list let every job writer be called from anywhere in
// scheduler.go, sweeps and lock release included), and how each site is
// covered: "phase" — a site of a phase the scheduler's job builds (the
// analysis collector, checked below) or whose constructor only covered
// callers build (builtBy) — or "stamps", a site whose own body stamps the
// repository it writes (StampReposChangedOutsideJob). A key is a file (the
// analysis collector's, kept from v0.29.73) or srctest.SiteKey(dir, func),
// one function wherever it lives in that package (v0.29.98). A writer is
// matched wherever it is referenced, a method value included
// (f.store.ReplaceRepoCommitDaily is taken, then called).
var jobCallers = map[string]map[string]string{
	"analysis":                      {"internal/collector/analysis.go": "phase", "internal/collector/lockfile_scan.go": "phase"},
	"BackfillGitLabCommitCount":     {srctest.SiteKey("internal/scheduler", "runFacadeAndAnalysis"): "phase"},
	"UpsertCommit":                  {srctest.SiteKey("internal/collector", "upsertCommitRowsFallback"): "phase"},
	"UpsertCommitBatch":             {srctest.SiteKey("internal/collector", "insertCommitBatch"): "phase"},
	"ReplaceRepoCommitDaily":        {srctest.SiteKey("internal/collector", "recordCommitDaily"): "phase"},
	"InvalidateCommitDailyComplete": {srctest.SiteKey("internal/collector", "invalidateUnrecordedDaily"): "phase"},
	"BackfillCommitAuthorIDs":       {srctest.SiteKey("internal/collector", "ResolveCommits"): "phase"},
	// The staged processor's writer sites (its ProcessRepo callers:
	// TestProcessRepoCallersAreCovered).
	"processor": {
		srctest.SiteKey("internal/collector", "ProcessRepo"):             "phase",
		srctest.SiteKey("internal/collector", "processStagedIssue"):      "phase",
		srctest.SiteKey("internal/collector", "processStagedPR"):         "phase",
		srctest.SiteKey("internal/collector", "processStagedIssueEvent"): "phase",
		srctest.SiteKey("internal/collector", "processStagedPREvent"):    "phase",
		srctest.SiteKey("internal/collector", "processStagedMessage"):    "phase",
	},
	// The drains' row processors run only under DrainList / DrainOnce, which
	// stamp through refreshRepoActivity (checked below).
	"DeleteStaleConversationRefs":  {srctest.SiteKey("internal/collector", "runMessageHealPass"): "stamps"},
	"ApplyTrackerAction":           {srctest.SiteKey("internal/collector", "processRow"): "phase"},
	"LinkOrCreateIssueFromEmail":   {srctest.SiteKey("internal/collector", "processRow"): "phase"},
	"UpsertMailingListMessageBody": {srctest.SiteKey("internal/collector", "processRow"): "phase"},
	"BridgeEmailToIssue":           {srctest.SiteKey("internal/collector", "processRow"): "phase"},
	"UpsertJiraComment":            {srctest.SiteKey("internal/collector", "processEnvelope"): "phase"},
	"UpsertJiraIssueFromAPI": {
		srctest.SiteKey("internal/collector", "processEnvelope"):     "phase",
		srctest.SiteKey("cmd/aveloxis", "backfillJiraIdentitiesCmd"): "stamps",
	},
	"bridgeEmailToIssueNoRecount": {},
}

// jobCallersOf is the allowance for writer fn: its own entry, else its
// class's (the processor's, or the analysis collector's).
func jobCallersOf(fn string) map[string]string {
	if c, ok := jobCallers[fn]; ok {
		return c
	}
	if pageCacheWriters[fn].how == "processor" {
		return jobCallers["processor"]
	}
	return jobCallers["analysis"]
}

// drainEntries are the drains' entry points: each refreshes through
// refreshRepoActivity, which stamps (TestDrainRefreshStampsTheRepository).
var drainEntries = []string{"func (p *MailingListProcessor) DrainList(", "func (p *JiraProcessor) DrainOnce("}

// builtBy lists, for each constructor of a phase whose writers are
// classified by file, the places allowed to build it and how each is
// covered: "job" (the scheduler's queued job), "stamps" (the enclosing
// function stamps after its run: StampReposChangedOutsideJob), "queue" (the
// enclosing file parks and releases the queue row: LockReposForDrain and
// ReleaseDrainLock stamp updated_at), "unread" (a caller that runs only a
// path writing columns no cached answer reads, with the reason).
//
// A key is "package directory.Constructor": matched as pkg.Constructor( from
// other packages and as an unqualified call inside its own.
var builtBy = map[string]map[string]string{
	"internal/collector.NewFacadeCollector": {
		srctest.SiteKey("internal/scheduler", "runFacadeAndAnalysis"): "job",
		srctest.SiteKey("internal/collector", "NewWithOptions"):       "built: the one-shot Collector (NewWithOptions below)",
		srctest.SiteKey("cmd/aveloxis", "rewalkWhitespaceCmd"):        "unread: the whitespace walk writes only commits' line counts",
	},
	"internal/collector.NewCommitResolver": {
		srctest.SiteKey("internal/scheduler", "runCommitResolution"): "job",
		srctest.SiteKey("internal/collector", "CollectRepo"):         "built: the one-shot Collector (NewWithOptions below)",
	},
	"internal/collector.NewWithOptions": {
		srctest.SiteKey("cmd/aveloxis", "runCollect"):        "stamps",
		srctest.SiteKey("internal/collector", "New"):         "built: a wrapper (New below)",
		srctest.SiteKey("internal/collector", "NewWithKeys"): "built: a wrapper (NewWithKeys below)",
	},
	// The wrappers of NewWithOptions: no production caller may build them.
	"internal/collector.New":         {},
	"internal/collector.NewWithKeys": {},
	"internal/collector.NewGapFillerWithMode": {
		srctest.SiteKey("internal/scheduler", "runJob"):       "job",
		srctest.SiteKey("cmd/aveloxis", "collectorGapFiller"): "queue",
	},
	"internal/collector.NewOpenItemRefresherWithMode": {
		srctest.SiteKey("internal/scheduler", "runJob"): "job",
	},
}

// ctorCallRE matches a call of builtBy key ctor from a function of package
// directory dir.
func ctorCallRE(ctor, dir string) *regexp.Regexp {
	pkgDir, name := ctor[:strings.LastIndex(ctor, ".")], ctor[strings.LastIndex(ctor, ".")+1:]
	if dir == pkgDir {
		return regexp.MustCompile(`(?:^|[^.\w])` + name + `\(`)
	}
	return regexp.MustCompile(`\b` + pkgDir[strings.LastIndex(pkgDir, "/")+1:] + `\.` + name + `\(`)
}

// processRepoCallers are the functions that call Processor.ProcessRepo (the
// staged processor's writers are classified "processor") and how each is
// covered: "job", "stamps" (its body stamps after the run), or the
// constructor of the type it is a method of (builtBy covers that).
var processRepoCallers = map[string]string{
	srctest.SiteKey("internal/scheduler", "collectAndProcess"):  "job",
	srctest.SiteKey("internal/scheduler", "drainOneRepo"):       "stamps",
	srctest.SiteKey("internal/collector", "runMessageHealPass"): "stamps",
	srctest.SiteKey("internal/collector", "collectAndProcess"):  "internal/collector.NewWithOptions",
	srctest.SiteKey("internal/collector", "refreshIssues"):      "internal/collector.NewOpenItemRefresherWithMode",
	srctest.SiteKey("internal/collector", "refreshPRs"):         "internal/collector.NewOpenItemRefresherWithMode",
	srctest.SiteKey("internal/collector", "fillIssueGaps"):      "internal/collector.NewGapFillerWithMode",
	srctest.SiteKey("internal/collector", "fillPRGaps"):         "internal/collector.NewGapFillerWithMode",
}

// dbDrainCallers are the functions inside internal/db that call a
// drain-classified writer without being one, and how each is covered:
// "stamps" (its body stamps the batch it wrote: stampBackfillRepos or
// StampReposChangedOutsideJob) or "drain" (only the drain files call it from
// outside the package).
var dbDrainCallers = map[string]string{
	"BackfillKeyedIssueProjection": "stamps",
	"BackfillThreadInheritance":    "stamps",
	"BridgeEmailToIssue":           "drain",
}

func TestPageCacheTableWritersAreCovered(t *testing.T) {
	files := srctest.PackageFiles(t, "internal/db", 20)
	tableRE := regexp.MustCompile(`(?i)(INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+(?:aveloxis_\w+\.)?(` + strings.Join(pageCacheTables, "|") + `)\b`)
	bodies := map[string]string{}
	found := map[string]bool{}
	for name, src := range files {
		src = srctest.StripGoComments(src)
		for fn, part := range srctest.FuncParts(t, src) {
			if fn == srctest.PackageLevel {
				continue
			}
			bodies[fn] += part
			sql := withoutSQLComments(part)
			if tableRE.MatchString(sql) || repoColumnWriteRE.MatchString(sql) {
				found[fn] = true
				if _, ok := pageCacheWriters[fn]; !ok {
					t.Errorf("%s: %s writes a table the repository page's cached answers read, and is not classified in pageCacheWriters — stamp data_changed_at in the same transaction (stampRepoCacheStateSQL), or classify it with the reason it is covered",
						name, fn)
				}
			}
		}
	}
	srctest.MinCount(t, "writers of the page-cache tables", len(found), 70)
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
		case "job", "processor", "drain", "migrate", "fleet", "unread":
			if cov.reason == "" {
				t.Errorf("%s is classified %s without a reason", fn, cov.how)
			}
		default:
			t.Errorf("%s: unknown coverage %q", fn, cov.how)
		}
	}

	// Inside the package: a drain writer is called only by another drain
	// writer, or by a reviewed caller that stamps or is itself reached only
	// from the drains.
	drainCallRE := map[string]*regexp.Regexp{}
	for fn, cov := range pageCacheWriters {
		if cov.how == "drain" {
			drainCallRE[fn] = regexp.MustCompile(`\.` + fn + `\(`)
		}
	}
	for caller, body := range bodies {
		if pageCacheWriters[caller].how == "drain" {
			continue
		}
		for fn, re := range drainCallRE {
			if !re.MatchString(body) {
				continue
			}
			switch dbDrainCallers[caller] {
			case "stamps":
				if !strings.Contains(body, "stampBackfillRepos(") && !strings.Contains(body, "StampReposChangedOutsideJob(") {
					t.Errorf("%s calls the drain writer %s and is classified as stamping, but its body stamps nothing", caller, fn)
				}
			case "drain":
			default:
				t.Errorf("%s (internal/db) calls the drain writer %s but is not reviewed in dbDrainCallers: stamp the repositories it writes, or classify it", caller, fn)
			}
		}
	}

	// The job, processor and drain classifications hold only while their
	// writers are called from their allowed sites alone, and the phases
	// those sites belong to are built only by covered callers (builtBy,
	// TestPhaseConstructorsAreBuiltOnlyByCoveredCallers).
	root := srctest.Root(t)
	var callWriters []string
	for fn, cov := range pageCacheWriters {
		if cov.how == "job" || cov.how == "processor" || cov.how == "drain" {
			callWriters = append(callWriters, fn)
		}
	}
	for caller, how := range dbDrainCallers {
		if how == "drain" {
			callWriters = append(callWriters, caller)
		}
	}
	callRE := regexp.MustCompile(`\.(` + strings.Join(callWriters, "|") + `)\b`)
	funcs := nonDBFuncParts(t)
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
			for enclosing, part := range srctest.FuncParts(t, src) {
				for _, m := range callRE.FindAllStringSubmatch(part, -1) {
					ok := false
					for a, kind := range jobCallersOf(m[1]) {
						if a != rel && a != srctest.SiteKey(srctest.SiteDir(rel), enclosing) {
							continue
						}
						ok = true
						if kind == "stamps" && !bodyStamps(part, funcs) {
							t.Errorf("%s (%s) writes through %s and is allowed as stamping, but does not stamp the repository it writes", rel, enclosing, m[1])
						}
					}
					if !ok {
						t.Errorf("%s (%s) calls %s — a %s-classified page-cache writer outside its allowed sites (jobCallers): stamp, or reclassify", rel, enclosing, m[1], pageCacheWriters[m[1]].how)
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
	for _, sig := range drainEntries {
		if !strings.Contains(srctest.StripGoComments(srctest.PackageFunc(t, "internal/collector", sig)), "refreshRepoActivity(") {
			t.Errorf("%s no longer refreshes through refreshRepoActivity (which stamps): its writers are covered by nothing", sig)
		}
	}
}

// nonDBFuncParts is every function of the non-test Go files outside
// internal/db, keyed by its SiteKey.
func nonDBFuncParts(t *testing.T) map[string]string {
	t.Helper()
	root := srctest.Root(t)
	out := map[string]string{}
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
			for fn, part := range srctest.FuncParts(t, srctest.StripGoComments(string(b))) {
				if fn != srctest.PackageLevel {
					out[srctest.SiteKey(srctest.SiteDir(rel), fn)] += part
				}
			}
			return nil
		})
	}
	srctest.MinCount(t, "non-db functions", len(out), 500)
	return out
}

// stampingHelpers are the functions a run outside a job may stamp through
// instead of calling StampReposChangedOutsideJob itself (heal-messages
// defers its stamp, round 1 finding 2 of 0.29.123); each is checked to stamp.
var stampingHelpers = []string{srctest.SiteKey("internal/collector", "stampHealBatch")}

// bodyStamps reports whether a function body stamps the repositories it
// wrote: StampReposChangedOutsideJob itself, or a stampingHelpers function
// whose own body does.
func bodyStamps(body string, funcs map[string]string) bool {
	if strings.Contains(body, "StampReposChangedOutsideJob(") {
		return true
	}
	for _, h := range stampingHelpers {
		name := h[strings.Index(h, "|")+1:]
		if strings.Contains(body, name+"(") && strings.Contains(funcs[h], "StampReposChangedOutsideJob(") {
			return true
		}
	}
	return false
}

// coveredSite checks one reviewed caller against its claim.
func coveredSite(t *testing.T, site, how, body string, funcs map[string]string) {
	t.Helper()
	switch {
	case how == "job":
		if !strings.HasPrefix(site, "internal/scheduler|") {
			t.Errorf("%s is classified as the queued job but is not in the scheduler", site)
		}
	case how == "stamps":
		if !bodyStamps(body, funcs) {
			t.Errorf("%s is classified as stamping after its run but its body stamps nothing (StampReposChangedOutsideJob)", site)
		}
	case how == "queue":
		file := strings.Join(fileFuncs(site, funcs), "")
		if !strings.Contains(file, "LockReposForDrain(") || !strings.Contains(file, "ReleaseDrainLock(") {
			t.Errorf("%s is classified as parking the queue row, but its package lacks LockReposForDrain/ReleaseDrainLock", site)
		}
	case strings.HasPrefix(how, "unread: "), strings.HasPrefix(how, "built: "):
	default:
		if _, ok := builtBy[how]; !ok {
			t.Errorf("%s: unknown coverage %q", site, how)
		}
	}
}

// fileFuncs is every function of site's package (its directory).
func fileFuncs(site string, funcs map[string]string) []string {
	dir := site[:strings.Index(site, "|")+1]
	var out []string
	for k, v := range funcs {
		if strings.HasPrefix(k, dir) {
			out = append(out, v)
		}
	}
	return out
}

// TestProcessRepoCallersAreCovered — the staged processor's writers are
// covered only while every Processor.ProcessRepo call is at a reviewed site
// that is the queued job, stamps after its run, or is a method of a phase
// built only by covered callers (0.29.120, D7: the leftover drain's
// synchronous fallback and heal-messages wrote outside any job unstamped).
func TestProcessRepoCallersAreCovered(t *testing.T) {
	funcs := nonDBFuncParts(t)
	seen := map[string]bool{}
	for site, body := range funcs {
		if !strings.Contains(body, ".ProcessRepo(") {
			continue
		}
		seen[site] = true
		how, ok := processRepoCallers[site]
		if !ok {
			t.Errorf("%s calls Processor.ProcessRepo but is not reviewed in processRepoCallers: its staged writes would be served stale by the API until the next collection", site)
			continue
		}
		coveredSite(t, site, how, body, funcs)
	}
	srctest.MinCount(t, "ProcessRepo callers", len(seen), 8)
	for site := range processRepoCallers {
		if !seen[site] {
			t.Errorf("processRepoCallers lists %s, which no longer calls ProcessRepo — remove it", site)
		}
	}
}

// TestPhaseConstructorsAreBuiltOnlyByCoveredCallers — the file-classified
// writers (the facade's, the commit resolver's, the processor's) are covered
// only while their phases are built by covered callers alone.
func TestPhaseConstructorsAreBuiltOnlyByCoveredCallers(t *testing.T) {
	funcs := nonDBFuncParts(t)
	for ctor, allowed := range builtBy {
		self := srctest.SiteKey(ctor[:strings.LastIndex(ctor, ".")], ctor[strings.LastIndex(ctor, ".")+1:])
		seen := 0
		for site, body := range funcs {
			if site == self || !ctorCallRE(ctor, site[:strings.Index(site, "|")]).MatchString(body) {
				continue
			}
			seen++
			how, ok := allowed[site]
			if !ok {
				t.Errorf("%s builds %s, which is not one of its covered callers (builtBy): its writes would be served stale by the API", site, ctor)
				continue
			}
			coveredSite(t, site, how, body, funcs)
		}
		if seen < len(allowed) {
			t.Errorf("%s is built at %d sites but builtBy lists %d — remove the ones that no longer build it", ctor, seen, len(allowed))
		}
	}
}

// stampPlacement checks a stamping writer's body for a stamp in the SAME
// transaction or statement as its data (whole-branch review: only the
// token was checked, and a stamp moved after the commit — the shape PR
// review 5402109493 removed — passed). Accepted: the stamp inlined into the
// writer's SQL (a data-modifying CTE), or a tx.Exec / batch.Queue of it
// before the body's Commit. A stamp on the pool is a separate statement
// that can fail after the data commits. "" means placed correctly.
func stampPlacement(body string) string {
	// The column set inline in the writer's own UPDATE of the repository row
	// (RecordCommitBounds, 0.29.120): the same statement as its data —
	// `data_changed_at = NOW()` / `= CASE … NOW()`, or the column in a
	// multiple-column assignment (round 2 of 0.29.123) followed by the
	// stamp-on-change idiom `THEN NOW() ELSE <alias>.data_changed_at END`.
	// This checks PLACEMENT only — the column is assigned in the writer's
	// own statement, never on the pool after its data — not that the NOW()
	// it finds is the column's value: a NOW() in the WHERE, or at another
	// list position, still passes (decided as a class in round 4, after
	// rounds 3 and 4 each found one more spelling). The value is the runtime
	// tests' job, both sides for each inline-stamping writer:
	// TestRecordCommitBoundsStampsTheRepository (a change stamps) and
	// TestRecordCommitBoundsStampsOnlyAChange (no change, no stamp);
	// TestUpdateRepoMetadataStampsWhenItFillsCreatedAt (both).
	if regexp.MustCompile("(?is)UPDATE\\s+(?:aveloxis_data\\.)?repos\\b[^`]*?(?:\\bdata_changed_at\\s*=\\s*(?:NOW\\(\\)|CASE\\b[^`]*?\\bNOW\\(\\))|(?:\\bSET\\s*|,\\s*)\\([^()]*\\bdata_changed_at\\b[^()]*\\)\\s*=\\s*\\([^`]*?\\bTHEN\\s+NOW\\(\\)\\s+ELSE\\s+(?:\\w+\\.)?data_changed_at\\s+END\\b)").MatchString(withoutSQLComments(body)) {
		return ""
	}
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
		// 0.29.120: the column inline in the writer's own UPDATE of repos.
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET first_commit_at = x, data_changed_at = NOW() WHERE r.repo_id = $1`)": true,
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET first_commit_at = x WHERE r.repo_id = $1`)":                          false,
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.issues SET closed_at = x, data_changed_at = NOW()`)":                             false,
		// The multiple-column form (round 2 finding 3 of 0.29.123).
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET (first_commit_at, data_changed_at) = (SELECT b.f, CASE WHEN b.f IS DISTINCT FROM r.first_commit_at THEN NOW() ELSE r.data_changed_at END FROM (SELECT x AS f) b) WHERE r.repo_id = $1`)": true,
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET (first_commit_at, last_commit_at) = (SELECT b.f, NOW() FROM (SELECT x AS f) b) WHERE r.repo_id = $1`)":                                                                                   false,
		// Round 3 finding 4: a NOW() that is not data_changed_at's value.
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET (data_changed_at, first_commit_at) = (SELECT r.data_changed_at, NOW()) WHERE r.repo_id = $1`)":                            false,
		"s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET (first_commit_at, data_changed_at) = (SELECT b.f, r.data_changed_at FROM (SELECT x AS f) b) WHERE r.updated_at < NOW()`)": false,
	} {
		if got := stampPlacement(body) == ""; got != ok {
			t.Errorf("stampPlacement(%q) accepted=%t, want %t (%s)", body, got, ok, stampPlacement(body))
		}
	}
}

// Round 1 finding 1 (0.29.123): the repos-column matcher reads past a ";"
// inside an SQL comment (UpdateRepoMetadata's "given repo; a DIFFERENT
// incoming ID" hid its created_at write from the registry).
func TestRepoColumnMatcherSeesPastSQLComments(t *testing.T) {
	src := "s.pool.QueryRow(ctx, `\n\tUPDATE aveloxis_data.repos\n\tSET a = $2,\n\t    -- one; two\n\t    created_at = COALESCE(repos.created_at, $8)\n\tWHERE repo_id = $1`)"
	if !repoColumnWriteRE.MatchString(withoutSQLComments(src)) {
		t.Error("an UPDATE of repos that sets created_at after a commented ';' must match")
	}
	if repoColumnWriteRE.MatchString(withoutSQLComments("s.pool.Exec(ctx, `UPDATE aveloxis_data.repos SET repo_name = $2 WHERE repo_id = $1`)")) {
		t.Error("an UPDATE of repos that sets no read column must not match")
	}
	// The multiple-column form (round 2 finding 3 of 0.29.123): a listed
	// read column is written; one only on the right side is not.
	if !repoColumnWriteRE.MatchString("s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET\n (first_commit_at, last_commit_at) = (SELECT 1, 2) WHERE r.repo_id = $1`)") {
		t.Error("SET (first_commit_at, ...) = (...) writes a read column and must match")
	}
	if repoColumnWriteRE.MatchString("s.pool.Exec(ctx, `UPDATE aveloxis_data.repos r SET (repo_name, repo_owner) = (SELECT r.repo_git, COALESCE(r.first_commit_at, NOW())) WHERE r.repo_id = $1`)") {
		t.Error("a read column only on the right side of a multiple-column SET must not match")
	}
}
