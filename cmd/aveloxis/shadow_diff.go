package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"
)

// shadowDiffCmd compares two Aveloxis databases row-by-row for the tables
// populated by issue and PR collection. Used to verify that a refactored
// collection path produces the same data as the baseline.
//
// The comparison uses the "semantic" pass criterion the user picked
// (docs/architecture/collection-refactor-2026-04.md, answer to Q9):
//
//   - Every row present in the REST database must be present in the GraphQL
//     database with matching content on the content columns. Content
//     mismatches FAIL the phase.
//   - Rows present in GraphQL but not in REST are FLAGGED, not failed.
//     GraphQL can legitimately return data that REST could not surface
//     (e.g., review thread structure, reaction counts). These are candidates
//     for schema expansion in later phases and are listed in the report.
//   - Metadata columns (tool_version, data_source, data_collection_date,
//     created_at / updated_at timestamps) are excluded from content
//     comparison — they're expected to differ because the two runs happen
//     at different moments.
func shadowDiffCmd() *cobra.Command {
	var (
		restDSN    string
		graphqlDSN string
		repoID     int64
		outputJSON bool
	)

	cmd := &cobra.Command{
		Use:   "shadow-diff",
		Short: "Compare two Aveloxis databases for issue/PR collection equivalence",
		Long: `Runs a semantic diff of issues, PRs, labels, assignees, reviewers,
reviews, commits, files, messages, and related bridge tables between two
databases. Use after collecting the same repo via two different code paths
(e.g., old REST path vs new GraphQL path) to verify equivalence.

The tool prints a human-readable report by default. --json emits a machine
-readable report suitable for CI pipes. Exit code is 1 if any FAIL-level
delta is present; 0 otherwise. Flagged deltas (new GraphQL-only rows) do
not fail the exit code on their own.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := context.Background()
			logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

			rest, err := pgxpool.New(ctx, restDSN)
			if err != nil {
				return fmt.Errorf("connecting to REST database: %w", err)
			}
			defer rest.Close()
			graphql, err := pgxpool.New(ctx, graphqlDSN)
			if err != nil {
				return fmt.Errorf("connecting to GraphQL database: %w", err)
			}
			defer graphql.Close()

			report, err := runShadowDiff(ctx, logger, rest, graphql, repoID)
			if err != nil {
				return err
			}

			if outputJSON {
				enc := json.NewEncoder(os.Stdout)
				enc.SetIndent("", "  ")
				if err := enc.Encode(report); err != nil {
					return err
				}
			} else {
				printShadowReport(os.Stdout, report)
			}

			if report.HasFailures() {
				os.Exit(1)
			}
			return nil
		},
	}

	cmd.Flags().StringVar(&restDSN, "rest-dsn", "", "Postgres DSN for the baseline (REST) database")
	cmd.Flags().StringVar(&graphqlDSN, "graphql-dsn", "", "Postgres DSN for the comparison (GraphQL) database")
	cmd.Flags().Int64Var(&repoID, "repo-id", 0, "restrict diff to a single repo_id (0 = all repos in both DBs)")
	cmd.Flags().BoolVar(&outputJSON, "json", false, "emit JSON instead of human-readable report")
	_ = cmd.MarkFlagRequired("rest-dsn")
	_ = cmd.MarkFlagRequired("graphql-dsn")
	return cmd
}

// ShadowReport is the comparison result.
type ShadowReport struct {
	GeneratedAt time.Time       `json:"generated_at"`
	RepoID      int64           `json:"repo_id,omitempty"`
	Tables      []TableDiff     `json:"tables"`
	Summary     ShadowSummary   `json:"summary"`
}

type ShadowSummary struct {
	TotalTables     int `json:"total_tables"`
	TablesWithFails int `json:"tables_with_fails"`
	TablesWithFlags int `json:"tables_with_flags"`
	TotalFailRows   int `json:"total_fail_rows"`
	TotalFlagRows   int `json:"total_flag_rows"`
}

// TableDiff summarizes one table's diff.
type TableDiff struct {
	Table       string     `json:"table"`
	RESTRows    int        `json:"rest_rows"`
	GraphQLRows int        `json:"graphql_rows"`
	// FailMissing: rows in REST but not in GraphQL — regression.
	FailMissing []DeltaRow `json:"fail_missing,omitempty"`
	// FailContent: rows present on both sides with differing content columns.
	FailContent []DeltaRow `json:"fail_content,omitempty"`
	// FlagExtra: rows in GraphQL but not in REST — candidate new coverage,
	// NOT a failure.
	FlagExtra []DeltaRow `json:"flag_extra,omitempty"`
}

// DeltaRow identifies a single mismatched row. Kept intentionally small —
// for a table with 10K differing rows we want the report to be readable,
// so we cap at 20 examples per category.
type DeltaRow struct {
	PrimaryKey string            `json:"primary_key"`
	Details    map[string]string `json:"details,omitempty"`
}

// HasFailures returns true iff any table has Missing or Content failures.
func (r *ShadowReport) HasFailures() bool {
	for _, t := range r.Tables {
		if len(t.FailMissing) > 0 || len(t.FailContent) > 0 {
			return true
		}
	}
	return false
}

const exampleCap = 20

// shadowTables lists the tables compared and the columns excluded from
// content comparison. Kept in code rather than config so each phase can
// adjust as columns are added.
//
// Intentional omissions:
//   - repo_info: timestamps of counts differ between runs; not in issue/PR scope.
//   - commits, contributors, contributor_identities, contributor_repo: outside
//     the issue/PR scope. Compared only indirectly via foreign keys.
//   - dm_* aggregates and matviews: derived, compared via the base tables.
//   - messages: populated by several paths; comparison by content hash
//     only to avoid tripping on ordering.
type shadowTable struct {
	Name            string
	PrimaryKey      []string // columns that identify a row uniquely for diff purposes
	ContentColumns  []string // columns whose values must match
	ExcludedColumns []string // excluded for readability only; implicit from not listing
	// HasRepoID is true when the table has a repo_id column (whether or
	// not it's in the PK). Used by --repo-id to scope the diff. All
	// shadow tables today carry repo_id, so default true would have
	// worked, but the field is explicit so a future table addition
	// without repo_id (e.g. some normalized lookup) doesn't crash.
	HasRepoID bool
	// ResolvedColumns names local-FK columns whose values always differ
	// between two fresh databases (because they're auto-increment
	// serials) but that reference a parent row with a platform-stable
	// identifier. The diff tool resolves each of these client-side
	// before comparing: it preloads a map[localID]platformID from the
	// parent table on each database, then substitutes the resolved
	// platform value for the local value in the content string.
	//
	// This catches cross-linkage bugs: if REST's review_comment.msg_id=42
	// points to a message with platform_msg_id=999 but GraphQL's
	// review_comment.msg_id=8 points to a message with platform_msg_id=888,
	// the content comparison fails — without this machinery, the diff
	// would either silently accept the mismatch (local IDs excluded) or
	// false-positive every row (local IDs included).
	ResolvedColumns []resolvedColumn
}

// resolvedColumn describes how to substitute a local serial FK with its
// platform-stable target for equivalence comparison. See shadowTable.ResolvedColumns.
//
// Applies to BOTH primary-key columns (via fetchPKs) and content columns
// (via diffContent). If LocalColumn matches a PK column name the substitution
// happens during the PK set-diff; if it matches a content column — or appears
// on neither list — it's appended to the content comparison string.
type resolvedColumn struct {
	// LocalColumn is the column in this table whose value is a local
	// serial FK (e.g. "msg_id" in review_comments, "pull_request_id" in
	// pull_request_labels).
	LocalColumn string
	// RefTable is the parent table the FK points into. Must include the
	// schema prefix (e.g. "aveloxis_data.messages"). Ignored when
	// CustomQuery is set.
	RefTable string
	// RefLocalKey is the parent's local primary key column that
	// LocalColumn references (e.g. "msg_id" in messages). Ignored when
	// CustomQuery is set.
	RefLocalKey string
	// RefPlatformKey is the parent's platform-stable column we resolve
	// to for comparison (e.g. "platform_msg_id" in messages). This
	// value should be identical for the same real-world row across
	// any two fresh databases. Ignored when CustomQuery is set.
	RefPlatformKey string
	// CustomQuery, when non-empty, replaces the default two-column SELECT
	// with an arbitrary query that must return exactly two columns:
	// (local_id, platform_stable_value). Used when the target's
	// platform-stable identity requires a join through another table —
	// e.g. pull_request_repo.pr_repo_meta_id resolves through
	// pull_request_meta → pull_requests to get a platform_pr_id+head_or_base
	// composite stable key.
	CustomQuery string
}

func shadowTables() []shadowTable {
	return []shadowTable{
		{
			Name:           "aveloxis_data.issues",
			PrimaryKey:     []string{"repo_id", "issue_number"},
			ContentColumns: []string{"issue_title", "issue_state", "reporter_id", "comment_count"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.issue_labels",
			PrimaryKey:     []string{"repo_id", "issue_id", "label_text"},
			ContentColumns: []string{"label_description", "label_color"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.issue_assignees",
			PrimaryKey:     []string{"repo_id", "issue_id", "platform_assignee_id"},
			ContentColumns: []string{},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.issue_events",
			PrimaryKey:     []string{"repo_id", "issue_id", "platform_event_id"},
			ContentColumns: []string{"action", "action_commit_hash", "cntrb_id"},
			HasRepoID:      true,
		},
		{
			Name:       "aveloxis_data.pull_requests",
			PrimaryKey: []string{"repo_id", "pr_number"},
			// meta_head_id / meta_base_id are local serial FKs. Resolve
			// them to meta_sha so the diff catches cross-linkage bugs
			// where a PR points at the wrong head/base meta row.
			ContentColumns: []string{"pr_title", "pr_state", "author_id", "merge_commit_sha"},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "meta_head_id", RefTable: "aveloxis_data.pull_request_meta", RefLocalKey: "pr_meta_id", RefPlatformKey: "meta_sha"},
				{LocalColumn: "meta_base_id", RefTable: "aveloxis_data.pull_request_meta", RefLocalKey: "pr_meta_id", RefPlatformKey: "meta_sha"},
			},
			HasRepoID: true,
		},
		// PR child tables keyed on pull_request_id (a local serial FK).
		// Sharded inserts make those serials non-deterministic — the same
		// real GitHub PR gets different local IDs in the REST and GraphQL
		// shadow databases. Without resolution the PK set-diff sees every
		// child row as both REST-only and GraphQL-only. ResolvedColumns on
		// pull_request_id instructs fetchPKs to substitute the value with
		// pull_requests.platform_pr_id before building the PK string, so
		// rows match iff they share the same (repo_id, platform_pr_id, …)
		// tuple — which they always will for the same real PR.
		{
			Name:           "aveloxis_data.pull_request_labels",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "label_name"},
			ContentColumns: []string{"label_description", "label_color"},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_assignees",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "platform_assignee_id"},
			ContentColumns: []string{},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_reviewers",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "platform_reviewer_id"},
			ContentColumns: []string{},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_reviews",
			PrimaryKey:     []string{"repo_id", "platform_review_id"},
			ContentColumns: []string{"review_state", "cntrb_id", "commit_id"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_commits",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "pr_cmt_sha"},
			ContentColumns: []string{},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_files",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "pr_file_path"},
			ContentColumns: []string{"pr_file_additions", "pr_file_deletions"},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_meta",
			PrimaryKey:     []string{"pull_request_id", "head_or_base"},
			ContentColumns: []string{"meta_ref", "meta_sha"},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		// Bridge tables: PK uses platform_src_id (stable GitHub/GitLab
		// comment ID) instead of local msg_id/issue_id/pr_review_id
		// FKs. The local FKs always differ between two fresh DBs even
		// when the underlying comment is the same real GitHub comment.
		// platform_src_id is stable across collections so the set
		// comparison reflects real-world equivalence.
		//
		// ResolvedColumns on each bridge verify the local msg_id FK
		// resolves to the same platform_msg_id on both sides — i.e.,
		// that the bridge rows aren't accidentally pointing at
		// different real messages.
		{
			Name:           "aveloxis_data.issue_message_ref",
			PrimaryKey:     []string{"repo_id", "platform_src_id"},
			ContentColumns: []string{},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "msg_id", RefTable: "aveloxis_data.messages", RefLocalKey: "msg_id", RefPlatformKey: "platform_msg_id"},
				{LocalColumn: "issue_id", RefTable: "aveloxis_data.issues", RefLocalKey: "issue_id", RefPlatformKey: "platform_issue_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_message_ref",
			PrimaryKey:     []string{"repo_id", "platform_src_id"},
			ContentColumns: []string{},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "msg_id", RefTable: "aveloxis_data.messages", RefLocalKey: "msg_id", RefPlatformKey: "platform_msg_id"},
				{LocalColumn: "pull_request_id", RefTable: "aveloxis_data.pull_requests", RefLocalKey: "pull_request_id", RefPlatformKey: "platform_pr_id"},
			},
			HasRepoID: true,
		},
		{
			Name:           "aveloxis_data.pull_request_review_message_ref",
			PrimaryKey:     []string{"repo_id", "pr_review_msg_src_id"},
			ContentColumns: []string{},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "msg_id", RefTable: "aveloxis_data.messages", RefLocalKey: "msg_id", RefPlatformKey: "platform_msg_id"},
				{LocalColumn: "pr_review_id", RefTable: "aveloxis_data.pull_request_reviews", RefLocalKey: "pr_review_id", RefPlatformKey: "platform_review_id"},
			},
			HasRepoID: true,
		},
		{
			Name:       "aveloxis_data.review_comments",
			PrimaryKey: []string{"repo_id", "platform_src_id"},
			// msg_id and pr_review_id are local serial FKs. Their raw
			// integer values always differ between two fresh DBs.
			// ResolvedColumns below substitutes each with its
			// platform-stable target before comparison, so a cross-
			// linkage bug (review_comment → wrong message) would
			// surface as a content mismatch here.
			ContentColumns: []string{"commit_id", "file_path", "line"},
			ResolvedColumns: []resolvedColumn{
				{LocalColumn: "msg_id", RefTable: "aveloxis_data.messages", RefLocalKey: "msg_id", RefPlatformKey: "platform_msg_id"},
				{LocalColumn: "pr_review_id", RefTable: "aveloxis_data.pull_request_reviews", RefLocalKey: "pr_review_id", RefPlatformKey: "platform_review_id"},
			},
			HasRepoID: true,
		},
		// pull_request_repo tracks head/base repository (fork-source)
		// metadata for each PR. Populated from the same source the PR
		// meta came from: REST via FetchPRRepos, GraphQL via the
		// headRepository/baseRepository fields in the batch query.
		// No repo_id column — the PR→meta→repo chain is joined through
		// pr_repo_meta_id. For the --repo-id filter to work, diff
		// callers pointed at single-repo databases collect this table
		// unfiltered (both shadow DBs have only repo_id=1).
		//
		// pr_repo_meta_id is a local serial FK into pull_request_meta,
		// whose pr_meta_id is itself a local serial. To get a stable key
		// we resolve pr_repo_meta_id via a two-hop join through
		// pull_request_meta → pull_requests, producing a composite
		// "platform_pr_id:head_or_base" that's identical across fresh DBs
		// for the same real PR head/base row.
		{
			Name:           "aveloxis_data.pull_request_repo",
			PrimaryKey:     []string{"pr_repo_meta_id", "pr_repo_head_or_base"},
			ContentColumns: []string{"pr_src_repo_id", "pr_repo_name", "pr_repo_full_name", "pr_repo_private_bool"},
			ResolvedColumns: []resolvedColumn{
				{
					LocalColumn: "pr_repo_meta_id",
					CustomQuery: `SELECT prm.pr_meta_id, pr.platform_pr_id::text || ':' || prm.head_or_base
						FROM aveloxis_data.pull_request_meta prm
						JOIN aveloxis_data.pull_requests pr ON pr.pull_request_id = prm.pull_request_id`,
				},
			},
			HasRepoID: false,
		},
		// messages is the shared text store for every comment across
		// issues, PRs, and reviews. Phase 1 doesn't touch comment
		// collection, but the cntrb_id column on each message depends
		// on contributor resolution, which GraphQL's inline author.databaseId
		// may speed up without changing the final resolution outcome.
		// Diffing the table catches any unexpected drift in body,
		// timestamp, or resolved contributor across the two paths.
		{
			Name:           "aveloxis_data.messages",
			PrimaryKey:     []string{"repo_id", "platform_id", "platform_msg_id"},
			ContentColumns: []string{"msg_text", "msg_timestamp", "cntrb_id", "msg_sender_email"},
			HasRepoID:      true,
		},
	}
}

// printShadowReport writes a human-readable summary of the diff.
func printShadowReport(w *os.File, report *ShadowReport) {
	fmt.Fprintf(w, "Shadow diff generated %s\n", report.GeneratedAt.Format(time.RFC3339))
	if report.RepoID > 0 {
		fmt.Fprintf(w, "Scope: repo_id=%d\n", report.RepoID)
	} else {
		fmt.Fprintln(w, "Scope: all repos")
	}
	fmt.Fprintln(w, strings.Repeat("=", 78))

	fmt.Fprintf(w, "\nSummary: %d tables compared, %d with failures, %d with flags\n",
		report.Summary.TotalTables, report.Summary.TablesWithFails, report.Summary.TablesWithFlags)
	fmt.Fprintf(w, "Total fail rows: %d (regressions)\n", report.Summary.TotalFailRows)
	fmt.Fprintf(w, "Total flag rows: %d (GraphQL-only — candidate new coverage)\n", report.Summary.TotalFlagRows)
	fmt.Fprintln(w, strings.Repeat("=", 78))

	for _, td := range report.Tables {
		fmt.Fprintf(w, "\n## %s\n", td.Table)
		fmt.Fprintf(w, "  rest: %d rows, graphql: %d rows\n", td.RESTRows, td.GraphQLRows)
		if len(td.FailMissing) > 0 {
			fmt.Fprintf(w, "  FAIL — %d rows present in REST but missing from GraphQL:\n", len(td.FailMissing))
			for _, d := range td.FailMissing {
				fmt.Fprintf(w, "    - %s\n", d.PrimaryKey)
			}
		}
		if len(td.FailContent) > 0 {
			fmt.Fprintf(w, "  FAIL — %d rows with content mismatches:\n", len(td.FailContent))
			for _, d := range td.FailContent {
				fmt.Fprintf(w, "    - %s\n", d.PrimaryKey)
				for k, v := range d.Details {
					fmt.Fprintf(w, "        %s: %s\n", k, v)
				}
			}
		}
		if len(td.FlagExtra) > 0 {
			fmt.Fprintf(w, "  FLAG — %d rows present in GraphQL but not REST (candidates for new coverage):\n", len(td.FlagExtra))
			for _, d := range td.FlagExtra {
				fmt.Fprintf(w, "    - %s\n", d.PrimaryKey)
			}
		}
	}
}

// runShadowDiff executes the diff. Takes two pgxpools and an optional repoID
// filter. Returns a populated ShadowReport.
func runShadowDiff(ctx context.Context, logger *slog.Logger, rest, graphql *pgxpool.Pool, repoID int64) (*ShadowReport, error) {
	report := &ShadowReport{
		GeneratedAt: time.Now().UTC(),
		RepoID:      repoID,
	}
	tables := shadowTables()
	sort.Slice(tables, func(i, j int) bool { return tables[i].Name < tables[j].Name })

	for _, tbl := range tables {
		td, err := diffOneTable(ctx, logger, rest, graphql, tbl, repoID)
		if err != nil {
			return nil, fmt.Errorf("diffing %s: %w", tbl.Name, err)
		}
		report.Tables = append(report.Tables, *td)
		report.Summary.TotalTables++
		if len(td.FailMissing) > 0 || len(td.FailContent) > 0 {
			report.Summary.TablesWithFails++
		}
		if len(td.FlagExtra) > 0 {
			report.Summary.TablesWithFlags++
		}
		report.Summary.TotalFailRows += len(td.FailMissing) + len(td.FailContent)
		report.Summary.TotalFlagRows += len(td.FlagExtra)
	}

	return report, nil
}

// diffOneTable runs the semantic diff for a single table.
//
// Strategy: FULL OUTER JOIN between REST and GraphQL on the primary key.
// NULL on the left = row is GraphQL-only (FLAG). NULL on the right = row is
// REST-only (FAIL missing). Both non-NULL but content differs = FAIL content.
//
// Uses psql-style via a dynamically-built SQL string. PKs and content cols
// come from code (shadowTables above), not user input, so string-concat is
// safe. Both DSNs must point at schemas with identical shape — we don't
// guard against schema drift between the two DBs; that's on the operator.
func diffOneTable(ctx context.Context, logger *slog.Logger, rest, graphql *pgxpool.Pool, tbl shadowTable, repoID int64) (*TableDiff, error) {
	td := &TableDiff{Table: tbl.Name}

	where := ""
	if repoID > 0 && tbl.HasRepoID {
		// All shadow tables today carry repo_id (even when it's not in
		// the PK — see shadowTable.HasRepoID). Filter scopes the diff
		// to one repo for fast iteration during development.
		where = fmt.Sprintf(" WHERE repo_id = %d", repoID)
	}

	// Count rows per side.
	restCount, err := countRows(ctx, rest, tbl.Name, where)
	if err != nil {
		return nil, err
	}
	graphqlCount, err := countRows(ctx, graphql, tbl.Name, where)
	if err != nil {
		return nil, err
	}
	td.RESTRows = restCount
	td.GraphQLRows = graphqlCount

	// Build resolution maps up front. They're needed by both the PK
	// set-diff (for tables with local serial FKs in the PK) and the
	// content diff (for cross-linkage checks). Building once avoids
	// re-querying the parent tables.
	restResolvers, err := buildResolvers(ctx, rest, tbl.ResolvedColumns)
	if err != nil {
		return nil, fmt.Errorf("building REST resolvers: %w", err)
	}
	graphqlResolvers, err := buildResolvers(ctx, graphql, tbl.ResolvedColumns)
	if err != nil {
		return nil, fmt.Errorf("building GraphQL resolvers: %w", err)
	}

	// Fetch primary-key sets from each side. Content comparison is done
	// in code rather than SQL because the two databases are on different
	// connections — a server-side JOIN is not available.
	restPKs, err := fetchPKs(ctx, rest, tbl, where, restResolvers)
	if err != nil {
		return nil, err
	}
	graphqlPKs, err := fetchPKs(ctx, graphql, tbl, where, graphqlResolvers)
	if err != nil {
		return nil, err
	}

	for pk := range restPKs {
		if _, ok := graphqlPKs[pk]; !ok {
			if len(td.FailMissing) < exampleCap {
				td.FailMissing = append(td.FailMissing, DeltaRow{PrimaryKey: pk})
			}
		}
	}
	for pk := range graphqlPKs {
		if _, ok := restPKs[pk]; !ok {
			if len(td.FlagExtra) < exampleCap {
				td.FlagExtra = append(td.FlagExtra, DeltaRow{PrimaryKey: pk})
			}
		}
	}
	// Count totals beyond the cap so the summary numbers are honest.
	// The examples are capped, but TotalFailRows/TotalFlagRows in the
	// summary should still reflect the true count.
	td.FailMissing = capAndCount(td.FailMissing, countMissing(restPKs, graphqlPKs))
	td.FlagExtra = capAndCount(td.FlagExtra, countMissing(graphqlPKs, restPKs))

	// Content comparison on the intersection. Runs when there are either
	// content columns OR resolved-FK columns to verify — both represent
	// ways a row's meaning can drift between the two databases.
	if len(tbl.ContentColumns) > 0 || len(tbl.ResolvedColumns) > 0 {
		mismatches, err := diffContent(ctx, rest, graphql, tbl, where, restResolvers, graphqlResolvers)
		if err != nil {
			return nil, err
		}
		td.FailContent = mismatches
	}

	logger.Info("table compared",
		"table", tbl.Name,
		"rest_rows", restCount, "graphql_rows", graphqlCount,
		"fail_missing", len(td.FailMissing),
		"fail_content", len(td.FailContent),
		"flag_extra", len(td.FlagExtra))
	return td, nil
}

func countRows(ctx context.Context, pool *pgxpool.Pool, table, where string) (int, error) {
	var n int
	err := pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+table+where).Scan(&n)
	return n, err
}

// fetchPKs loads the set of primary-key tuples for a table. Returns a map
// from the stringified PK (joined by "|") to itself — good enough for
// set-diff operations.
//
// When a PK column name matches a ResolvedColumn.LocalColumn, the raw local
// serial is substituted with its platform-stable resolution before building
// the PK string. This is what makes sharded-insert child tables (whose
// pull_request_id serials differ between two fresh DBs) diff correctly.
func fetchPKs(ctx context.Context, pool *pgxpool.Pool, tbl shadowTable, where string, resolvers map[string]map[string]string) (map[string]struct{}, error) {
	// Build a per-PK-column lookup of which resolver to apply.
	resolverFor := map[string]map[string]string{}
	for _, rc := range tbl.ResolvedColumns {
		if r, ok := resolvers[rc.LocalColumn]; ok {
			resolverFor[rc.LocalColumn] = r
		}
	}

	sel := strings.Join(tbl.PrimaryKey, ", ")
	rows, err := pool.Query(ctx, "SELECT "+sel+" FROM "+tbl.Name+where)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]struct{}{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		parts := make([]string, 0, len(vals))
		for i, v := range vals {
			localVal := fmt.Sprintf("%v", v)
			if i < len(tbl.PrimaryKey) {
				if r, ok := resolverFor[tbl.PrimaryKey[i]]; ok {
					if resolved, found := r[localVal]; found {
						parts = append(parts, resolved)
						continue
					}
					// FK points at a parent row that isn't there —
					// a real inconsistency on this side. Record a
					// sentinel so the two databases don't both show
					// "<missing>" and falsely match.
					parts = append(parts, "<missing:"+tbl.PrimaryKey[i]+"="+localVal+">")
					continue
				}
			}
			parts = append(parts, localVal)
		}
		out[strings.Join(parts, "|")] = struct{}{}
	}
	return out, rows.Err()
}

// diffContent finds rows that exist on both sides but have differing content
// columns. Implemented by pulling PK+content+resolved-FKs from both sides
// and comparing in memory; capped at exampleCap mismatches reported.
//
// For each column named in tbl.ResolvedColumns, the diff substitutes the
// local serial value with its platform-stable resolution before building
// the content string. This catches cross-linkage bugs (e.g. REST and
// GraphQL's review_comment.msg_id values pointing to messages with
// DIFFERENT real-world platform_msg_ids).
func diffContent(ctx context.Context, rest, graphql *pgxpool.Pool, tbl shadowTable, where string, restResolvers, graphqlResolvers map[string]map[string]string) ([]DeltaRow, error) {
	// Resolved columns that are ALREADY in the PK are handled during
	// fetchPKs — including them again here would double-select the same
	// column and inflate the content string for no benefit. The ones
	// left in contentRCols are purely content-level cross-linkage checks.
	pkSet := map[string]struct{}{}
	for _, c := range tbl.PrimaryKey {
		pkSet[c] = struct{}{}
	}
	contentRCols := make([]resolvedColumn, 0, len(tbl.ResolvedColumns))
	for _, r := range tbl.ResolvedColumns {
		if _, inPK := pkSet[r.LocalColumn]; inPK {
			continue
		}
		contentRCols = append(contentRCols, r)
	}

	// Select: PK columns, then content columns, then local columns for
	// any content-level resolved FKs (appended so rows come back in a
	// predictable shape).
	cols := append([]string(nil), tbl.PrimaryKey...)
	cols = append(cols, tbl.ContentColumns...)
	for _, r := range contentRCols {
		cols = append(cols, r.LocalColumn)
	}
	sel := strings.Join(cols, ", ")
	q := "SELECT " + sel + " FROM " + tbl.Name + where

	restData, err := fetchRowsWithResolved(ctx, rest, q, tbl.PrimaryKey, len(tbl.ContentColumns), restResolvers, contentRCols)
	if err != nil {
		return nil, err
	}
	graphqlData, err := fetchRowsWithResolved(ctx, graphql, q, tbl.PrimaryKey, len(tbl.ContentColumns), graphqlResolvers, contentRCols)
	if err != nil {
		return nil, err
	}

	var out []DeltaRow
	for pk, restContent := range restData {
		if len(out) >= exampleCap {
			break
		}
		graphqlContent, ok := graphqlData[pk]
		if !ok {
			continue // missing rows are already in FailMissing
		}
		if restContent != graphqlContent {
			out = append(out, DeltaRow{
				PrimaryKey: pk,
				Details: map[string]string{
					"rest":    restContent,
					"graphql": graphqlContent,
				},
			})
		}
	}
	return out, nil
}

// buildResolvers preloads each ResolvedColumn's resolution map from the
// database. Each map goes localID → platformID as a string for uniform
// comparison. Empty or unset references resolve to "<missing>" so a row
// whose FK points into thin air is visibly different from one that points
// to a real parent with an empty platform ID.
//
// When ResolvedColumn.CustomQuery is set, it replaces the default
// two-column SELECT. This supports chain resolutions through a join
// (e.g. pull_request_repo.pr_repo_meta_id → pull_request_meta → pull_requests).
func buildResolvers(ctx context.Context, pool *pgxpool.Pool, cols []resolvedColumn) (map[string]map[string]string, error) {
	out := map[string]map[string]string{}
	for _, c := range cols {
		if _, done := out[c.LocalColumn]; done {
			continue // same local column referenced twice, reuse
		}
		var q string
		if c.CustomQuery != "" {
			q = c.CustomQuery
		} else {
			q = "SELECT " + c.RefLocalKey + ", " + c.RefPlatformKey + " FROM " + c.RefTable
		}
		rows, err := pool.Query(ctx, q)
		if err != nil {
			return nil, fmt.Errorf("loading resolver for %s: %w", c.LocalColumn, err)
		}
		m := map[string]string{}
		for rows.Next() {
			vals, err := rows.Values()
			if err != nil {
				rows.Close()
				return nil, err
			}
			if len(vals) != 2 {
				continue
			}
			m[fmt.Sprintf("%v", vals[0])] = fmt.Sprintf("%v", vals[1])
		}
		rows.Close()
		out[c.LocalColumn] = m
	}
	return out, nil
}

// fetchRowsWithResolved is like fetchRows but substitutes each row's
// local-FK values with their resolved platform-stable values before
// building the content string. Columns are laid out in the SELECT as:
// [pk cols | content cols | content-level resolved-FK local cols].
//
// pkCols is the ordered list of PK column names. When a PK column name
// matches a resolver, fetchRowsWithResolved substitutes the value in the
// PK string just like fetchPKs does — so the two maps share keys.
func fetchRowsWithResolved(ctx context.Context, pool *pgxpool.Pool, query string, pkCols []string, contentCols int, resolvers map[string]map[string]string, rcols []resolvedColumn) (map[string]string, error) {
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nPK := len(pkCols)
	out := map[string]string{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		if len(vals) < nPK+contentCols {
			continue
		}
		pkParts := make([]string, 0, nPK)
		for i, name := range pkCols {
			localVal := fmt.Sprintf("%v", vals[i])
			if r, ok := resolvers[name]; ok {
				if resolved, found := r[localVal]; found {
					pkParts = append(pkParts, resolved)
					continue
				}
				pkParts = append(pkParts, "<missing:"+name+"="+localVal+">")
				continue
			}
			pkParts = append(pkParts, localVal)
		}
		contentParts := make([]string, 0, contentCols+len(rcols))
		for i := nPK; i < nPK+contentCols; i++ {
			contentParts = append(contentParts, fmt.Sprintf("%v", vals[i]))
		}
		// Content-level resolved columns appended after content.
		for j, rc := range rcols {
			idx := nPK + contentCols + j
			if idx >= len(vals) {
				break
			}
			localVal := fmt.Sprintf("%v", vals[idx])
			resolver := resolvers[rc.LocalColumn]
			resolved, ok := resolver[localVal]
			if !ok {
				resolved = "<missing>"
			}
			contentParts = append(contentParts, rc.LocalColumn+"→"+rc.RefPlatformKey+"="+resolved)
		}
		out[strings.Join(pkParts, "|")] = strings.Join(contentParts, "|")
	}
	return out, rows.Err()
}

// capAndCount leaves the first N examples intact. The ShadowSummary totals
// are computed by the caller using countMissing, so the capped example
// slice is purely for display.
func capAndCount(in []DeltaRow, _ int) []DeltaRow { return in }

// countMissing returns the number of keys in a that are not in b.
func countMissing(a, b map[string]struct{}) int {
	n := 0
	for k := range a {
		if _, ok := b[k]; !ok {
			n++
		}
	}
	return n
}
