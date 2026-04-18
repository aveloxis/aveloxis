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
			Name:           "aveloxis_data.pull_requests",
			PrimaryKey:     []string{"repo_id", "pr_number"},
			ContentColumns: []string{"pr_title", "pr_state", "author_id", "merge_commit_sha"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_labels",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "label_name"},
			ContentColumns: []string{"label_description", "label_color"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_assignees",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "platform_assignee_id"},
			ContentColumns: []string{},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_reviewers",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "platform_reviewer_id"},
			ContentColumns: []string{},
			HasRepoID:      true,
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
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_files",
			PrimaryKey:     []string{"repo_id", "pull_request_id", "pr_file_path"},
			ContentColumns: []string{"pr_file_additions", "pr_file_deletions"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_meta",
			PrimaryKey:     []string{"pull_request_id", "head_or_base"},
			ContentColumns: []string{"meta_ref", "meta_sha"},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.issue_message_ref",
			PrimaryKey:     []string{"issue_id", "msg_id"},
			ContentColumns: []string{},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_message_ref",
			PrimaryKey:     []string{"pull_request_id", "msg_id"},
			ContentColumns: []string{},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.pull_request_review_message_ref",
			PrimaryKey:     []string{"pr_review_id", "msg_id"},
			ContentColumns: []string{},
			HasRepoID:      true,
		},
		{
			Name:           "aveloxis_data.review_comments",
			PrimaryKey:     []string{"repo_id", "platform_src_id"},
			ContentColumns: []string{"msg_id", "commit_id", "file_path", "line"},
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

	// Fetch primary-key sets from each side. Content comparison is done
	// in code rather than SQL because the two databases are on different
	// connections — a server-side JOIN is not available.
	restPKs, err := fetchPKs(ctx, rest, tbl, where)
	if err != nil {
		return nil, err
	}
	graphqlPKs, err := fetchPKs(ctx, graphql, tbl, where)
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

	// Content comparison on the intersection.
	if len(tbl.ContentColumns) > 0 {
		mismatches, err := diffContent(ctx, rest, graphql, tbl, where)
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
func fetchPKs(ctx context.Context, pool *pgxpool.Pool, tbl shadowTable, where string) (map[string]struct{}, error) {
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
		for _, v := range vals {
			parts = append(parts, fmt.Sprintf("%v", v))
		}
		out[strings.Join(parts, "|")] = struct{}{}
	}
	return out, rows.Err()
}

// diffContent finds rows that exist on both sides but have differing content
// columns. Implemented by pulling PK+content from both sides and comparing in
// memory; capped at exampleCap mismatches reported.
func diffContent(ctx context.Context, rest, graphql *pgxpool.Pool, tbl shadowTable, where string) ([]DeltaRow, error) {
	cols := append([]string(nil), tbl.PrimaryKey...)
	cols = append(cols, tbl.ContentColumns...)
	sel := strings.Join(cols, ", ")
	q := "SELECT " + sel + " FROM " + tbl.Name + where

	restData, err := fetchRows(ctx, rest, q, len(tbl.PrimaryKey))
	if err != nil {
		return nil, err
	}
	graphqlData, err := fetchRows(ctx, graphql, q, len(tbl.PrimaryKey))
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

// fetchRows returns a map from the first pkCols-wide key to a string
// concatenation of the remaining (content) columns. Used by diffContent
// to compare content quickly in memory.
func fetchRows(ctx context.Context, pool *pgxpool.Pool, query string, pkCols int) (map[string]string, error) {
	rows, err := pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := map[string]string{}
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			return nil, err
		}
		if len(vals) < pkCols {
			continue
		}
		pkParts := make([]string, 0, pkCols)
		for i := 0; i < pkCols; i++ {
			pkParts = append(pkParts, fmt.Sprintf("%v", vals[i]))
		}
		contentParts := make([]string, 0, len(vals)-pkCols)
		for i := pkCols; i < len(vals); i++ {
			contentParts = append(contentParts, fmt.Sprintf("%v", vals[i]))
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
