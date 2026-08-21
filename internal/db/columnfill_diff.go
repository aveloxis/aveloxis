// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

// columnfill_diff.go — per-column fill-rate comparison for
// `aveloxis data-test` (v0.26.1).
//
// The row-count diff (rowcount_diff.go) catches missing ROWS; it is
// blind to a column the new binary stopped populating. The canonical
// example: issue_labels.platform_label_id is 0 on every row under the
// GraphQL path — row counts identical, data silently gone. This diff
// compares, per column, how many rows carry a MEANINGFUL value in each
// database.
//
// "Populated" is type-aware because this schema's idioms for "not
// filled in" are TEXT DEFAULT '' and INTEGER DEFAULT 0, not NULL:
//   - text-ish columns:    IS NOT NULL AND <> ''
//   - numeric columns:     IS NOT NULL AND <> 0
//   - everything else:     IS NOT NULL (booleans exempt from the
//     zero-check — false is a legitimate value; timestamps, uuids,
//     jsonb likewise)
//
// Classification is deliberately asymmetric, mirroring the row diff's
// philosophy of flagging REGRESSIONS specifically:
//   - FAIL: populated in released, COMPLETELY unpopulated in new — a
//     column went dark (dropped mapping, renamed JSON tag, a code path
//     that no longer fills the field).
//   - FLAG: any other difference. The two collections run ~30 minutes
//     apart against a live repo, so small drift (an edited profile, a
//     late comment) is expected — a hard FAIL on any inequality would
//     make the harness flaky. The report shows both numbers for human
//     review.
//   - PASS: equal populated counts (including both-zero).

package db

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ColumnFillDiffRow is one column's fill comparison across the two
// databases.
type ColumnFillDiffRow struct {
	Schema            string `json:"schema"`
	Table             string `json:"table"`
	Column            string `json:"column"`
	ReleasedPopulated int64  `json:"released_populated"`
	NewPopulated      int64  `json:"new_populated"`
	Status            string `json:"status"` // PASS | FLAG | FAIL (see file header)
}

// ColumnFillDiffReport is the structured result of the per-column
// comparison. Only non-PASS rows are retained — with ~2,500 columns
// across the schemas, an all-rows report would bury the signal.
//
// v0.27.129: the report also carries SCHEMA-SHAPE drift between the
// sides. The 2026-08-21 main-vs-branch run crashed on the first
// deliberate table drop it met (contributors_old, removed in
// v0.27.115) because enumeration was released-side-only and every
// released table was queried against the new side. Shape drift is
// exactly what a cross-version harness must REPORT, never crash on:
//   - TablesOnlyInReleased: tables the release under test DROPPED
//     (the row-count diff already FAILs these when they carried rows;
//     here they are visible as shape notes and their columns are
//     skipped).
//   - TablesOnlyInNew: tables the release ADDED (columns skipped —
//     no baseline exists).
//   - AddedColumns: new-side-only columns of BOTH-SIDES tables, with
//     their new-side populated counts (informational — the FLAG
//     mechanism only covers columns present on both sides).
//   - Removed columns (released-only columns of both-sides tables)
//     land in Rows: Status REMOVED when they carried data (a data-loss
//     shape — counts as a failure) or REMOVED-EMPTY when dark.
type ColumnFillDiffReport struct {
	Schemas              []string            `json:"schemas"`
	ColumnsChecked       int                 `json:"columns_checked"`
	Rows                 []ColumnFillDiffRow `json:"rows"` // FAIL + FLAG + REMOVED*
	TablesOnlyInReleased []string            `json:"tables_only_in_released,omitempty"`
	TablesOnlyInNew      []string            `json:"tables_only_in_new,omitempty"`
	AddedColumns         []ColumnFillDiffRow `json:"added_columns,omitempty"` // new-side populated count in NewPopulated
}

// HasFailures reports whether any column went dark or was removed
// while still carrying data in the released side.
func (r *ColumnFillDiffReport) HasFailures() bool {
	for _, row := range r.Rows {
		if row.Status == "FAIL" || row.Status == "REMOVED" {
			return true
		}
	}
	return false
}

// classifyColumnFill — see the file header for the FAIL/FLAG/PASS
// contract and its rationale.
func classifyColumnFill(released, newPopulated int64) string {
	switch {
	case released == newPopulated:
		return "PASS"
	case released > 0 && newPopulated == 0:
		return "FAIL"
	default:
		return "FLAG"
	}
}

// populatedPredicate builds the type-aware "carries a meaningful
// value" SQL predicate for one (already-quoted) column identifier.
func populatedPredicate(quotedCol, dataType string) string {
	switch dataType {
	case "text", "character varying", "character":
		return fmt.Sprintf("%s IS NOT NULL AND %s <> ''", quotedCol, quotedCol)
	case "smallint", "integer", "bigint", "numeric", "real", "double precision":
		return fmt.Sprintf("%s IS NOT NULL AND %s <> 0", quotedCol, quotedCol)
	default:
		// boolean, timestamps, uuid, jsonb, arrays, ...
		return fmt.Sprintf("%s IS NOT NULL", quotedCol)
	}
}

type columnRef struct {
	schema, table, column, dataType string
}

// colFillQuerier is the minimal query surface ColumnFillDiff needs.
// *pgxpool.Pool and pgx.Tx both satisfy it; the self-comparison
// integration test passes ONE repeatable-read transaction as both
// sides so the comparison sees a single MVCC snapshot even while
// other tests write to the same scratch database.
type colFillQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// ColumnFillDiff enumerates every column of every base table in the
// given schemas (default: the same set as RowCountDiff) on BOTH
// databases and compares per-column populated counts over the
// INTERSECTION; one-sided tables and columns are reported as schema
// shape drift (v0.27.129 — a dropped table used to crash the diff).
func ColumnFillDiff(ctx context.Context, released, newPool *pgxpool.Pool, schemas []string) (*ColumnFillDiffReport, error) {
	return columnFillDiff(ctx, released, newPool, schemas)
}

func columnFillDiff(ctx context.Context, released, newSide colFillQuerier, schemas []string) (*ColumnFillDiffReport, error) {
	if len(schemas) == 0 {
		schemas = []string{"aveloxis_data", "aveloxis_ops", "aveloxis_scan"}
	}

	relCols, err := enumerateColumns(ctx, released, schemas)
	if err != nil {
		return nil, fmt.Errorf("enumerate released columns: %w", err)
	}
	newCols, err := enumerateColumns(ctx, newSide, schemas)
	if err != nil {
		return nil, fmt.Errorf("enumerate new columns: %w", err)
	}

	colKey := func(c columnRef) string { return c.schema + "." + c.table + "." + c.column }
	tblKey := func(c columnRef) string { return c.schema + "." + c.table }
	relTables, newTables := map[string]bool{}, map[string]bool{}
	relColSet, newColSet := map[string]bool{}, map[string]bool{}
	for _, c := range relCols {
		relTables[tblKey(c)] = true
		relColSet[colKey(c)] = true
	}
	for _, c := range newCols {
		newTables[tblKey(c)] = true
		newColSet[colKey(c)] = true
	}

	report := &ColumnFillDiffReport{Schemas: schemas}
	seenTbl := map[string]bool{}
	for _, c := range relCols {
		if tk := tblKey(c); !newTables[tk] && !seenTbl[tk] {
			seenTbl[tk] = true
			report.TablesOnlyInReleased = append(report.TablesOnlyInReleased, tk)
		}
	}
	for _, c := range newCols {
		if tk := tblKey(c); !relTables[tk] && !seenTbl[tk] {
			seenTbl[tk] = true
			report.TablesOnlyInNew = append(report.TablesOnlyInNew, tk)
		}
	}
	sort.Strings(report.TablesOnlyInReleased)
	sort.Strings(report.TablesOnlyInNew)

	// Shared columns are compared; released-only columns of SHARED
	// tables are the removed-column class (queried on released only);
	// new-only columns of shared tables are additions (queried on new
	// only). Columns of one-sided tables are covered by the table-level
	// notes and skipped.
	var shared, removed, added []columnRef
	for _, c := range relCols {
		if !newTables[tblKey(c)] {
			continue
		}
		if newColSet[colKey(c)] {
			shared = append(shared, c)
		} else {
			removed = append(removed, c)
		}
	}
	for _, c := range newCols {
		if relTables[tblKey(c)] && !relColSet[colKey(c)] {
			added = append(added, c)
		}
	}
	report.ColumnsChecked = len(shared)

	releasedFill, err := populatedCounts(ctx, released, append(append([]columnRef{}, shared...), removed...))
	if err != nil {
		return nil, fmt.Errorf("released DB fill counts: %w", err)
	}
	newFill, err := populatedCounts(ctx, newSide, append(append([]columnRef{}, shared...), added...))
	if err != nil {
		return nil, fmt.Errorf("new DB fill counts: %w", err)
	}

	for _, c := range shared {
		key := colKey(c)
		row := ColumnFillDiffRow{
			Schema:            c.schema,
			Table:             c.table,
			Column:            c.column,
			ReleasedPopulated: releasedFill[key],
			NewPopulated:      newFill[key],
		}
		row.Status = classifyColumnFill(row.ReleasedPopulated, row.NewPopulated)
		if row.Status != "PASS" {
			report.Rows = append(report.Rows, row)
		}
	}
	for _, c := range removed {
		row := ColumnFillDiffRow{
			Schema: c.schema, Table: c.table, Column: c.column,
			ReleasedPopulated: releasedFill[colKey(c)],
		}
		if row.ReleasedPopulated > 0 {
			row.Status = "REMOVED" // carried data — a data-loss shape
		} else {
			row.Status = "REMOVED-EMPTY" // dark column dropped — shape note
		}
		report.Rows = append(report.Rows, row)
	}
	for _, c := range added {
		report.AddedColumns = append(report.AddedColumns, ColumnFillDiffRow{
			Schema: c.schema, Table: c.table, Column: c.column,
			NewPopulated: newFill[colKey(c)], Status: "ADDED",
		})
	}

	order := func(s string) int {
		switch s {
		case "FAIL":
			return 0
		case "REMOVED":
			return 1
		default:
			return 2
		}
	}
	sort.Slice(report.Rows, func(i, j int) bool {
		oi, oj := order(report.Rows[i].Status), order(report.Rows[j].Status)
		if oi != oj {
			return oi < oj
		}
		ki := report.Rows[i].Schema + "." + report.Rows[i].Table + "." + report.Rows[i].Column
		kj := report.Rows[j].Schema + "." + report.Rows[j].Table + "." + report.Rows[j].Column
		return ki < kj
	})
	sort.Slice(report.AddedColumns, func(i, j int) bool {
		return report.AddedColumns[i].Schema+"."+report.AddedColumns[i].Table+"."+report.AddedColumns[i].Column <
			report.AddedColumns[j].Schema+"."+report.AddedColumns[j].Table+"."+report.AddedColumns[j].Column
	})
	return report, nil
}

func enumerateColumns(ctx context.Context, pool colFillQuerier, schemas []string) ([]columnRef, error) {
	rows, err := pool.Query(ctx, `
		SELECT c.table_schema, c.table_name, c.column_name, c.data_type
		FROM information_schema.columns c
		JOIN information_schema.tables t
		  ON t.table_schema = c.table_schema AND t.table_name = c.table_name
		WHERE t.table_type = 'BASE TABLE'
		  AND c.table_schema = ANY($1)
		ORDER BY c.table_schema, c.table_name, c.ordinal_position
	`, schemas)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []columnRef
	for rows.Next() {
		var c columnRef
		if err := rows.Scan(&c.schema, &c.table, &c.column, &c.dataType); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}

// populatedCounts computes the populated count for every column, ONE
// single-scan query per table (all of a table's columns aggregated in
// one pass).
func populatedCounts(ctx context.Context, pool colFillQuerier, cols []columnRef) (map[string]int64, error) {
	byTable := map[string][]columnRef{}
	for _, c := range cols {
		key := c.schema + "." + c.table
		byTable[key] = append(byTable[key], c)
	}

	out := map[string]int64{}
	for _, tableCols := range byTable {
		schema, table := tableCols[0].schema, tableCols[0].table
		exprs := make([]string, len(tableCols))
		for i, c := range tableCols {
			quoted := fmt.Sprintf("%q", c.column)
			exprs[i] = fmt.Sprintf("COUNT(*) FILTER (WHERE %s)", populatedPredicate(quoted, c.dataType))
		}
		// Identifiers come from information_schema, quoted with %q —
		// same safety posture as tableRowCounts.
		q := fmt.Sprintf(`SELECT %s FROM %q.%q`, strings.Join(exprs, ", "), schema, table)
		row := pool.QueryRow(ctx, q)
		dests := make([]any, len(tableCols))
		vals := make([]int64, len(tableCols))
		for i := range vals {
			dests[i] = &vals[i]
		}
		if err := row.Scan(dests...); err != nil {
			return nil, fmt.Errorf("fill counts for %s.%s: %w", schema, table, err)
		}
		for i, c := range tableCols {
			out[c.schema+"."+c.table+"."+c.column] = vals[i]
		}
	}
	return out, nil
}
